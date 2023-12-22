/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.erasurecode.net;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.antlr.runtime.tree.Tree;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type.Collection;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureEncoder;
import org.apache.cassandra.io.erasurecode.NativeRSEncoder;
import org.apache.cassandra.io.erasurecode.alibaba.OSSAccess;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate.SSTableContentWithHashID;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.shaded.netty.buffer.ByteBuf;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.math3.exception.NullArgumentException;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Support parity update operations.
 * 1. For an old sstable, there has some cases needed to be considered, if any
 * of them are not true we will add the old sstable to the waiting list.
 * Otherwise we will retrieve the parity code of the corresponding EC strip, and
 * add the to the ready list.
 * 1.1 We should check if the old sstable is available;
 * 1.2 Check if the old sstable corresponded ec strip is not updating;
 * 
 * 2. For a new sstable, we save it to globalReadyNewSSTableForECStripUpdateMap
 * directly.
 */

public class ECParityUpdateVerbHandler implements IVerbHandler<ECParityUpdate> {
    public static final ECParityUpdateVerbHandler instance = new ECParityUpdateVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECParityUpdateVerbHandler.class);
    private static List<InetAddressAndPort> parityNodes = new ArrayList<InetAddressAndPort>();

    private static volatile long globalConsumedNewSSTable = 0;
    private static volatile long globalReceivedNewSSTable = 0;
    private static volatile long globalConsumedOldSSTable = 0;
    private static volatile long globalReceivedOldSSTable = 0;

    /**
     * Receives update data from the primary node, and performs the following steps:
     * 1. First consume the new data list, perform parity update
     * 2. If new data is empty while the old data is not empty, add old data to the
     * global queue
     */
    @Override
    public synchronized void doVerb(Message<ECParityUpdate> message) throws IOException {
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            logger.debug("ELECT-Debug: this is a forwarding header");
        }

        ECParityUpdate parityUpdateData = message.payload;
        if (parityNodes.isEmpty()) {
            parityNodes = parityUpdateData.parityNodes;
        }

        InetAddressAndPort primaryNode = message.from();
        int codeLength = StorageService.getErasureCodeLength();

        if (parityUpdateData.isOldSSTable) {

            globalReceivedOldSSTable++;

            logger.debug("ELECT-Debug: [Parity Update] Get a old sstable ({}) from primary node {}",
                    parityUpdateData.sstable.sstHash, primaryNode);

            String oldSSTHash = parityUpdateData.sstable.sstHash;
            String stripID = StorageService.instance.globalSSTHashToStripIDMap.get(oldSSTHash);

            // Strip ID is null means that the previous erasure coding task has not
            // completed, so we need to add this sstable to a cache map.
            if (stripID == null || StorageService.instance.globalUpdatingStripList.contains(stripID)) {

                // just add this old sstable to the cache map
                StorageService.instance.globalPendingOldSSTableForECStripUpdateMap.put(parityUpdateData.sstable.sstHash,
                        parityUpdateData.sstable);

                logger.debug(
                        "ELECT-Debug: In node {}, strip id {} for sstHash {} is not ready, so we save it to [pending list],this hash is from primary node {}, the old sstable map is {}, new sstable map is {}",
                        FBUtilities.getBroadcastAddressAndPort(),
                        stripID,
                        oldSSTHash,
                        primaryNode,
                        StorageService.instance.globalReadyOldSSTableForECStripUpdateMap.keySet(),
                        StorageService.instance.globalReadyNewSSTableForECStripUpdateMap.keySet());
            } else {

                // Check if the there is a new sstable that has the same sstHash with this old
                // one
                if (!isNewSSTableQueueContainThisOldSSTable(
                        StorageService.instance.globalReadyNewSSTableForECStripUpdateMap.get(primaryNode),
                        parityUpdateData.sstable)) {

                    logger.debug(
                            "ELECT-Debug: we need get parity codes for sstable ({}), add it to the ready list, mark strip id {} as updating",
                            oldSSTHash, stripID);
                    StorageService.instance.globalUpdatingStripList.add(stripID);
                    // if(StorageService.instance.globalUpdatingStripList.containsKey(stripID)) {
                    // StorageService.instance.globalUpdatingStripList.compute(stripID, (key,
                    // oldValue) -> oldValue + 1);
                    // } else {
                    // StorageService.instance.globalUpdatingStripList.put(stripID, 1);
                    // }

                    retrieveParityCodeForOldSSTable(oldSSTHash, stripID, codeLength);
                    parityUpdateData.sstable.isRequestParityCode = true;

                    ECNetutils.addOldSSTableForECStripeUpdateToReadyList(primaryNode, parityUpdateData.sstable);
                }

                // isNewSSTableQueueContainThisOldSSTable(StorageService.instance.globalReadyNewSSTableForECStripUpdateMap.get(primaryNode),
                // parityUpdateData.sstable);

            }

        } else {
            logger.debug("ELECT-Debug: [Parity Update] Get a new sstable ({}) from primary node {}, add it to ready list",
                    parityUpdateData.sstable.sstHash, primaryNode);
            globalReceivedNewSSTable++;
            ECNetutils.addNewSSTableForECStripeUpdateToReadyList(primaryNode, parityUpdateData.sstable);
        }

    }

    private static List<InetAddressAndPort> getParityNodes() {
        return parityNodes;
    }

    public static Runnable getParityUpdateRunnable() {
        return new ParityUpdateRunnable();
    }

    private static class ParityUpdateRunnable implements Runnable {

        private static volatile long executeCount = 0;

        @Override
        public synchronized void run() {
            String keyspaceName = "ycsb";
            int codeLength = StorageService.getErasureCodeLength();

            List<String> traversedSSTables = new ArrayList<String>();
            executeCount++;

            logger.debug(
                    "ELECT-Debug: the entries of globalPendingOldSSTableForECStripUpdateMap is ({}), the entries of globalReadyOldSSTableForECStripUpdateMap is ({}), traversedSSTables are ({}), received new sstable count is ({}), consumed new sstable count is ({}), received old sstable count is ({}), consumed old sstable count is ({}), total received ec messages are ({}), consumed ec messages are ({}), globalRecvECMetadatas is ({}), global consume ECMetadatas is ({}), global ready ECMetadata count is ({}), global pending ECMetadata count is ({}), generated normal metadata is ({}), padding zero metadata is ({}), migrated parity code count is ({}), migrated sstable count is ({}) , execute count is ({})",
                    StorageService.instance.globalPendingOldSSTableForECStripUpdateMap.size(),
                    StorageService.instance.globalReadyOldSSTableForECStripUpdateCount, traversedSSTables,
                    globalReceivedNewSSTable, globalConsumedNewSSTable, globalReceivedOldSSTable,
                    globalConsumedOldSSTable,
                    StorageService.instance.totalReceivedECMessages, StorageService.instance.totalConsumedECMessages,
                    StorageService.instance.globalRecvECMetadatas, StorageService.instance.globalConsumedECMetadatas,
                    StorageService.instance.globalReadyECMetadataCount,
                    StorageService.instance.globalBolckedECMetadataCount,
                    StorageService.instance.generatedNormalECMetadata,
                    StorageService.instance.generatedPaddingZeroECMetadata,
                    StorageService.instance.migratedParityCodeCount,
                    StorageService.instance.migratedRawSSTablecount,
                    executeCount);
            long traversedNewSSTables = 0;

            long totalTimeOfRetrievedParityCodes = 0;
            // Perform parity update
            for (Map.Entry<InetAddressAndPort, ConcurrentLinkedQueue<SSTableContentWithHashID>> entry : StorageService.instance.globalReadyOldSSTableForECStripUpdateMap
                    .entrySet()) {

                // get oldReplicaNodes
                InetAddressAndPort primaryNode = entry.getKey();
                List<InetAddressAndPort> oldReplicaNodes = StorageService.instance
                        .getReplicaNodesWithPortFromPrimaryNode(primaryNode, keyspaceName);

                ConcurrentLinkedQueue<SSTableContentWithHashID> newSSTableQueue = StorageService.instance.globalReadyNewSSTableForECStripUpdateMap
                        .get(primaryNode);
                ConcurrentLinkedQueue<SSTableContentWithHashID> oldSSTableQueue = entry.getValue();

                // if( oldSSTableQueue.size() < newSSTableQueue.size()) {
                // logger.error("ELECT-ERROR: new sstable count ({}) is more than the old count
                // ({})!", newSSTableQueue.size(),
                // oldSSTableQueue.size());
                // }

                // Case1: Consume old data with new data firstly.
                // In this case, old replica nodes are the same to new replica nodes
                while (!oldSSTableQueue.isEmpty() && !newSSTableQueue.isEmpty()) {

                    SSTableContentWithHashID newSSTable = newSSTableQueue.poll();
                    StorageService.instance.globalReadyOldSSTableForECStripUpdateCount--;
                    SSTableContentWithHashID oldSSTable = oldSSTableQueue.poll();
                    globalConsumedNewSSTable++;
                    globalConsumedOldSSTable++;
                    traversedNewSSTables++;
                    traversedSSTables.add(oldSSTable.sstHash);

                    logger.debug("ELECT-Debug: Parity update case 1, Select a new sstable ({}) and an old sstable ({})",
                            newSSTable.sstHash, oldSSTable.sstHash);

                    // if the new obj is consumed, move the same name sstable from cache queue to
                    // old queue
                    if (StorageService.instance.globalPendingNewOldSSTableForECStripUpdateMap
                            .containsKey(newSSTable.sstHash)) {
                        oldSSTableQueue.add(StorageService.instance.globalPendingNewOldSSTableForECStripUpdateMap
                                .get(newSSTable.sstHash));
                        StorageService.instance.globalPendingNewOldSSTableForECStripUpdateMap
                                .remove(newSSTable.sstHash);
                    }

                    // logger.debug("ELECT-Debug: [Parity update] We check the parity codes for
                    // replacing a new sstable ({}) with an old sstable ({})", newSSTable.sstHash,
                    // oldSSTable.sstHash);
                    try {
                        totalTimeOfRetrievedParityCodes += performECStripUpdate("case 1", oldSSTable, newSSTable,
                                codeLength, oldReplicaNodes);
                    } catch (FileNotFoundException | InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                }

                if (!newSSTableQueue.isEmpty()) {
                    logger.debug("ELECT-Debug: for primary node ({}) new sstable is not completely consumed ({})!",
                            primaryNode, newSSTableQueue.size(), oldSSTableQueue.size());
                    continue;
                }

                // if (!newSSTableQueue.isEmpty()) {
                // logger.error("ELECT-ERROR: The new sstables are not completely consumed!!!");
                // }

                // Case2: If old data is not completely consumed, we select sstables from
                // globalRecvQueues
                while (!oldSSTableQueue.isEmpty()) {

                    if (StorageService.instance.globalRecvQueues.containsKey(primaryNode)) {
                        ECMessage msg = ECNetutils.getDataBlockFromGlobalRecvQueue(primaryNode);
                        SSTableContentWithHashID newSSTable = new SSTableContentWithHashID(
                                msg.ecMessageContent.sstHashID, msg.sstContent);
                        SSTableContentWithHashID oldSSTable = oldSSTableQueue.poll();
                        globalConsumedOldSSTable++;
                        StorageService.instance.globalReadyOldSSTableForECStripUpdateCount--;
                        traversedSSTables.add(oldSSTable.sstHash);
                        logger.debug(
                                "ELECT-Debug: Parity update case 2, Select a new sstable ({}) and an old sstable ({})",
                                newSSTable.sstHash, oldSSTable.sstHash);

                        try {
                            totalTimeOfRetrievedParityCodes += performECStripUpdate("case 2", oldSSTable, newSSTable,
                                    codeLength, oldReplicaNodes);
                        } catch (FileNotFoundException | InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                    } else {
                        break;
                    }
                }

                // Case3: Old data still not completely consumed, we have to padding zero
                while (!oldSSTableQueue.isEmpty()) {

                    byte[] newSSTContent = new byte[codeLength];
                    SSTableContentWithHashID newSSTable = new SSTableContentWithHashID(
                            ECNetutils.stringToHex(String.valueOf(newSSTContent.hashCode())),
                            newSSTContent);
                    SSTableContentWithHashID oldSSTable = oldSSTableQueue.poll();
                    globalConsumedOldSSTable++;
                    traversedSSTables.add(oldSSTable.sstHash);

                    StorageService.instance.globalReadyOldSSTableForECStripUpdateCount--;

                    logger.debug("ELECT-Debug: Parity update case 3, Select a new sstable ({}) and an old sstable ({})",
                            newSSTable.sstHash, oldSSTable.sstHash);
                    try {
                        totalTimeOfRetrievedParityCodes += performECStripUpdate("case 3", oldSSTable, newSSTable,
                                codeLength, oldReplicaNodes);
                    } catch (FileNotFoundException | InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            logger.debug(
                    "ELECT-Debug: we are going to release NO. ({}) parity update thread, the total consumed time is ({}), this transaction we consumed ({}) old sstables and ({}) new sstables",
                    executeCount, totalTimeOfRetrievedParityCodes, traversedSSTables.size(), traversedNewSSTables);

        }

    }

    /**
     * This is the method that start EC strip update sub-threads
     * 
     * @param updateCase      We have 3 cases for EC strip update.
     * @param oldSSTable      The encoded sstable that has been compacted by the
     *                        primary node.
     * @param newSSTable      The new sstable for replacing the oldSSTable in the EC
     *                        strip.
     * @param codeLength      The uniform code length for erasure coding.
     * @param oldReplicaNodes
     * @return The retry times (in seconds) to retrieve the needed parity code.
     * @throws FileNotFoundException
     * @throws InterruptedException
     */
    private static synchronized int performECStripUpdate(String updateCase,
            SSTableContentWithHashID oldSSTable,
            SSTableContentWithHashID newSSTable,
            int codeLength,
            List<InetAddressAndPort> oldReplicaNodes) throws FileNotFoundException, InterruptedException {

        logger.debug("ELECT-Debug: [Parity update {}]  We select a new sstable ({}) to update an old sstable ({})",
                updateCase, newSSTable.sstHash, oldSSTable.sstHash);
        List<InetAddressAndPort> parityNodes = getParityNodes();
        String oldStripID = StorageService.instance.globalSSTHashToStripIDMap.get(oldSSTable.sstHash);
        if (oldStripID == null) {
            throw new NullPointerException(String.format("ELECT-ERROR: we cannot get strip id (%s) for sstable (%s)",
                    oldStripID, oldSSTable.sstHash));
        }

        // For safety, we should make sure the parity code is ready
        if (!oldSSTable.isRequestParityCode) {
            retrieveParityCodeForOldSSTable(oldSSTable.sstHash, oldStripID, codeLength);
            oldSSTable.isRequestParityCode = true;
        }
        int retryTime = waitUntilParityCodesReady(oldSSTable.sstHash, parityNodes);
        logger.debug("ELECT-Debug: we spend ({}) seconds to get the parity code from peers nodes ({})", retryTime,
                parityNodes);

        if (StorageService.instance.globalSSTHashToParityCodeMap.get(oldSSTable.sstHash) == null) {
            throw new NullPointerException(
                    String.format("ELECT-ERROR: we cannot get parity codes for sstable (%s)", oldSSTable.sstHash));
        }

        if (StorageService.instance.globalStripIdToECMetadataMap.get(oldStripID).sstHashIdList == null ||
                StorageService.instance.globalStripIdToECMetadataMap.get(oldStripID).sstHashIdList.isEmpty()) {
            throw new NullPointerException(
                    String.format("ELECT-ERROR: we cannot get sstHash list for strip id (%s)", oldStripID));
        }

        // ByteBuffer oldData = oldSSTable.sstContent;
        // ByteBuffer newData = newSSTable.sstContent;
        // codeLength = Stream.of(codeLength, newSSTable.sstContentSize,
        // oldSSTable.sstContentSize).max(Integer::compareTo).orElse(codeLength);
        Stage.ERASURECODE.maybeExecuteImmediately(new ErasureCodeUpdateRunnable(oldSSTable,
                newSSTable,
                StorageService.instance.globalSSTHashToParityCodeMap.get(oldSSTable.sstHash),
                StorageService.instance.globalStripIdToECMetadataMap.get(oldStripID).sstHashIdList
                        .indexOf(oldSSTable.sstHash),
                codeLength,
                parityNodes,
                oldReplicaNodes,
                oldReplicaNodes));

        return retryTime;
    }

    private static boolean isNewSSTableQueueContainThisOldSSTable(
            ConcurrentLinkedQueue<SSTableContentWithHashID> newQueue, SSTableContentWithHashID oldSSTable) {
        if (newQueue != null) {
            for (SSTableContentWithHashID newSSTable : newQueue) {
                if (newSSTable.sstHash.equals(oldSSTable.sstHash)) {
                    StorageService.instance.globalPendingNewOldSSTableForECStripUpdateMap.put(oldSSTable.sstHash,
                            oldSSTable);
                    logger.debug(
                            "ELECT-Debug: For sstable ({}), the new obj is still not consumed, we add the old obj to cache map",
                            oldSSTable.sstHash);
                    return true;
                }
            }
        }

        return false;

    }

    /**
     * This method is called when we decide to save a EC strip update signal to the
     * process queue.
     * 
     * @param oldSSTHash
     * @param stripID
     * @param codeLength
     */
    public static synchronized void retrieveParityCodeForOldSSTable(String oldSSTHash, String stripID, int codeLength) {

        String localParityCodeDir = ECNetutils.getLocalParityCodeDir();

        // read ec_metadata from memory, get the needed parity hash list
        List<String> parityHashList = null;
        try {
            parityHashList = StorageService.instance.globalStripIdToECMetadataMap.get(stripID).parityHashList;
        } catch (Exception e) {
            logger.error("ELECT-ERROR: When we are update old sstable ({}), we cannot to get ecMetadata for stripID {}",
                    oldSSTHash, stripID);
        }

        if (parityHashList == null) {
            ECNetutils.printStackTace(String.format(
                    "ELECT-ERROR: When we are update old sstable (%s), we cannot to get ecMetadata for stripID (%s)",
                    oldSSTHash, stripID));
        } else {
            ByteBuffer[] parityCodes = new ByteBuffer[parityHashList.size()];
            if (codeLength == 0)
                codeLength = StorageService.getErasureCodeLength();

            for (int i = 0; i < parityHashList.size(); i++) {
                parityCodes[i] = ByteBuffer.allocateDirect(StorageService.getErasureCodeLength());
            }
            try {

                if (DatabaseDescriptor.getEnableMigration() && DatabaseDescriptor.getTargetStorageSaving() > 0.45 &&
                        ECNetutils.checkIsParityCodeMigrated(parityHashList.get(0))) {

                    for (int i = 0; i < parityCodes.length; i++) {
                        String parityCodeFileName = localParityCodeDir + parityHashList.get(i);

                        int retry_count = 0;
                        long start = currentTimeMillis();
                        while (!StorageService.ossAccessObj.downloadFileAsByteArrayFromOSS(parityCodeFileName,
                                parityNodes.get(0).getHostAddress(false)) &&
                                retry_count < ECNetutils.getMigrationRetryCount()) {
                            retry_count++;
                        }
                        long timeCost = currentTimeMillis() - start;
                        StorageService.instance.migratedParityCodeTimeCost += timeCost;
                        logger.debug("The migrate time for {} is {}", parityHashList.get(i), timeCost);
                        byte[] parityCode = ECNetutils.readBytesFromFile(parityCodeFileName);
                        parityCodes[i].put(parityCode);
                        // StorageService.ossAccessObj.deleteSingleFileInOSS(parityCodeFileName);
                        StorageService.instance.migratedParityCodeCount--;
                        StorageService.instance.migratedParityCodes.remove(parityHashList.get(i));
                    }

                    StorageService.instance.globalSSTHashToParityCodeMap.put(oldSSTHash, parityCodes);

                } else {
                    // get the needed parity code locally
                    String parityCodeFileName = localParityCodeDir + parityHashList.get(0);
                    ByteBuffer localParityCode;
                    localParityCode = ByteBuffer.wrap(ECNetutils.readBytesFromFile(parityCodeFileName));

                    parityCodes[0].put(localParityCode);
                    // parityCodes[0].rewind();

                    // get old parity codes from old sstable hash
                    StorageService.instance.globalSSTHashToParityCodeMap.put(oldSSTHash, parityCodes);
                    logger.debug(
                            "ELECT-Debug: Perform parity update for old sstable ({}), we are retrieving parity codes for strip id {}, we had read local parity code {}",
                            oldSSTHash, stripID, parityCodeFileName);

                    // get the needed parity code remotely, send a parity code request
                    for (int i = 1; i < parityHashList.size(); i++) {
                        ECRequestParity request = new ECRequestParity(parityHashList.get(i),
                                oldSSTHash,
                                i,
                                false,
                                null);
                        request.requestParityCode(parityNodes.get(i));
                    }
                    // delete local parity code file
                    ECNetutils.deleteFileByName(parityCodeFileName);
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                // e.printStackTrace();
                throw new IllegalAccessError(String.format(
                        "ELECT-ERROR: When we are retrieving parity codes for strip id %s to perform parity update old sstable (%s), cannot read parity code from %s",
                        stripID, oldSSTHash));
            }

        }

    }

    /**
     * [WARNING!] Make sure to avoid dead loops
     * This method
     * 
     * @param oldSSTHash
     * @param parityNodes
     * @return The retry times (in seconds) to retrieve the needed parity code.
     * @throws InterruptedException
     * @throws FileNotFoundException
     */
    private static int waitUntilParityCodesReady(String oldSSTHash, List<InetAddressAndPort> parityNodes)
            throws InterruptedException, FileNotFoundException {
        int retryCount = 0;

        ByteBuffer[] parityCodes = StorageService.instance.globalSSTHashToParityCodeMap.get(oldSSTHash);
        if (parityCodes != null) {
            while (!checkParityCodesAreReady(parityCodes)) {
                if (retryCount < 100) {
                    Thread.sleep(100);
                    retryCount++;
                } else {
                    throw new FileNotFoundException(String.format(
                            "ELECT-ERROR: cannot retrieve the remote parity codes for sstHash (%s) from parity nodes (%s)",
                            oldSSTHash, parityNodes.subList(1, parityNodes.size())));
                }
            }
        } else {
            throw new InterruptedException(
                    String.format("ELECT-ERROR: We cannot get parity codes for sstable %s", oldSSTHash));

        }

        for (ByteBuffer parityCode : parityCodes) {
            parityCode.rewind();
        }

        return retryCount;

    }

    private static boolean checkParityCodesAreReady(ByteBuffer[] parityCodes) {
        for (ByteBuffer buf : parityCodes) {
            if (buf.position() == 0) {
                return false;
            }
        }
        return true;
    }

    private static void forwardToLocalNodes(Message<ECParityUpdate> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECParityUpdate> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECParityUpdate> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }

    /**
     * [ELECT]
     * To support perform erasure coding with multiple threads, we implement the
     * following Runnable class
     * 
     * @param ecDataNum the value of k
     * @param ecParity  the value of m
     * @param messages  the input data to be processed, length equal to ecDataNum
     */
    private static class ErasureCodeUpdateRunnable implements Runnable {
        private final int ecDataNum = DatabaseDescriptor.getEcDataNodes();
        private final int ecParityNum = DatabaseDescriptor.getParityNodes();
        private final SSTableContentWithHashID oldSSTable;
        private final SSTableContentWithHashID newSSTable;
        private final ByteBuffer[] parityCodes;
        private final int targetDataIndex;
        private final int codeLength;
        private final List<InetAddressAndPort> parityNodes;
        private final List<InetAddressAndPort> oldRelicaNodes;
        private final List<InetAddressAndPort> newRelicaNodes;

        ErasureCodeUpdateRunnable(SSTableContentWithHashID oldSSTable,
                SSTableContentWithHashID newSSTable,
                ByteBuffer[] oldParityCodes,
                int targetDataIndex, int codeLength, List<InetAddressAndPort> parityNodes,
                List<InetAddressAndPort> oldRelicaNodes,
                List<InetAddressAndPort> newRelicaNodes) {
            this.oldSSTable = oldSSTable;
            this.newSSTable = newSSTable;
            this.parityCodes = oldParityCodes;
            this.targetDataIndex = targetDataIndex;
            this.codeLength = codeLength;
            this.parityNodes = parityNodes;
            this.oldRelicaNodes = oldRelicaNodes;
            this.newRelicaNodes = newRelicaNodes;
        }

        @Override
        public void run() {
            long startTime = currentTimeMillis();
            ErasureCoderOptions ecOptions = new ErasureCoderOptions(ecDataNum, ecParityNum);
            ErasureEncoder encoder = new NativeRSEncoder(ecOptions);

            logger.debug("ELECT-Debug: let's start computing erasure code");

            // Encoding input and output
            ByteBuffer[] oldData = new ByteBuffer[1];
            ByteBuffer[] newData = new ByteBuffer[1];

            // prepare old data
            oldData[0] = ByteBuffer.allocateDirect(codeLength);
            oldData[0].put(oldSSTable.sstContent);
            int oldRemaining = oldData[0].remaining();
            if (oldRemaining > 0) {
                byte[] zeros = new byte[oldRemaining];
                oldData[0].put(zeros);
            }
            oldData[0].rewind();

            // Prepare new data
            newData[0] = ByteBuffer.allocateDirect(codeLength);
            newData[0].put(newSSTable.sstContent);
            int newRemaining = newData[0].remaining();
            if (newRemaining > 0) {
                byte[] zeros = new byte[newRemaining];
                newData[0].put(zeros);
            }
            newData[0].rewind();

            ByteBuffer[] newParityCodes = new ByteBuffer[parityCodes.length];
            for (int i = 0; i < newParityCodes.length; i++) {
                newParityCodes[i] = ByteBuffer.allocateDirect(codeLength);
            }

            // 0: old data, 1: new data, m is old parity codes
            ByteBuffer[] dataUpdate = new ByteBuffer[2 + parityCodes.length];
            for (int i = 0; i < dataUpdate.length; i++) {
                dataUpdate[i] = ByteBuffer.allocateDirect(codeLength);
            }
            // fill this buffer
            dataUpdate[0] = oldData[0];
            dataUpdate[0].rewind();
            dataUpdate[1] = newData[0];
            dataUpdate[1].rewind();

            for (int i = 2; i < dataUpdate.length; i++) {
                dataUpdate[i] = parityCodes[i - 2];
                dataUpdate[i].rewind();
            }

            // Encode update
            try {
                encoder.encodeUpdate(dataUpdate, newParityCodes, targetDataIndex);
            } catch (IOException e) {
                logger.error("ELECT-ERROR: Perform erasure code error", e);
            }

            long timeCost = currentTimeMillis() - startTime;
            StorageService.instance.encodingTime += timeCost;
            // generate parity hash code
            List<String> parityHashList = new ArrayList<String>();
            for (ByteBuffer parityCode : newParityCodes) {
                parityHashList.add(ECNetutils.stringToHex(String.valueOf(parityCode.hashCode())));
            }

            // record first parity code to current node
            String localParityCodeDir = ECNetutils.getLocalParityCodeDir();
            int needMirateParityCodeCount = ECNetutils.getNeedMigrateParityCodesCount();
            if ((DatabaseDescriptor.getEnableMigration() && DatabaseDescriptor.getTargetStorageSaving() > 0.45 &&
                    needMirateParityCodeCount > StorageService.instance.migratedParityCodeCount)
                    || DatabaseDescriptor.getStorageSavingGrade() >= 2) {

                for (int i = 0; i < newParityCodes.length; i++) {

                    byte[] parityInBytes = new byte[StorageService.getErasureCodeLength()];
                    newParityCodes[i].get(parityInBytes);

                    long startUploadTime = System.currentTimeMillis();
                    if (!StorageService.ossAccessObj.uploadFileToOSS(localParityCodeDir + parityHashList.get(i),
                            parityInBytes)) {
                        logger.error("[ELECT]: Could not upload parity SSTable: {}",
                                localParityCodeDir + parityHashList.get(i));
                    } else {
                        StorageService.instance.migratedParityCodeCount++;
                        StorageService.instance.migratedParityCodes.add(parityHashList.get(i));
                    }
                    long uploadTimeCost = System.currentTimeMillis() - startUploadTime;
                    StorageService.instance.migratedParityCodeTimeCost += uploadTimeCost;
                    logger.debug("The migrate time for {} is {}", parityHashList.get(i), uploadTimeCost);

                }

            } else {

                try {
                    FileChannel fileChannel = FileChannel.open(Paths.get(localParityCodeDir, parityHashList.get(0)),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE);
                    fileChannel.write(newParityCodes[0]);
                    fileChannel.close();
                    // logger.debug("ELECT-Debug: parity code file created: {}",
                    // parityCodeFile.getName());
                } catch (IOException e) {
                    logger.error("ELECT-ERROR: Perform erasure code error", e);
                }
                // sync encoded data to parity nodes
                ECParityNode ecParityNode = new ECParityNode(null, null, 0);
                ecParityNode.distributeCodedDataToParityNodes(newParityCodes, parityNodes, parityHashList);

            }

            // update ECMetadata and distribute it
            // get old ECMetadata content
            String stripID = StorageService.instance.globalSSTHashToStripIDMap.get(oldSSTable.sstHash);
            ECMetadataContent oldMetadata = StorageService.instance.globalStripIdToECMetadataMap.get(stripID);
            // update the isParityUpdate, sstHashIdList, parityHashList, replication nodes,
            // stripID
            ECMetadata ecMetadata = new ECMetadata(oldMetadata);
            ecMetadata.updateAndDistributeMetadata(parityHashList, true,
                    oldSSTable.sstHash, newSSTable.sstHash, targetDataIndex,
                    oldRelicaNodes, newRelicaNodes);

            // remove the entry to save memory
            StorageService.instance.globalSSTHashToParityCodeMap.remove(oldSSTable.sstHash);
            logger.debug("ELECT-Debug: we remove the parity code for old sstHash ({}) in memory.", oldSSTable.sstHash);
        }

    }

    public static void test1() throws FileNotFoundException {
        throw new FileNotFoundException("test throws IllegalStateException");
    }

    public static void test() throws FileNotFoundException {
        test1();
    }

    public static void main(String[] args) {
        try {
            test();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // ByteBuffer buffer = ByteBuffer.allocateDirect(10);
        // logger.debug("buffer.remaining = {}, buffer.hasRemaining = {},
        // buffer.position = {}, buffer.limit = {}, buffer.capacity = {}",
        // buffer.remaining(), buffer.hasRemaining(), buffer.position(), buffer.limit(),
        // buffer.capacity());
        // buffer.put((byte) 1);
        // logger.debug("buffer.remaining = {}, buffer.hasRemaining = {},
        // buffer.position = {}, buffer.limit = {}, buffer.capacity = {}",
        // buffer.remaining(), buffer.hasRemaining(), buffer.position(), buffer.limit(),
        // buffer.capacity());
        // buffer.rewind();
        // logger.debug("buffer.remaining = {}, buffer.hasRemaining = {},
        // buffer.position = {}, buffer.limit = {}, buffer.capacity = {}",
        // buffer.remaining(), buffer.hasRemaining(), buffer.position(), buffer.limit(),
        // buffer.capacity());
    }

}
