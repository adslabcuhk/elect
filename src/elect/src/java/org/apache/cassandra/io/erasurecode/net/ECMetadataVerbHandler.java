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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.LeveledGenerations;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionManager.AllSSTableOpStatus;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.SSTableReaderComparator;
import org.apache.cassandra.io.erasurecode.net.ECSyncSSTableVerbHandler.DataForRewrite;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECMetadataVerbHandler implements IVerbHandler<ECMetadata> {
    public static final ECMetadataVerbHandler instance = new ECMetadataVerbHandler();
    // private static final String ecMetadataDir = System.getProperty("user.dir") +
    // "/data/ECMetadata/";
    public static List<ECMetadata> ecMetadatas = new ArrayList<ECMetadata>();

    private static final Logger logger = LoggerFactory.getLogger(ECMetadataVerbHandler.class);

    private volatile static boolean isConsumeBlockedECMetadataOccupied = false;

    // Reset
    public static final String RESET = "\033[0m"; // Text Reset

    // Regular Colors
    public static final String WHITE = "\033[0;30m"; // WHITE
    public static final String RED = "\033[0;31m"; // RED
    public static final String GREEN = "\033[0;32m"; // GREEN
    public static final String YELLOW = "\033[0;33m"; // YELLOW
    public static final String BLUE = "\033[0;34m"; // BLUE
    public static final String PURPLE = "\033[0;35m"; // PURPLE
    public static final String CYAN = "\033[0;36m"; // CYAN
    public static final String GREY = "\033[0;37m"; // GREY

    @Override
    public synchronized void doVerb(Message<ECMetadata> message) throws IOException {
        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            logger.debug("ELECT-Debug: this is a forwarding header");
        }

        InetAddressAndPort sourceIP = message.from();
        InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        // receive metadata and record it to files (append)
        // ecMetadatas.add(message.payload);
        // logger.debug("ELECT-Debug: received metadata: {}, {},{},{}", message.payload,
        // message.payload.sstHashIdList, message.payload.primaryNodes,
        // message.payload.relatedNodes);
        ECMetadata ecMetadata = message.payload;

        Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap = ecMetadata.ecMetadataContent.sstHashIdToReplicaMap;
        String newSSTHashForUpdate = ecMetadata.ecMetadataContent.sstHashIdList
                .get(ecMetadata.ecMetadataContent.targetIndex);
        String oldSSTHashForUpdate = ecMetadata.ecMetadataContent.oldSSTHashForUpdate;
        String ksName = ecMetadata.ecMetadataContent.keyspace;

        // if(newSSTHashForUpdate.equals(oldSSTHashForUpdate)) {
        // logger.error("ELECT-ERROR: new sstHash ({}) should not the same to olSSTHash
        // ({})", newSSTHashForUpdate, oldSSTHashForUpdate);
        // } else {
        // logger.debug("ELECT-Debug: Get a parity update signal, the old sstHash is ({}),
        // the new sstHash is ({})", oldSSTHashForUpdate, newSSTHashForUpdate);
        // logger.debug("ELECT-Debug: got sstHashIdToReplicaMap: {} ",
        // sstHashIdToReplicaMap);
        for (Map.Entry<String, List<InetAddressAndPort>> entry : sstHashIdToReplicaMap.entrySet()) {
            String newSSTableHash = entry.getKey();
            if (!localIP.equals(entry.getValue().get(0)) && entry.getValue().contains(localIP)) {
                int index = entry.getValue().indexOf(localIP);
                String secondaryCfName = ecMetadata.ecMetadataContent.cfName + index;

                // transformECMetadataToECSSTable(ecMetadata, ks, cfName, sstableHash,
                // sourceIP);

                BlockedECMetadata blockedECMetadata;

                // Check if the old sstable is available, if not, add it to the queue
                StorageService.instance.globalRecvECMetadatas++;
                if (ecMetadata.ecMetadataContent.isParityUpdate) {
                    if (!entry.getKey().equals(newSSTHashForUpdate)) {
                        blockedECMetadata = new BlockedECMetadata(newSSTableHash,
                                sourceIP,
                                new ECMetadata(new ECMetadataContent(ecMetadata.ecMetadataContent.stripeId,
                                        ksName,
                                        ecMetadata.ecMetadataContent.cfName,
                                        ecMetadata.ecMetadataContent.sstHashIdList,
                                        ecMetadata.ecMetadataContent.parityHashList,
                                        ecMetadata.ecMetadataContent.primaryNodes,
                                        ecMetadata.ecMetadataContent.secondaryNodes,
                                        ecMetadata.ecMetadataContent.parityNodes,
                                        sstHashIdToReplicaMap,
                                        entry.getKey(),
                                        true,
                                        ecMetadata.ecMetadataContent.targetIndex,
                                        ecMetadata.ecMetadataContent.zeroChunksNum)),
                                secondaryCfName);

                    } else {
                        blockedECMetadata = new BlockedECMetadata(newSSTableHash, sourceIP, ecMetadata,
                                secondaryCfName);
                    }

                    boolean isECSSTableAvailable = (StorageService.instance.globalSSTHashToECSSTableMap
                            .get(blockedECMetadata.ecMetadata.ecMetadataContent.oldSSTHashForUpdate) != null);
                    boolean isPreviousUpdateDone = !StorageService.instance.globalUpdatingSSTHashList
                            .contains(ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
                    logger.debug(
                            "ELECT-Debug: [ECMetadata for Strip Update, Save it for {}] ECMetadataVerbHandler get a needed update sstHash {} from parity node {}, we update it because oldSSTHash {} changed to newSSTHash {}, the replica nodes are {}, sstHashList is {}, strip id is {}, isECSSTableAvailable? {}, isPreviousUpdateDone? {}",
                            secondaryCfName, newSSTableHash, sourceIP, oldSSTHashForUpdate, newSSTHashForUpdate,
                            entry.getValue(), ecMetadata.ecMetadataContent.sstHashIdList,
                            ecMetadata.ecMetadataContent.stripeId,
                            isECSSTableAvailable, isPreviousUpdateDone);
                    saveECMetadataToBlockList(blockedECMetadata,
                            blockedECMetadata.ecMetadata.ecMetadataContent.oldSSTHashForUpdate,
                            isECSSTableAvailable && isPreviousUpdateDone);

                } else {
                    blockedECMetadata = new BlockedECMetadata(newSSTableHash, sourceIP, ecMetadata, secondaryCfName);
                    logger.debug(
                            "ELECT-Debug: [ECMetadata for Erasure Coding, Save it for {}] ECMetadataVerbHandler get a needed update sstHash {} from parity node {}, we are going to record it directly, the replica nodes are {}, sstHashList is {}, strip id is {}, zero chunks number is {}",
                            secondaryCfName, newSSTableHash, sourceIP, entry.getValue(),
                            ecMetadata.ecMetadataContent.sstHashIdList, ecMetadata.ecMetadataContent.stripeId,
                            ecMetadata.ecMetadataContent.zeroChunksNum);
                    saveECMetadataToBlockList(blockedECMetadata,
                            blockedECMetadata.ecMetadata.ecMetadataContent.oldSSTHashForUpdate, true);
                }

            } else {
                logger.debug(
                        "ELECT-Debug: [Drop it] ECMetadataVerbHandler get sstHash {} from {}, the replica nodes are {}, strip id is {}",
                        newSSTableHash, sourceIP, entry.getValue(), ecMetadata.ecMetadataContent.stripeId);
            }

        }
        // }

    }

    public static Runnable getConsumeBlockedECMetadataRunnable() {
        // Consume the blocked ecMetadata if needed
        logger.debug(
                "ELECT-Debug: This is getConsumeBlockedECMetadataRunnable, the globalReadyECMetadatas is {}, isConsumeBlockedECMetadataOccupied is ({})",
                StorageService.instance.globalReadyECMetadatas.size(),
                isConsumeBlockedECMetadataOccupied);

        return new ConsumeBlockedECMetadataRunnable();
    }

    /**
     * To avoid concurrency conflicts, we store all received ECMetadata in Map
     * <globalReadyECMetadatas>.
     * And we set up a periodically task to consume ECMetadata, that is transforming
     * the ECMetadata into a ecSSTable.
     * Note that the ECMetadata could be generated in two cases:
     * 1. During the first time generating erasure coding;
     * 2. Parity update.
     */
    private static class ConsumeBlockedECMetadataRunnable implements Runnable {

        private final int MAX_RETRY_COUNT = 5;

        @Override
        public synchronized void run() {
            if (StorageService.instance.globalReadyECMetadatas.isEmpty()) {
                logger.debug("ELECT-Debug: globalReadyECMetadatas is empty.");
                return;
            }

            // if(isConsumeBlockedECMetadataOccupied) {
            // logger.debug("ELECT-Debug: ConsumeBlockedECMetadataOccupied is true");
            // return;
            // }

            // isConsumeBlockedECMetadataOccupied = true;

            logger.debug("ELECT-Debug: This is ConsumeBlockedECMetadataRunnable");

            for (Map.Entry<String, ConcurrentLinkedQueue<BlockedECMetadata>> entry : StorageService.instance.globalReadyECMetadatas
                    .entrySet()) {
                String ks = "ycsb";
                String cfName = entry.getKey();

                // for (BlockedECMetadata metadata : entry.getValue()) {
                while (!entry.getValue().isEmpty()) {
                    BlockedECMetadata metadata = entry.getValue().poll();
                    while (metadata.retryCount <= MAX_RETRY_COUNT) {
                        try {
                            if (!transformECMetadataToECSSTable(metadata.ecMetadata, ks, cfName,
                                    metadata.newSSTableHash,
                                    metadata.sourceIP)) {
                                logger.debug(
                                        "ELECT-Debug: Perform transformECMetadataToECSSTable successfully, new sstHash is ({}), old sstHash is ({})",
                                        metadata.newSSTableHash,
                                        metadata.ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
                                StorageService.instance.globalConsumedECMetadatas++;
                                // entry.getValue().remove(metadata);
                                break;
                            } else if (metadata.retryCount < MAX_RETRY_COUNT) {
                                metadata.retryCount++;
                                logger.debug(
                                        "ELECT-Debug: Cannot transform ecmetadata ({}) to ecSSTable, retry count is ({})",
                                        metadata.ecMetadata.ecMetadataContent.stripeId, metadata.retryCount);
                                Thread.sleep(100);
                            } else {
                                logger.debug(
                                        "ELECT-Debug: Still cannot create transactions, but we won't try it again, write the data down immediately.");
                                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cfName);

                                StorageService.instance.globalConsumedECMetadatas++;
                                // If the ecMetadata is for erasure coding, just transform it
                                if (!metadata.ecMetadata.ecMetadataContent.isParityUpdate) {
                                    DataForRewrite dataForRewrite = StorageService.instance.globalSSTHashToSyncedFileMap
                                            .get(metadata.newSSTableHash);
                                    if (dataForRewrite != null) {
                                        String fileNamePrefix = dataForRewrite.fileNamePrefix;
                                        transformECMetadataToECSSTableForErasureCode(metadata.ecMetadata,
                                                new ArrayList<SSTableReader>(),
                                                cfs,
                                                fileNamePrefix,
                                                metadata.newSSTableHash,
                                                metadata.sourceIP, dataForRewrite.firstKey, dataForRewrite.lastKey,
                                                dataForRewrite.sourceKeys);
                                        // entry.getValue().remove(metadata);
                                    } else {
                                        logger.error(
                                                "ELECT-ERROR: cannot get rewrite data of {} during redo transformECMetadataToECSSTable",
                                                metadata.newSSTableHash);
                                    }
                                } else {
                                    // TODO: Wait until the target ecSSTable is released
                                    // transformECMetadataToECSSTableForParityUpdate(metadata.ecMetadata, cfs,
                                    // metadata.sstableHash);
                                    StorageService.instance.globalUpdatingSSTHashList
                                            .remove(metadata.ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
                                    logger.error("ELECT-ERROR: wait until the target ecSSTable is released");
                                }
                                break;

                            }
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            break;
                        }

                    }

                    // entry.getValue().remove(metadata);
                }

            }

            if (StorageService.instance.globalReadyECMetadataCount == StorageService.instance.globalConsumedECMetadatas
                    &&
                    StorageService.instance.globalBolckedECMetadataCount > 0) {

                List<String> pendingECMetadata = new ArrayList<String>();
                for (Map.Entry<String, ConcurrentLinkedQueue<BlockedECMetadata>> entry : StorageService.instance.globalPendingECMetadata
                        .entrySet()) {
                    if (!entry.getValue().isEmpty()) {
                        for (BlockedECMetadata metadata : entry.getValue()) {
                            pendingECMetadata.add(metadata.ecMetadata.ecMetadataContent.stripeId);
                        }
                    }
                }

                logger.debug(
                        "ELECT-Debug: globalRecvECMetadatas is ({}), global consume ECMetadatas is ({}), global ready ECMetadata count is ({}), global pending ECMetadata count is ({}), pending ECMetadata size is ({}), pending ECMetadatas are ({})",
                        StorageService.instance.globalRecvECMetadatas,
                        StorageService.instance.globalConsumedECMetadatas,
                        StorageService.instance.globalReadyECMetadataCount,
                        StorageService.instance.globalBolckedECMetadataCount,
                        pendingECMetadata.size(), pendingECMetadata);
                if (StorageService.instance.globalConsumedECMetadatas == StorageService.instance.globalReadyECMetadataCount
                        &&
                        StorageService.instance.globalBolckedECMetadataCount > 0) {
                    for (Map.Entry<String, ConcurrentLinkedQueue<BlockedECMetadata>> entry : StorageService.instance.globalPendingECMetadata
                            .entrySet()) {
                        if (!entry.getValue().isEmpty()) {
                            logger.debug(
                                    "ELECT-Debug: the old sstHash is ({}), cfName is ({}), new sstHash is ({}), sourceIp is ({}), stripe id is ({})",
                                    entry.getKey(), entry.getValue().peek().cfName,
                                    entry.getValue().peek().newSSTableHash, entry.getValue().peek().sourceIP,
                                    entry.getValue().peek().ecMetadata.ecMetadataContent.stripeId);
                        }
                    }
                }
            }

            // isConsumeBlockedECMetadataOccupied = false;

        }

    }

    /**
     * 
     * @param ecMetadata
     * @param ks
     * @param cfName
     * @param sstableHash
     * @param sourceIP
     * @return Is failed to create the transaction?
     *         Add a concurrent lock here
     * @throws InterruptedException
     */
    private static boolean transformECMetadataToECSSTable(ECMetadata ecMetadata, String ks, String cfName,
            String newSSTHash, InetAddressAndPort sourceIP) throws InterruptedException {

        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cfName);
        // get the dedicated level of sstables
        if (!ecMetadata.ecMetadataContent.isParityUpdate) {
            // [In progress of erasure coding]
            DataForRewrite dataForRewrite = StorageService.instance.globalSSTHashToSyncedFileMap.get(newSSTHash);

            if (dataForRewrite != null) {

                String fileNamePrefix = dataForRewrite.fileNamePrefix;
                List<SSTableReader> sstables = new ArrayList<>(
                        cfs.getSSTableForLevel(LeveledGenerations.getMaxLevelCount() - 1));
                if (!sstables.isEmpty()) {
                    // Collections.sort(sstables, new SSTableReaderComparator());
                    DecoratedKey firstKeyForRewrite = dataForRewrite.firstKey;
                    DecoratedKey lastKeyForRewrite = dataForRewrite.lastKey;
                    List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>(
                            LeveledManifest.overlapping(firstKeyForRewrite.getToken(),
                                    lastKeyForRewrite.getToken(),
                                    sstables));
                    // use binary search to find related sstables
                    // rewriteSStables = getRewriteSSTables(sstables, firstKeyForRewrite,
                    // lastKeyForRewrite);
                    // logger.debug("ELECT-Debug: read sstable from ECMetadata, sstable name is {}",
                    // ecSSTable.getFilename());

                    return transformECMetadataToECSSTableForErasureCode(ecMetadata, rewriteSStables, cfs,
                            fileNamePrefix,
                            newSSTHash, sourceIP, firstKeyForRewrite, lastKeyForRewrite,
                            dataForRewrite.sourceKeys);
                } else {
                    logger.info("ELECT-Debug: cannot replace the existing sstables yet, as {} is lower than {}",
                            cfs.getColumnFamilyName(), LeveledGenerations.getMaxLevelCount() - 1);
                }
            } else {
                throw new InterruptedException(String.format(
                        "ELECT-ERROR: cannot get rewrite data of {%s} during erasure coding, message is from {%s}, target cfs is {%s}",
                        newSSTHash, sourceIP, cfName));
            }
            return false;

        } else {
            // if(ecMetadata.ecMetadataContent.sstHashIdList.indexOf(newSSTHash) ==
            // ecMetadata.ecMetadataContent.targetIndex)
            StorageService.instance.globalUpdatingSSTHashList.add(ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
            logger.debug("ELECT-Debug: We mark the old sstable {} as updating.",
                    ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
            return transformECMetadataToECSSTableForParityUpdate(ecMetadata, cfs, newSSTHash);
            // return false;
        }

    }

    private static boolean transformECMetadataToECSSTableForErasureCode(ECMetadata ecMetadata,
            List<SSTableReader> rewriteSStables,
            ColumnFamilyStore cfs, String fileNamePrefix,
            String newSSTHash, InetAddressAndPort sourceIP,
            DecoratedKey firstKeyForRewrite, DecoratedKey lastKeyForRewrite,
            Map<String, DecoratedKey> sourceKeys) {

        final LifecycleTransaction updateTxn = cfs.getTracker().tryModify(rewriteSStables, OperationType.COMPACTION);

        // M is the sstable from primary node, M` is the corresponding sstable of
        // secondary node

        if (updateTxn != null) {

            logger.debug("ELECT-Debug: Create an erasure coding transaction ({}), sstHash is ({})", updateTxn.opId(),
                    newSSTHash);

            if (rewriteSStables.isEmpty() ||
                    isOnlyContainerECSSTable(rewriteSStables)) {

                logger.debug(
                        "ELECT-Debug: rewriteSStables is empty or only, table name is {}, sstHash ({}), just record it!",
                        cfs.getColumnFamilyName(), newSSTHash);
                cfs.updateECSSTable(ecMetadata, newSSTHash, cfs, fileNamePrefix, updateTxn);
                StorageService.instance.globalSSTHashToSyncedFileMap.remove(newSSTHash);

            }
            // else if (rewriteSStables.size() == 1) {
            // logger.debug("ELECT-Debug: Anyway, we just replace the sstables");
            // List<String> expiredFiles = new ArrayList<String>();
            // for(Component comp :
            // SSTableReader.componentsFor(rewriteSStables.get(0).descriptor)) {
            // expiredFiles.add(rewriteSStables.get(0).descriptor.filenameFor(comp));
            // }

            // cfs.updateECSSTable(ecMetadata, newSSTHash, cfs, fileNamePrefix, updateTxn);
            // // delete the old sstables after we update the new one
            // for(String fileName : expiredFiles) {
            // ECNetutils.deleteFileByName(fileName);
            // }

            // }
            else if (rewriteSStables.size() >= 1) {
                logger.debug(
                        "ELECT-Debug: for sstHash ({}), many sstables are involved, table name is {}, {} sstables need to rewrite!",
                        newSSTHash, cfs.getColumnFamilyName(), rewriteSStables.size());
                // logger.debug("ELECT-Debug: rewrite sstable {} Data.db with EC.db",
                // ecSSTable.descriptor);
                try {
                    AllSSTableOpStatus status = cfs.sstablesRewrite(
                            sourceKeys,
                            firstKeyForRewrite,
                            lastKeyForRewrite,
                            rewriteSStables, ecMetadata, fileNamePrefix, updateTxn, false,
                            Long.MAX_VALUE, false, 1);
                    if (status != AllSSTableOpStatus.SUCCESSFUL)
                        ECNetutils.printStatusCode(status.statusCode, cfs.name);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            StorageService.instance.globalSSTHashToSyncedFileMap.remove(newSSTHash);
        } else {
            // Save ECMetadata and redo ec transition later
            logger.debug(
                    "ELECT-Debug: [ErasureCoding] failed to get transactions for the sstables ({}), we will try it later",
                    newSSTHash);
            // BlockedECMetadata blockedECMetadata = new BlockedECMetadata(sstableHash,
            // sourceIP, ecMetadata);
            // saveECMetadataToBlockList(cfs.getColumnFamilyName(), blockedECMetadata);
            return true;
        }

        return false;

    }

    private static boolean isOnlyContainerECSSTable(Iterable<SSTableReader> sstables) {
        for (SSTableReader sstable : sstables) {
            if (Files.exists(Paths.get(sstable.descriptor.filenameFor(Component.DATA)))) {
                return false;
            }
        }
        return true;
    }

    private static boolean transformECMetadataToECSSTableForParityUpdate(ECMetadata ecMetadata, ColumnFamilyStore cfs,
            String newSSTHash) {
        // [In progress of parity update], update the related sstables, there are two
        // cases:
        // 1. For the parity update sstable, replace the ECMetadata
        // 2. For the non-updated sstable, just replace the files
        // String currentSSTHash = entry.getKey();
        int sstIndex = ecMetadata.ecMetadataContent.sstHashIdList.indexOf(newSSTHash);
        // need a old sstHash
        logger.debug(
                "ELECT-Debug: [Parity Update] we are going to update the old sstable ({}) with a new one ({}) for strip id ({}) in ({})",
                ecMetadata.ecMetadataContent.oldSSTHashForUpdate, newSSTHash, ecMetadata.ecMetadataContent.stripeId,
                cfs.getColumnFamilyName());
        SSTableReader oldECSSTable = StorageService.instance.globalSSTHashToECSSTableMap
                .get(ecMetadata.ecMetadataContent.oldSSTHashForUpdate);

        if (oldECSSTable != null) {
            if (sstIndex == ecMetadata.ecMetadataContent.targetIndex) {
                // replace ec sstable

                DataForRewrite dataForRewrite = StorageService.instance.globalSSTHashToSyncedFileMap.get(newSSTHash);
                if (dataForRewrite != null) {

                    String fileNamePrefix = dataForRewrite.fileNamePrefix;
                    final LifecycleTransaction updateTxn = cfs.getTracker()
                            .tryModify(Collections.singletonList(oldECSSTable), OperationType.COMPACTION);

                    if (updateTxn != null) {

                        logger.debug(
                                "ELECT-Debug: Create a stripe update transaction ({}), new sstable is ({}), old sstable is ({}).",
                                updateTxn.opId(), newSSTHash, oldECSSTable.getSSTableHashID());
                        List<String> expiredFiles = new ArrayList<String>();
                        for (Component comp : SSTableReader.componentsFor(oldECSSTable.descriptor)) {
                            expiredFiles.add(oldECSSTable.descriptor.filenameFor(comp));
                        }
                        cfs.updateECSSTable(ecMetadata, newSSTHash, cfs, fileNamePrefix, updateTxn);
                        for (String fileName : expiredFiles) {
                            ECNetutils.deleteFileByName(fileName);
                        }

                        StorageService.instance.globalUpdatingSSTHashList
                                .remove(ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
                        logger.debug(
                                "ELECT-Debug: We get a transaction for old sstable ({}), and new sstable ({}) successfully.",
                                ecMetadata.ecMetadataContent.oldSSTHashForUpdate, newSSTHash);
                        // remove the entry to save memory
                        // StorageService.instance.globalSSTHashToECSSTableMap.remove(ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
                        return false;
                    } else {
                        logger.debug(
                                "ELECT-Debug:[Parity Update] failed to get transactions for the old sstables ({}), and new sstable ({}) we will try it later",
                                oldECSSTable.getSSTableHashID(), newSSTHash);
                        return true;
                    }
                } else {
                    logger.error(
                            "ELECT-ERROR:[Parity Update] cannot get rewrite data of {} during parity update for old sstable {}",
                            newSSTHash, oldECSSTable.getSSTableHashID());
                }

            } else {
                // Just replace the files
                try {
                    logger.debug(
                            "ELECT-Debug: When we update sstable ({}), we just need to replace the old ec metadata files",
                            newSSTHash);
                    SSTableReader.loadECMetadata(ecMetadata, oldECSSTable.descriptor, null);
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                StorageService.instance.globalUpdatingSSTHashList
                        .remove(ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
            }
        } else {
            ECNetutils.printStackTace(String.format("ELECT-ERROR: [Parity Update] cannot get ecSSTable for sstHash(%s)",
                    ecMetadata.ecMetadataContent.oldSSTHashForUpdate));
        }
        return false;
    }

    public static void checkTheBlockedUpdateECMetadata(SSTableReader oldECSSTable) {
        if (StorageService.instance.globalPendingECMetadata.containsKey(oldECSSTable.getSSTableHashID()) &&
                !StorageService.instance.globalPendingECMetadata.get(oldECSSTable.getSSTableHashID()).isEmpty()) {
            if (StorageService.instance.globalPendingECMetadata.get(oldECSSTable.getSSTableHashID()).size() > 1) {
                logger.debug("ELECT-Debug: the size of globalPendingECMetadata for sstHash ({}) is ({})",
                        oldECSSTable.getSSTableHashID(),
                        StorageService.instance.globalPendingECMetadata.get(oldECSSTable.getSSTableHashID()).size());
            }
            while (!StorageService.instance.globalPendingECMetadata.get(oldECSSTable.getSSTableHashID()).isEmpty()) {
                BlockedECMetadata blockedECMetadata = StorageService.instance.globalPendingECMetadata
                        .get(oldECSSTable.getSSTableHashID()).poll();
                logger.debug("ELECT-Debug: we move an ECMetadata for old sstable {} from [Pending list] to [Ready list]",
                        oldECSSTable.getSSTableHashID());
                StorageService.instance.globalBolckedECMetadataCount--;
                saveECMetadataToBlockList(blockedECMetadata, null, true);
            }
        }
    }

    public static class BlockedECMetadata {
        public final String newSSTableHash;
        public final InetAddressAndPort sourceIP;
        public final String cfName;
        public ECMetadata ecMetadata;
        public int retryCount = 0;

        public BlockedECMetadata(String newSSTableHash, InetAddressAndPort sourceIP, ECMetadata ecMetadata,
                String cfName) {
            this.newSSTableHash = newSSTableHash;
            this.sourceIP = sourceIP;
            this.ecMetadata = ecMetadata;
            this.cfName = cfName;
        }
    }

    private synchronized static void saveECMetadataToBlockList(BlockedECMetadata metadata, String oldSSTHash,
            boolean isAddToTheProcessQueueDirectly) {

        if (isAddToTheProcessQueueDirectly) {
            logger.debug(
                    "ELECT-Debug: Save the ECMetadata ({}) to the [Ready List] for oldSSTHash ({}), newSSTHash ({}), metadata.oldHash is ({})",
                    metadata.ecMetadata.ecMetadataContent.stripeId, oldSSTHash, metadata.newSSTableHash,
                    metadata.ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
            if (StorageService.instance.globalReadyECMetadatas.containsKey(metadata.cfName)) {
                // if(!StorageService.instance.globalReadyECMetadatas.get(metadata.cfName).contains(metadata))
                StorageService.instance.globalReadyECMetadatas.get(metadata.cfName).add(metadata);
            } else {
                ConcurrentLinkedQueue<BlockedECMetadata> blockList = new ConcurrentLinkedQueue<BlockedECMetadata>();
                blockList.add(metadata);
                StorageService.instance.globalReadyECMetadatas.put(metadata.cfName, blockList);
            }
            StorageService.instance.globalReadyECMetadataCount++;
        } else {
            logger.debug(
                    "ELECT-Debug: Save the ECMetadata ({}) to global [Pending List] for oldSSTHash ({}), newSSTHash ({}), metadata.oldHash is ({})",
                    metadata.ecMetadata.ecMetadataContent.stripeId, oldSSTHash, metadata.newSSTableHash,
                    metadata.ecMetadata.ecMetadataContent.oldSSTHashForUpdate);
            if (StorageService.instance.globalPendingECMetadata.containsKey(oldSSTHash)) {
                // if(!StorageService.instance.globalPendingECMetadata.get(oldSSTHash).contains(metadata))
                StorageService.instance.globalPendingECMetadata.get(oldSSTHash).add(metadata);
            } else {
                ConcurrentLinkedQueue<BlockedECMetadata> blockList = new ConcurrentLinkedQueue<BlockedECMetadata>();
                blockList.add(metadata);
                StorageService.instance.globalPendingECMetadata.put(oldSSTHash, blockList);
            }
            StorageService.instance.globalBolckedECMetadataCount++;
        }
    }

    private static void forwardToLocalNodes(Message<ECMetadata> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECMetadata> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECMetadata> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }

    private static List<SSTableReader> getRewriteSSTables(List<SSTableReader> sstables, DecoratedKey first,
            DecoratedKey last) {
        List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>();
        // TODO: add head and tail
        // first search which sstable does the first key stored
        int left = 0;
        int right = sstables.size() - 1;
        int mid = 0;
        while (left <= right) {
            mid = (left + right) / 2;
            SSTableReader sstable = sstables.get(mid);
            if (sstable.first.compareTo(first) <= 0 &&
                    sstable.last.compareTo(first) >= 0) {
                // rewriteSStables.add(sstable);
                break;
            } else if (sstable.first.compareTo(first) < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // then search which sstable does the last key stored
        int tail = mid + 1;
        while (tail < sstables.size() && last.compareTo(sstables.get(tail).first) >= 0) {
            if (sstables.get(tail).getSSTableLevel() != LeveledGenerations.getMaxLevelCount() - 1)
                logger.warn("ELECT-Warns: sstable level {} is not equal to threshold {}",
                        sstables.get(tail).getSSTableLevel(), LeveledGenerations.getMaxLevelCount() - 1);
            if (!sstables.get(tail).isReplicationTransferredToErasureCoding())
                rewriteSStables.add(sstables.get(tail));
            tail++;
        }

        int head = mid - 1;
        if (head >= 0 && (rewriteSStables.size() > 1 || first.compareTo(sstables.get(head).last) <= 0)) {
            if (sstables.get(head).getSSTableLevel() != LeveledGenerations.getMaxLevelCount() - 1)
                logger.warn("ELECT-Warns: sstable level {} is not equal to threshold {}",
                        sstables.get(head).getSSTableLevel(), LeveledGenerations.getMaxLevelCount() - 1);
            if (!sstables.get(head).isReplicationTransferredToErasureCoding())
                rewriteSStables.add(sstables.get(head));
            head--;
        }

        if (head >= 0 && !sstables.get(head).isReplicationTransferredToErasureCoding())
            rewriteSStables.add(sstables.get(head));
        if (tail < sstables.size() && !sstables.get(tail).isReplicationTransferredToErasureCoding())
            rewriteSStables.add(sstables.get(tail));

        return rewriteSStables;
    }

}
