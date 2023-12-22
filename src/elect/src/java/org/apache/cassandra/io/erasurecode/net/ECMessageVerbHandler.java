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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureEncoder;
import org.apache.cassandra.io.erasurecode.NativeRSEncoder;
import org.apache.cassandra.io.erasurecode.alibaba.OSSAccess;
import org.apache.cassandra.io.erasurecode.net.ECMessage.ECMessageContent;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class ECMessageVerbHandler implements IVerbHandler<ECMessage> {

    public static final ECMessageVerbHandler instance = new ECMessageVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECMessage.class);

    // private static ConcurrentHashMap<InetAddressAndPort,
    // Queue<ECMessage>>recvQueues = new ConcurrentHashMap<InetAddressAndPort,
    // Queue<ECMessage>>();

    private void respond(Message<?> respondTo, InetAddressAndPort respondToAddress) {
        Tracing.trace("Enqueuing response to {}", respondToAddress);
        MessagingService.instance().send(respondTo.emptyResponse(), respondToAddress);
    }

    private void failed() {
        Tracing.trace("Payload application resulted in ECTimeout, not replying");
    }

    /*
     * TODO list:
     * 1. Collect k SST contents;
     * 2. Compute erasure coding locally;
     * 3. Send code to another parity nodes
     */
    @Override
    public synchronized void doVerb(Message<ECMessage> message) throws IOException {
        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            logger.debug("ELECT-Debug: this is a forwarding header");
        }

        if (StorageService.instance.globalRecvSSTHashList.contains(message.payload.ecMessageContent.sstHashID)) {
            return;
        } else {
            StorageService.instance.globalRecvSSTHashList.add(message.payload.ecMessageContent.sstHashID);
        }

        byte[] sstContent = message.payload.sstContent;
        int ec_data_num = message.payload.ecMessageContent.ecDataNum;

        // for (String ep : message.payload.parityNodesString.split(",")) {
        // message.payload.parityNodes.add(InetAddressAndPort.getByName(ep.substring(1)));
        // }

        logger.debug(
                "ELECT-Debug: get a new sstable ({}) for erasure coding!!! message is from: {}, primaryNode is {}, parityNodes is {}, sstable content size is {}",
                message.payload.ecMessageContent.sstHashID,
                message.from(), message.payload.ecMessageContent.replicaNodes.get(0),
                message.payload.ecMessageContent.parityNodes,
                message.payload.sstContent.length);

        InetAddressAndPort primaryNode = message.payload.ecMessageContent.replicaNodes.get(0);

        // save the received data to recvQueue
        ECNetutils.saveECMessageToGlobalRecvQueue(primaryNode, message.payload);

        // logger.debug("ELECT-Debug: recvQueues is {}", recvQueues);

        // StorageService.instance.globalRecvQueues.forEach((address, queue) ->
        // System.out.print("Queue length of " + address + " is " + queue.size()));
        String logString = "ELECT-Debug: Insight the globalRecvQueues";
        for (Map.Entry<InetAddressAndPort, ConcurrentLinkedQueue<ECMessage>> entry : StorageService.instance.globalRecvQueues
                .entrySet()) {
            String str = entry.getKey().toString() + " has " + entry.getValue().size() + " elements, ";
            logString += str;
        }
        logger.debug(logString);

        // check whether we should update the parity code

        Tracing.trace("recieved sstContent is: {}, ec_data_num is: {}, sourceEdpoint is: {}, header is: {}",
                sstContent, ec_data_num, message.from(), message.header);
    }

    public static Runnable getErasureCodingRunable() {
        return new ErasureCodingRunable();
    }

    // Once we have k different sstContent, do erasure coding locally
    private static class ErasureCodingRunable implements Runnable {

        private static int THRESHOLD_OF_PADDING_ZERO_CHUNKS = 5;
        private static int cnt = 0;

        @Override
        public synchronized void run() {

            if (StorageService.instance.globalRecvQueues.size() == 0)
                return;

            int codeLength = StorageService.getErasureCodeLength();
            if (StorageService.instance.globalRecvQueues.size() > 0 &&
                    StorageService.instance.globalRecvQueues.size() < DatabaseDescriptor.getEcDataNodes()) {
                if (cnt < THRESHOLD_OF_PADDING_ZERO_CHUNKS &&
                        StorageService.instance.globalPendingOldSSTableForECStripUpdateMap.size() < 50 ||
                        StorageService.instance.globalReadyOldSSTableForECStripUpdateCount > 0) {
                    cnt++;
                    logger.debug("ELECT-Debug: retry to perform erasure coding count is {}", cnt);
                } else {
                    // Padding zero chunk to consume the blocked sstables
                    cnt = 0;
                    logger.debug(
                            "ELECT-Debug: sstContents is not enough to do erasure coding, we need to padding zero: recvQueues size is {}",
                            StorageService.instance.globalRecvQueues.size());
                    while (StorageService.instance.globalRecvQueues.size() > 0) {
                        ECMessage tmpArray[] = new ECMessage[DatabaseDescriptor.getEcDataNodes()];
                        // traverse the recvQueues
                        int count = 0;
                        for (InetAddressAndPort addr : StorageService.instance.globalRecvQueues.keySet()) {
                            tmpArray[count] = ECNetutils.getDataBlockFromGlobalRecvQueue(addr);
                            count++;
                            if (count == DatabaseDescriptor.getEcDataNodes())
                                break;
                        }
                        // compute erasure coding locally;
                        int zeroChunkNum = 0;
                        if (count < DatabaseDescriptor.getEcDataNodes()) {
                            zeroChunkNum = DatabaseDescriptor.getEcDataNodes() - count;
                            for (int j = 0; j < zeroChunkNum; j++) {
                                byte[] newSSTContent = new byte[codeLength];
                                ECMessage zeroChunk = new ECMessage(newSSTContent,
                                        new ECMessageContent(
                                                ECNetutils.stringToHex(String.valueOf(newSSTContent.hashCode())),
                                                "ycsb", "usertable0",
                                                new ArrayList<InetAddressAndPort>()));
                                tmpArray[count] = zeroChunk;
                                count++;
                            }
                        }

                        if (count == DatabaseDescriptor.getEcDataNodes()) {
                            StorageService.instance.generatedPaddingZeroECMetadata++;
                            Stage.ERASURECODE.maybeExecuteImmediately(
                                    new PerformErasureCodeRunnable(tmpArray, codeLength, zeroChunkNum));
                        } else {
                            logger.debug("ELECT-Debug: Can not get enough data for erasure coding, tmpArray.length is {}.",
                                    count);
                        }
                    }
                }

            } else {

                while (StorageService.instance.globalRecvQueues.size() >= DatabaseDescriptor.getEcDataNodes()) {
                    logger.debug("ELECT-Debug: sstContents are enough to do erasure coding: recvQueues size is {}",
                            StorageService.instance.globalRecvQueues.size());
                    ECMessage[] tmpArray = new ECMessage[DatabaseDescriptor.getEcDataNodes()];
                    // traverse the recvQueues
                    int count = 0;
                    for (InetAddressAndPort addr : StorageService.instance.globalRecvQueues.keySet()) {
                        tmpArray[count] = ECNetutils.getDataBlockFromGlobalRecvQueue(addr);
                        count++;
                        if (count == DatabaseDescriptor.getEcDataNodes())
                            break;
                    }
                    // compute erasure coding locally;
                    if (count == DatabaseDescriptor.getEcDataNodes()) {
                        StorageService.instance.generatedNormalECMetadata++;
                        Stage.ERASURECODE
                                .maybeExecuteImmediately(new PerformErasureCodeRunnable(tmpArray, codeLength, 0));
                    } else {
                        logger.debug("ELECT-Debug: Can not get enough data for erasure coding, count is {}.", count);
                    }

                }
                cnt = 0;

            }
        }

    }

    private static void forwardToLocalNodes(Message<ECMessage> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECMessage> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECMessage> message = useSameMessageID ? builder.build() : null;

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
    private static class PerformErasureCodeRunnable implements Runnable {
        private final int ecDataNum;
        private final int ecParityNum;
        private final ECMessage[] messages;
        private final int codeLength;
        private final int zeroChunkNum;

        PerformErasureCodeRunnable(ECMessage[] message, int codeLength, int zeroChunkNum) {
            this.ecDataNum = DatabaseDescriptor.getEcDataNodes();
            this.ecParityNum = DatabaseDescriptor.getParityNodes();
            this.messages = message;
            this.codeLength = codeLength;
            this.zeroChunkNum = zeroChunkNum;
        }

        @Override
        public synchronized void run() {
            if (messages.length != ecDataNum) {
                logger.error("ELECT-ERROR: message length is not equal to ecDataNum");
            }
            // int codeLength = messages[0].sstSize;
            // for (ECMessage msg : messages) {
            // codeLength = codeLength < msg.sstSize? msg.sstSize : codeLength;
            // }

            long startTime = currentTimeMillis();
            ErasureCoderOptions ecOptions = new ErasureCoderOptions(ecDataNum, ecParityNum);
            ErasureEncoder encoder = new NativeRSEncoder(ecOptions);

            logger.debug("ELECT-Debug: let's start computing erasure coding");

            // Encoding input and output
            ByteBuffer[] data = new ByteBuffer[ecDataNum];
            ByteBuffer[] parity = new ByteBuffer[ecParityNum];

            // Prepare input data
            for (int i = 0; i < messages.length; i++) {
                if (messages[i].sstContent == null || codeLength < messages[i].sstContent.length) {
                    throw new IllegalStateException(String.format(
                            "ELECT-ERROR: sstable (%s) content is illegal, code length is (%s), content length is (%s)",
                            messages[i].ecMessageContent.sstHashID, codeLength, messages[i].sstContent.length));
                }
                data[i] = ByteBuffer.allocateDirect(codeLength);
                data[i].put(messages[i].sstContent);
                logger.debug("ELECT-Debug: remaining data is {}, codeLength is {}", data[i].remaining(), codeLength);
                int remaining = data[i].remaining();
                if (remaining > 0) {
                    byte[] zeros = new byte[remaining];
                    data[i].put(zeros);
                }
                // data[i].put(new byte[data[i].remaining()]);
                // logger.debug("ELECT-Debug: message[{}].sstconetent {}, data[{}] is: {}",
                // i, messages[i].sstContent,i, data[i]);
                data[i].rewind();
            }

            // Prepare parity data
            for (int i = 0; i < ecParityNum; i++) {
                parity[i] = ByteBuffer.allocateDirect(codeLength);
            }

            // Encode
            try {
                encoder.encode(data, parity);
            } catch (IOException e) {
                logger.error("ELECT-ERROR: Perform erasure code error", e);
            }
            long timeCost = currentTimeMillis() - startTime;
            StorageService.instance.encodingTime += timeCost;
            // generate parity hash code
            List<String> parityHashList = new ArrayList<String>();
            for (ByteBuffer parityCode : parity) {
                parityHashList.add(ECNetutils.stringToHex(String.valueOf(parityCode.hashCode())));
            }

            // record first parity code to current node
            String localParityCodeDir = ECNetutils.getLocalParityCodeDir();
            int needMirateParityCodeCount = ECNetutils.getNeedMigrateParityCodesCount();
            if ((DatabaseDescriptor.getEnableMigration() && DatabaseDescriptor.getTargetStorageSaving() > 0.45 &&
                    needMirateParityCodeCount > StorageService.instance.migratedParityCodeCount)
                    || DatabaseDescriptor.getStorageSavingGrade() >= 2) {

                for (int i = 0; i < parity.length; i++) {

                    byte[] parityInBytes = new byte[codeLength];
                    parity[i].get(parityInBytes);

                    long startUploadParityTime = System.currentTimeMillis();
                    if (!StorageService.ossAccessObj.uploadFileToOSS(localParityCodeDir + parityHashList.get(i),
                            parityInBytes)) {
                        logger.error("[ELECT]: Could not upload parity SSTable: {}",
                                localParityCodeDir + parityHashList.get(i));
                    } else {

                        StorageService.instance.migratedParityCodeCount++;
                        StorageService.instance.migratedParityCodes.add(parityHashList.get(i));
                    }
                    long uploadParityTimeCost = System.currentTimeMillis() - startUploadParityTime;
                    StorageService.instance.migratedParityCodeTimeCost += uploadParityTimeCost;

                }

            } else {

                try {
                    FileChannel fileChannel = FileChannel.open(Paths.get(localParityCodeDir, parityHashList.get(0)),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE);
                    fileChannel.write(parity[0]);
                    fileChannel.close();
                    // logger.debug("ELECT-Debug: parity code file created: {}",
                    // parityCodeFile.getName());
                } catch (IOException e) {
                    logger.error("ELECT-ERROR: Perform erasure code error", e);
                }
                // sync encoded data to parity nodes
                ECParityNode ecParityNode = new ECParityNode(null, null, 0);
                ecParityNode.distributeCodedDataToParityNodes(parity, messages[0].ecMessageContent.parityNodes,
                        parityHashList);

            }

            // Transform to ECMetadata and dispatch to related nodes
            // ECMetadata ecMetadata = new ECMetadata("", "", "", new
            // ArrayList<String>(),new ArrayList<String>(),
            // new ArrayList<InetAddressAndPort>(), new HashSet<InetAddressAndPort>(), new
            // HashMap<String, List<InetAddressAndPort>>());
            ECMetadata ecMetadata = new ECMetadata(
                    new ECMetadataContent("", "", "", new ArrayList<String>(), new ArrayList<String>(),
                            new ArrayList<InetAddressAndPort>(), new HashSet<InetAddressAndPort>(),
                            new ArrayList<InetAddressAndPort>(),
                            new HashMap<String, List<InetAddressAndPort>>(), "", false, 0, zeroChunkNum));
            ecMetadata.generateAndDistributeMetadata(messages, parityHashList);
        }

    }

}
