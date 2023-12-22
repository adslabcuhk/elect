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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.xml.crypto.Data;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureDecoder;
import org.apache.cassandra.io.erasurecode.NativeRSDecoder;
import org.apache.cassandra.io.erasurecode.alibaba.OSSAccess;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.compiler.STParser.compoundElement_return;

public class ECRecovery {
    private static final Logger logger = LoggerFactory.getLogger(ECRecovery.class);
    public static final ECRecovery instance = new ECRecovery();

    public static void recoveryDataFromErasureCodes(final String sstHash, CountDownLatch latch) throws Exception {

        if(ECNetutils.getIsRecovered(sstHash))
            return;

        logger.debug("ELECT-Debug: [Debug recovery] This is recovery for sstHash ({})", sstHash);

        int k = DatabaseDescriptor.getEcDataNodes();
        int m = DatabaseDescriptor.getParityNodes();

        // Step 1: Get the ECSSTable from global map and get the ecmetadata
        SSTableReader sstable = StorageService.instance.globalSSTHashToECSSTableMap.get(sstHash);
        if (sstable == null)
            throw new NullPointerException(String.format("ELECT-ERROR: Cannot get ECSSTable (%s)", sstHash));

        String ecMetadataFile = sstable.descriptor.filenameFor(Component.EC_METADATA);

        byte[] ecMetadataInBytes = ECNetutils.readBytesFromFile(ecMetadataFile);
        logger.debug("ELECT-Debug: [Debug recovery] the size of ecMetadataInBytes for sstHash ({}) is ({})", sstHash,
                ecMetadataInBytes.length);
        ECMetadataContent ecMetadataContent = (ECMetadataContent) ByteObjectConversion
                .byteArrayToObject(ecMetadataInBytes);
        if (ecMetadataContent == null)
            throw new NullPointerException(
                    String.format("ELECT-Debug: [Debug recovery] The ecMetadata for sstHash ({}) is null!", sstHash));

        logger.debug("ELECT-Debug: [Debug recovery] read ecmetadata ({}) for old sstable ({})", ecMetadataContent.stripeId,
                sstHash);

        long startRetrieveTime = System.currentTimeMillis();
        // Step 2: Request the coding blocks from related nodes
        int codeLength = StorageService.getErasureCodeLength();
        logger.debug("ELECT-Debug: [Debug recovery] retrieve chunks for sstable ({})", sstHash);
        retrieveErasureCodesForRecovery(ecMetadataContent, sstHash, codeLength, k, m);
        logger.debug("ELECT-Debug: [Debug recovery] retrieve chunks for ecmetadata ({}) successfully", sstHash);

        // ByteBuffer[] recoveryOriginalSrc =
        // StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash);

        if (StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash) == null) {
            throw new NullPointerException(
                    String.format("ELECT-ERROR: we cannot get erasure code for sstable (%s)", sstHash));
        }

        logger.debug("ELECT-Debug: [Debug recovery] wait chunks for sstable ({})", sstHash);
        int[] decodeIndexes = waitUntilRequestCodesReady(
                StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash), sstHash, k,
                ecMetadataContent.zeroChunksNum, codeLength);
        long retrieveTimeCost = System.currentTimeMillis() - startRetrieveTime;
        StorageService.instance.degradedRetrieveTime += retrieveTimeCost;

        int eraseIndex = ecMetadataContent.sstHashIdList.indexOf(sstHash);
        if (eraseIndex == -1)
            throw new NullPointerException(
                    String.format("ELECT-ERROR: we cannot get index for sstable (%s) in ecMetadata, sstHash list is ({})",
                            sstHash, ecMetadataContent.stripeId, ecMetadataContent.sstHashIdList));
        int[] eraseIndexes = { eraseIndex };

        logger.debug(
                "ELECT-Debug: [Debug recovery] When we recovery sstable ({}), the data blocks of strip id ({}) is ready.",
                sstHash, ecMetadataContent.stripeId);


    
        // Step 3: Decode the raw data

        long startTime = System.currentTimeMillis();

        ByteBuffer[] recoveryOriginalSrc = new ByteBuffer[k];

        for (int i = 0; i < k; i++) {
            recoveryOriginalSrc[i] = ByteBuffer.allocateDirect(codeLength);
            recoveryOriginalSrc[i]
                    .put(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[decodeIndexes[i]]);
            recoveryOriginalSrc[i].rewind();
        }

        ErasureCoderOptions ecOptions = new ErasureCoderOptions(k, m);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);
        ByteBuffer[] output = new ByteBuffer[1];
        output[0] = ByteBuffer.allocateDirect(codeLength);
        decoder.decode(recoveryOriginalSrc, decodeIndexes, eraseIndexes, output);

        logger.debug("ELECT-Debug: [Debug recovery] We recovered the raw data of sstable ({}) successfully.", sstHash);

        // Step 4: record the raw data locally
        int dataFileSize = (int) sstable.getDataFileSize();
        logger.debug(
                "ELECT-Debug: [Debug recovery] we load the raw sstable content of ({}), the length of decode data is ({}), sstHash is ({}), the data file size is ({}) ",
                sstable.descriptor, output[0].remaining(), sstable.getSSTableHashID(), dataFileSize);
        byte[] sstContent = new byte[dataFileSize];
        output[0].get(sstContent);
        SSTableReader.loadRawData(sstContent, sstable.descriptor, sstable);


        long decodeCostTime = System.currentTimeMillis() - startTime;
        StorageService.instance.degradedReadDecodingTime += decodeCostTime;
        // debug
        logger.debug(
                "ELECT-Debug: Recovery sstHashList is ({}), parity hash list is ({}), stripe id is ({}), sstHash to replica map is ({}), sstable hash is ({}), descriptor is ({}), decode indexes are ({}), erase index is ({}), zero chunks are ({})",
                ecMetadataContent.sstHashIdList, ecMetadataContent.parityHashList, ecMetadataContent.stripeId,
                ecMetadataContent.sstHashIdToReplicaMap, sstable.getSSTableHashID(), sstable.descriptor, decodeIndexes,
                eraseIndex, ecMetadataContent.zeroChunksNum);

        // Step 5: send the raw data to the peer secondary nodes
        List<InetAddressAndPort> replicaNodes = ecMetadataContent.sstHashIdToReplicaMap.get(sstHash);
        if (replicaNodes == null) {
            throw new NullPointerException(
                    String.format("ELECT-ERROR: we cannot get replica nodes for sstable (%s)", sstHash));
        }

        // Step 6: send the raw data to the peer secondary nodes
        // InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        // for (int i = 1; i< replicaNodes.size(); i++) {
        // if(!replicaNodes.get(i).equals(localIP)){
        // String cfName = "usertable" + i;
        // ECRecoveryForSecondary recoverySignal = new ECRecoveryForSecondary(sstHash,
        // sstContent, cfName);
        // recoverySignal.sendDataToSecondaryNode(replicaNodes.get(i));
        // }
        // }

        // TODO: Wait until all data is ready.
        // Thread.sleep(5000);
        // remove the sstHash
        StorageService.instance.globalSSTHashToErasureCodesMap.remove(sstHash);
        logger.debug("ELECT-Debug: recovery for sstHash ({}) is done!", sstHash);
        StorageService.instance.recoveringSSTables.remove(sstHash);
        latch.countDown();
    }

    /**
     * This method is called when we decide to save a EC strip update signal to the
     * process queue.
     * 
     * @param oldSSTHash
     * @param stripID
     * @param codeLength
     */
    public static void retrieveErasureCodesForRecovery(ECMetadataContent ecMetadataContent, String oldSSTHash,
            int codeLength, int k, int m) {
        logger.debug(
                "ELECT-Debug: [Debug recovery] Initialize erasure codes for recovery, code length is ({}), k is ({}), m is ({}).",
                codeLength, k, m);
        // Step 0: Initialize the data and parity blocks
        ByteBuffer[] erasureCodes = new ByteBuffer[k + m];
        for (int i = 0; i < k + m; i++) {
            erasureCodes[i] = ByteBuffer.allocateDirect(codeLength);
        }

        logger.debug("ELECT-Debug: [Debug recovery] retrieve data chunks for sstable ({})", oldSSTHash);
        // Step 1: Retrieve the data blocks.
        StorageService.instance.globalSSTHashToErasureCodesMap.put(oldSSTHash, erasureCodes);
        if (ecMetadataContent.sstHashIdToReplicaMap != null) {
            // int index = 0;
            for (Map.Entry<String, List<InetAddressAndPort>> entry : ecMetadataContent.sstHashIdToReplicaMap
                    .entrySet()) {

                if (entry.getKey().equals(oldSSTHash))
                    continue;
                int index = ecMetadataContent.sstHashIdList.indexOf(entry.getKey());
                if (index == -1)
                    throw new NullPointerException(String.format(
                            "ELECT-ERROR: we cannot get index for sstable (%s) in ecMetadata, sstHash list is ({})",
                            oldSSTHash, ecMetadataContent.stripeId, ecMetadataContent.sstHashIdList));
                ECRequestData request = new ECRequestData(oldSSTHash, entry.getKey(), index);
                logger.debug("ELECT-Debug: Recovery sstable ({}), we request raw data of ({}) from nodes ({}), all replication nodes are ({})", 
                             oldSSTHash, entry.getKey(), entry.getValue().get(0), entry.getValue());
                request.requestData(entry.getValue().get(0));
                // index++;
            }
        } else {
            throw new IllegalArgumentException(String.format("ELECT-ERROR: sstHashIDToReplicaMap is null!"));
        }

        logger.debug("ELECT-Debug: [Debug recovery] retrieve parity chunks for sstable ({})", oldSSTHash);
        // Step 2: Retrieve parity blocks.
        if (ecMetadataContent.parityHashList == null ||
                ecMetadataContent.parityNodes == null) {
            ECNetutils.printStackTace(String.format(
                    "ELECT-ERROR: When we are recovery old sstable (%s), we cannot to get parity hash or parity code for stripID (%s)",
                    oldSSTHash, ecMetadataContent.stripeId));
        } else {

            // send recovery signal to the first parity node
            ECRequestParityForRecovery signal = new ECRequestParityForRecovery(oldSSTHash, ecMetadataContent.parityHashList, ecMetadataContent.parityNodes);
            signal.requestParityCodesForRecovery(ecMetadataContent.parityNodes.get(0));
            
        }

    }

    // [WARNING!] Make sure to avoid dead loops
    public static int[] waitUntilRequestCodesReady(ByteBuffer[] buffers, String oldSSTHash, int k, int zeroChunkNum,
            int codeLength) {
        int retryCount = 0;
        int[] decodeIndexes = new int[k];
        if (buffers != null) {
            while (!checkCodesAreReady(buffers, k - zeroChunkNum, oldSSTHash)) {
                try {
                    if (retryCount < 5000) {
                        Thread.sleep(10);
                        retryCount++;
                    } else {
                        throw new IllegalStateException(String
                                .format("ELECT-ERROR: cannot retrieve the remote codes for sstHash (%s)", oldSSTHash));
                    }

                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    break;
                }
            }
        } else {
            throw new NullPointerException(
                    String.format("ELECT-ERROR: We cannot get parity codes for sstable %s", oldSSTHash));
        }

        int j = 0;
        for (int i = 0; i < buffers.length; i++) {
            if (j >= k) {
                break;
            }

            if (buffers[i].position() != 0) {
                buffers[i].rewind();
                decodeIndexes[j++] = i;
            }
        }

        // padding zero
        for (int i = k - zeroChunkNum; i < buffers.length && j < k; i++) {
            byte[] zeroChunk = new byte[codeLength];
            buffers[i].put(zeroChunk);
            buffers[i].rewind();
            decodeIndexes[j++] = i;
        }

        return decodeIndexes;
    }

    private static boolean checkCodesAreReady(ByteBuffer[] checkBuffers, int chunkNum, String sstHash) {
        int readyBlocks = 0;
        for (ByteBuffer buf : checkBuffers) {
            if (buf.position() != 0) {
                readyBlocks++;
            }
            if (readyBlocks >= chunkNum) {
                logger.debug("ELECT-Debug: [Debug recovery] The erasure code for sstable ({}) is ready!", sstHash);
                return true;
            }
        }
        logger.debug("ELECT-Debug: [Debug recovery] checkCodesAreReady, ready blocks for sstable ({}) are ({})", sstHash, readyBlocks);
        return false;
    }

}
