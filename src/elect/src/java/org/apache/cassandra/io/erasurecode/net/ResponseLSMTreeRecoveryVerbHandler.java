/*
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.io.File;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureDecoder;
import org.apache.cassandra.io.erasurecode.NativeRSDecoder;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.core.util.Time;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
public class ResponseLSMTreeRecoveryVerbHandler implements IVerbHandler<ResponseLSMTreeRecovery> {
    
    private static final Logger logger = LoggerFactory.getLogger(ResponseLSMTreeRecoveryVerbHandler.class);
    public static final ResponseLSMTreeRecoveryVerbHandler instance = new ResponseLSMTreeRecoveryVerbHandler();
    @Override
    public void doVerb(Message<ResponseLSMTreeRecovery> message) throws FileNotFoundException {
        

        String rawCfPath = message.payload.rawCfPath;
        String cfName = message.payload.cfName;

        logger.debug("ELECT-Debug: We receive a signal from ({}), start to decode the cfname ({})", message.from(), cfName);

        // calculate the copy file time
        long retrieveFileCost = currentTimeMillis() - StorageService.instance.recoveringCFS.get(cfName);

        long startDecodeTimeStamp = currentTimeMillis();
        // read the ec sstables and perform the recovery
        File dataFolder = new File(rawCfPath);
        int cnt = 0;
        if(cfName.equals("usertable0")) {
            if(dataFolder.exists() && dataFolder.isDirectory()) {
                File[] subDirectories = dataFolder.listFiles(File::isDirectory);
                if(subDirectories != null) {

                    for(File subDir : subDirectories) {
                        logger.debug("ELECT-Debug: current dir is ({})", subDir.getAbsolutePath());
                        File[] files = subDir.listFiles();
                        for(File file : files) {
                            if(file.isFile() && file.getName().contains("EC.db")) {
                                cnt++;
                                try {
                                    Stage.RECOVERY.maybeExecuteImmediately(new RecoveryForLSMTreeRunnable(file.getAbsolutePath(), subDir.getAbsolutePath()));
                                    // recoveryDataFromErasureCodesForLSMTree(file.getAbsolutePath(), subDir.getAbsolutePath());
                                    // if(cnt % 35 == 0) {
                                    //     Thread.sleep(10000);
                                    // }
                                    // Time.sleep(1, TimeUnit.SECONDS);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                        }
                    }

                }

            } else {
                throw new FileNotFoundException(String.format("ELECT-ERROR: Folder for path (%s) does not exists!", rawCfPath));
            }
        } else {
            logger.debug("ELECT-Debug: For the secondary LSM-tree ({}), we do not decode them.", cfName);
        }

        long endDecodeTimeStamp = currentTimeMillis();

        // caculate the decode time
        long decodeTimeCost = endDecodeTimeStamp - startDecodeTimeStamp;
        
        logger.debug("ELECT-Debug: We recovery the colum family ({}), the retrieve file cost is {}(ms), decode time cost is {}(ms), the decoded sstable count is ({})", 
                     cfName, retrieveFileCost, decodeTimeCost, cnt);

        if(cfName.equals("usertable0")) {
            String data = String.format("TableName: %s\nRetrieve File Cost: %s(ms)\nDecode Time Cost: %s(ms)\nDecode SSTables Count: %s(ms)\n\n\n",
                                        cfName, retrieveFileCost, decodeTimeCost, cnt);
            ECNetutils.recordResults(ECNetutils.getFullNodeRecoveryLogFile(), data);
            LSMTreeRecovery.recoveryLSMTree("ycsb", "usertable1");
        } else if (cfName.equals("usertable1")) {
            String data = String.format("TableName: %s\nRetrieve File Cost: %s(ms)\nDecode Time Cost: %s(ms)\nDecode SSTables Count: %s\n\n\n",
                                        cfName, retrieveFileCost, decodeTimeCost, cnt);
            ECNetutils.recordResults(ECNetutils.getFullNodeRecoveryLogFile(), data);
            LSMTreeRecovery.recoveryLSMTree("ycsb", "usertable2");
        } else if (cfName.equals("usertable2")) {
            String data = String.format("TableName: %s\nRetrieve File Cost: %s(ms)\nDecode Time Cost: %s(ms)\nDecode SSTables Count: %s(ms)\n\n\n",
                                        cfName, retrieveFileCost, decodeTimeCost, cnt);
            ECNetutils.recordResults(ECNetutils.getFullNodeRecoveryLogFile(), data);
        }

    }


    private static class RecoveryForLSMTreeRunnable implements Runnable {

        private final String ecMetadataFile;
        private final String cfPath;

        public RecoveryForLSMTreeRunnable(String ecMetadataFile, String cfPath) {
            this.ecMetadataFile = ecMetadataFile;
            this.cfPath = cfPath;
        }

        @Override
        public void run() {
            try {
                recoveryDataFromErasureCodesForLSMTree(ecMetadataFile, cfPath);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


    private static void recoveryDataFromErasureCodesForLSMTree(String ecMetadataFile, String cfPath) throws Exception {
        int k = DatabaseDescriptor.getEcDataNodes();
        int m = DatabaseDescriptor.getParityNodes();
        logger.debug("ELECT-Debug: start recovery ecMetadata ({})", ecMetadataFile);
        // Step 1: Get the ECSSTable from global map and get the ecmetadata
        byte[] ecMetadataInBytes = ECNetutils.readBytesFromFile(ecMetadataFile);
        logger.debug("ELECT-Debug: [Debug recovery] the size of ecMetadataInBytes is ({})", ecMetadataInBytes.length);
        long startRecoveryTime = System.currentTimeMillis();
        ECMetadataContent ecMetadataContent = (ECMetadataContent) ByteObjectConversion.byteArrayToObject(ecMetadataInBytes);
        if(ecMetadataContent == null)
            throw new NullPointerException(String.format("ELECT-Debug: [Debug recovery] The ecMetadata for ecMetadataFile ({}) is null!", ecMetadataFile));

        
        // Step 2: Request the coding blocks from related nodes
        InetAddressAndPort localIp = FBUtilities.getBroadcastAddressAndPort();
        int index = ecMetadataContent.primaryNodes.indexOf(localIp);
        if(index == -1)
            throw new NullPointerException(String.format("ELECT-ERROR: we cannot get index from primary node list", ecMetadataContent.primaryNodes));
        String sstHash = ecMetadataContent.sstHashIdList.get(index);       
        int codeLength = StorageService.getErasureCodeLength();
        logger.debug("ELECT-Debug: [Debug recovery] retrieve chunks for sstable ({})", sstHash);
        
        long startRetrieveTime = System.currentTimeMillis();
        ECRecovery.retrieveErasureCodesForRecovery(ecMetadataContent, sstHash, codeLength, k, m);
        logger.debug("ELECT-Debug: [Debug recovery] retrieve chunks for ecmetadata ({}) successfully", sstHash);

        // ByteBuffer[] recoveryOriginalSrc = StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash);

        if(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash) == null) {
            throw new NullPointerException(String.format("ELECT-ERROR: we cannot get erasure code for sstable (%s)", sstHash));
        }

        logger.debug("ELECT-Debug: [Debug recovery] wait chunks for sstable ({})", sstHash);
        int[] decodeIndexes = ECRecovery.waitUntilRequestCodesReady(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash), sstHash, k, ecMetadataContent.zeroChunksNum, codeLength);

        long retrieveTimeCost = System.currentTimeMillis() - startRetrieveTime;
        StorageService.instance.retrieveChunksTimeForLSMTreeRecovery += retrieveTimeCost;

        int eraseIndex = ecMetadataContent.sstHashIdList.indexOf(sstHash);
        if(eraseIndex == -1)
            throw new NullPointerException(String.format("ELECT-ERROR: we cannot get index for sstable (%s) in ecMetadata, sstHash list is ({})", sstHash, ecMetadataContent.stripeId, ecMetadataContent.sstHashIdList));
        int[] eraseIndexes = { eraseIndex };

        logger.debug("ELECT-Debug: [Debug recovery] When we recovery sstable ({}), the data blocks of strip id ({}) is ready.", sstHash, ecMetadataContent.stripeId);


        // Step 3: Decode the raw data

        ByteBuffer[] recoveryOriginalSrc = new ByteBuffer[k];

        for(int i = 0; i < k; i++) {
            recoveryOriginalSrc[i] = ByteBuffer.allocateDirect(codeLength);
            recoveryOriginalSrc[i].put(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[decodeIndexes[i]]);
            recoveryOriginalSrc[i].rewind();
        }

        StorageService.instance.globalSSTHashToErasureCodesMap.remove(sstHash);
        ErasureCoderOptions ecOptions = new ErasureCoderOptions(k, m);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);
        ByteBuffer[] output = new ByteBuffer[1];
        output[0] = ByteBuffer.allocateDirect(codeLength);
        decoder.decode(recoveryOriginalSrc, decodeIndexes, eraseIndexes, output);

        logger.debug("ELECT-Debug: [Debug recovery] We recovered the raw data of sstable ({}) successfully.", sstHash);


        // Step 4: record the raw data locally
        String[] parts = ecMetadataFile.split("/");
        String sstableId = parts[parts.length - 1].split(".")[0].split("-")[1];
        String statsMetadataFileName = cfPath + "nb-" + sstableId + "-big-Statistics.db";
        String dataFileName = cfPath + "nb-" + sstableId + "-big-Data.db";
        byte[] statsMetadatInBytes = ECNetutils.readBytesFromFile(statsMetadataFileName);
        StatsMetadata statsMetadata = (StatsMetadata) ByteObjectConversion.byteArrayToObject(statsMetadatInBytes);

        int dataFileSize = (int) statsMetadata.dataFileSize;
        // logger.debug("ELECT-Debug: [Debug recovery] we load the raw sstable content of ({}), the length of decode data is ({}), sstHash is ({}), the data file size is ({}) ", sstable.descriptor, output[0].remaining(), sstable.getSSTableHashID(), dataFileSize);
        byte[] sstContent = new byte[dataFileSize];
        output[0].get(sstContent);
        ECNetutils.writeBytesToFile(dataFileName, sstContent);
        // SSTableReader.loadRawData(sstContent, sstable.descriptor, sstable);

        // debug
        // logger.debug("ELECT-Debug: Recovery sstHashList is ({}), parity hash list is ({}), stripe id is ({}), sstHash to replica map is ({}), sstable hash is ({}), descriptor is ({}), decode indexes are ({}), erase index is ({}), zero chunks are ({})", 
        //              ecMetadataContent.sstHashIdList, ecMetadataContent.parityHashList, ecMetadataContent.stripeId, ecMetadataContent.sstHashIdToReplicaMap, sstable.getSSTableHashID(), sstable.descriptor, decodeIndexes, eraseIndex, ecMetadataContent.zeroChunksNum);


        // Step 5: send the raw data to the peer secondary nodes
        // List<InetAddressAndPort> replicaNodes = ecMetadataContent.sstHashIdToReplicaMap.get(sstHash);
        // if(replicaNodes == null) {
        //     throw new NullPointerException(String.format("ELECT-ERROR: we cannot get replica nodes for sstable (%s)", sstHash));
        // }

        // Step 6: send the raw data to the peer secondary nodes
        // InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        // for (int i = 1; i< replicaNodes.size(); i++) {
        //     if(!replicaNodes.get(i).equals(localIP)){
        //         String cfName = "usertable" + i;
        //         ECRecoveryForSecondary recoverySignal = new ECRecoveryForSecondary(sstHash, sstContent, cfName);
        //         recoverySignal.sendDataToSecondaryNode(replicaNodes.get(i));
        //     }
        // }

        // TODO: Wait until all data is ready.
        // Thread.sleep(5000);
        long recoveryTimeCost = System.currentTimeMillis() - startRecoveryTime;
        StorageService.instance.recoveryTimeForLSMTreeRecovery += recoveryTimeCost;
        logger.debug("ELECT-Debug: recovery for sstHash ({}) is done!", sstHash);
    }



}
