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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECResponseParityForRecoveryVerbHandler implements IVerbHandler<ECResponseParityForRecovery>{
    public static final ECResponseParityForRecoveryVerbHandler instance = new ECResponseParityForRecoveryVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ECResponseParityForRecoveryVerbHandler.class);
    @Override
    public void doVerb(Message<ECResponseParityForRecovery> message) throws IOException {
        String sstHash = message.payload.sstHash;
        List<String> parityHashList = message.payload.parityHashList;
        String firstParityNode = message.from().getHostAddress(false);
        String localParityCodeDir = ECNetutils.getLocalParityCodeDir();
        int k = DatabaseDescriptor.getEcDataNodes();

        logger.debug("ELECT-Debug: Get parity code ({}) from ({}) for sstable ({}), the recovery flag is ({})", parityHashList, message.from(), sstHash);


        for(int i = 0; i < parityHashList.size(); i++) {
            String parityCodeFileName = localParityCodeDir + parityHashList.get(i);

            int retryDownloadCount = 0;
            long startDownloadTime = System.currentTimeMillis();
            while (retryDownloadCount < ECNetutils.getMigrationRetryCount()) {
                if (StorageService.ossAccessObj.downloadFileAsByteArrayFromOSS(parityCodeFileName, firstParityNode)) {
                    break;
                }
                retryDownloadCount++;
            }

            int MAX_RETRY_COUNT = 5;
            Path path = Paths.get(parityCodeFileName);
            int retryCheckFileCount = 0;
            while (retryCheckFileCount < MAX_RETRY_COUNT) {
                if (Files.exists(path) && Files.size(path) == (StorageService.getErasureCodeLength())) {
                    break;
                }
                else {
                    try {
                        Thread.sleep(1000);
                        logger.debug("ELECT-Debug: Check the downloaded file is ready? For parity code ({}), the size is ({})", parityCodeFileName, Files.size(path) );
                        retryCheckFileCount++;
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        break;
                    }
                }
            }
            long  downloadTimeCost = System.currentTimeMillis() - startDownloadTime;
            StorageService.instance.readParityMigrationTime += downloadTimeCost;

            // if(StorageService.ossAccessObj.downloadFileAsByteArrayFromOSS(parityCodeFileName, firstParityNode)) {
            byte[] parityCode = ECNetutils.readBytesFromFile(parityCodeFileName);
            if(parityCode.length == 0)
                throw new IllegalArgumentException(String.format("ELECT-ERROR: cannot read data from parity code file (%s)", parityCodeFileName));

            if(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash) == null) {
                // throw new IllegalArgumentException(String.format("ELECT-ERROR: The erasure codes for sstHash (%s) is empty", sstHash));
                logger.debug("ELECT-Debug: The erasure codes for sstHash ({}) is empty", sstHash);
                return;
            }

            if(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[k + i].position() != 0) {
                throw new IllegalArgumentException(String.format("ELECT-ERROR: The erasure codes index (%s) for sstHash (%s) is not empty", k + i, sstHash));
            }


            StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[k + i].put(parityCode);
            logger.debug("ELECT-Debug: Put parity code ({}) for sstable ({}), the index is ({})", parityCodeFileName, sstHash, k+i);
            // } else {
            //     throw new FileNotFoundException(String.format("ELECT-ERROR: cannot download file (%s) from cloud", parityCodeFileName));
            // }
        }

    }

}
