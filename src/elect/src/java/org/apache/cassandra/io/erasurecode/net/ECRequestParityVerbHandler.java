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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.alibaba.OSSAccess;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECRequestParityVerbHandler implements IVerbHandler<ECRequestParity> {
    public static final ECRequestParityVerbHandler instance = new ECRequestParityVerbHandler();
    private static final int MAX_RETRY_COUNT = 5;

    private static final Logger logger = LoggerFactory.getLogger(ECRequestParityVerbHandler.class);

    @Override
    public void doVerb(Message<ECRequestParity> message) throws IOException {

        String parityHash = message.payload.parityHash;
        String sstHash = message.payload.sstHash;
        int parityIndex = message.payload.parityIndex;
        boolean isRecovery = message.payload.isRecovery;
        String requestNode = message.payload.requestNode;
        String receivedParityCodeDir = ECNetutils.getReceivedParityCodeDir();
        String filePath = receivedParityCodeDir + parityHash;
        

        byte[] parityCode;

        logger.debug("ELECT-Debug: get a request parity code ({}) message from node ({})", parityHash, message.from());
        // String filePath = "/path/to/file.txt";
        Path path = Paths.get(filePath);
        int retryCount = 0;
        while (retryCount < MAX_RETRY_COUNT) {
            if (Files.exists(path)) {
                break;
            }
            else {
                try {
                    Thread.sleep(1000);
                    retryCount++;
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    break;
                }
            }
        }

        // if(!Files.exists(path)) {
        // ECNetutils.retrieveDataFromCloud("127.0.0.1",
        // message.from().getHostAddress(false), "cfName", parityHash, filePath);
        // }
        // if (DatabaseDescriptor.getEnableMigration()) {
        //     if (!StorageService.ossAccessObj.downloadFileFromOSS(filePath, filePath)) {
        //         logger.error("[ELECT]: Could not download parity SSTable: {}",
        //                 filePath);
        //     }
        // }

        if (!Files.exists(path)) {
            throw new IllegalStateException(String.format(
                    "ELECT-ERROR: we cannot find parity code file %s requested from %s", filePath, message.from()));
        }

        try {
            parityCode = ECNetutils.readBytesFromFile(filePath);
            // response this parityCode to to source node
            ECResponseParity response = new ECResponseParity(parityHash, sstHash, parityCode, parityIndex, isRecovery);

            if(requestNode == null)
                response.responseParity(message.from());
            else
                response.responseParity(InetAddressAndPort.getByName(requestNode));

            // delete parity code file locally if it is parity update
            if (!isRecovery)
                ECNetutils.deleteFileByName(filePath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("ELECT-ERROR: failed to find parity code file {} requested from {}", filePath, message.from());
            e.printStackTrace();
        }

    }

}
