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
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.alibaba.OSSAccess;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECRequestParityForRecoveryVerbHandler implements IVerbHandler<ECRequestParityForRecovery> {
    public static final ECRequestParityForRecoveryVerbHandler instance = new ECRequestParityForRecoveryVerbHandler();
    private static final int MAX_RETRY_COUNT = 5;

    private static final Logger logger = LoggerFactory.getLogger(ECRequestParityForRecoveryVerbHandler.class);

    @Override
    public void doVerb(Message<ECRequestParityForRecovery> message) throws IOException {

        List<String> parityHashList = message.payload.parityHashList;
        String sstHash = message.payload.sstHash;
        List<InetAddressAndPort>  parityNodes = message.payload.parityNodeList;
        int k = DatabaseDescriptor.getEcDataNodes();

        String localParityCodeDir = ECNetutils.getLocalParityCodeDir();

        // Parity codes are migrated
        if (DatabaseDescriptor.getEnableMigration() && DatabaseDescriptor.getTargetStorageSaving() > 0.45 && 
            ECNetutils.checkIsParityCodeMigrated(parityHashList.get(0))) {

            // send a signal back to the requested node, let that node download parity code from cloud
            ECResponseParityForRecovery signal = new ECResponseParityForRecovery(sstHash, parityHashList);
            signal.responseParityCodesForRecovery(message.from());

            return;
            
        } else {
            // Parity codes are normally distributed
            // first get local parity codes
            try {
                String parityCodeFileName = localParityCodeDir + parityHashList.get(0);
                logger.debug("ELECT-Debug: Read parity code ({}) locally for recovery, the file is exists? ({})", parityCodeFileName, Files.exists(Paths.get(parityCodeFileName)));
                // send back to the requested node
                byte[] parityCode = ECNetutils.readBytesFromFile(parityCodeFileName);
                ECResponseParity response = new ECResponseParity(parityHashList.get(0), sstHash, parityCode, k, true);
                response.responseParity(message.from());

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            // get the needed parity code remotely, send a parity code request
            logger.debug("ELECT-Debug: Recovery stage, the parity codes are ({})", parityHashList);
            for (int i = 1; i < parityHashList.size(); i++) {
                logger.debug("ELECT-Debug: Recovery stage, request parity code ({}) for sstable ({}) from parity node ({})", parityHashList.get(i), sstHash, parityNodes.get(i));
                ECRequestParity request = new ECRequestParity(parityHashList.get(i), 
                                                              sstHash, 
                                                              i + k, 
                                                              true, 
                                                              message.from().getHostAddress(false));
                if(parityNodes.get(i) instanceof InetAddressAndPort)
                    request.requestParityCode(parityNodes.get(i));
                else {
                    throw new IllegalStateException(String.format("ELECT-ERROR: the parity node (%s) is not type of InetAddressAndPort", parityNodes.get(i)));
                }
            }
        }

    }

}
