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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECResponseParityVerbHandler implements IVerbHandler<ECResponseParity>{
    public static final ECResponseParityVerbHandler instance = new ECResponseParityVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ECResponseParityVerbHandler.class);
    @Override
    public void doVerb(Message<ECResponseParity> message) throws IOException {
        String sstHash = message.payload.sstHash;
        String parityHash = message.payload.parityHash;
        byte[] parityCode = message.payload.parityCode;
        int parityIndex = message.payload.parityIndex;
        boolean isRecovery = message.payload.isRecovery;

        logger.debug("ELECT-Debug: Get parity code ({}) from ({}) for sstable ({}), the recovery flag is ({})", parityHash, message.from(), sstHash, isRecovery);

        // save it to the map for stripe update
        if(!isRecovery) {
            if(StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash) != null) {
                StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash)[parityIndex].put(parityCode);
                // StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash)[parityIndex].rewind(); 
                logger.debug("ELECT-Debug: Get parity code ({}) from ({}) for update sstable ({})", parityHash, message.from(), sstHash);
            } else {
                throw new NullPointerException(String.format("ELECT-ERROR: We receive parity code (%s) from (%s), but cannot find parity codes for sstable (%s)", parityHash, message.from(), sstHash));
            }
        } else {
            if(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash) != null) {
                if(parityIndex >= DatabaseDescriptor.getEcDataNodes() && parityIndex < DatabaseDescriptor.getEcDataNodes() + DatabaseDescriptor.getParityNodes()) {
                    if(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[parityIndex] != null &&
                       StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[parityIndex].remaining() >= parityCode.length) {
                        StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[parityIndex].put(parityCode);
                        logger.debug("ELECT-Debug: Get parity code ({}) from ({}) for recovery sstable ({})", parityHash, message.from(), sstHash);
                    } else {
                        throw new IllegalStateException(String.format("ELECT-ERROR: get an outbound index (%s) when we recovery sstHash (%s), the size of erasure codes (%s), buffer size is (%s), parity code size is (%s)", 
                                                                      parityIndex, sstHash, StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash).length, StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[parityIndex].position(), parityCode.length));
                    }
                } else {
                    throw new IllegalStateException(String.format("ELECT-ERROR: get an outbound index (%s) when we recovery sstHash (%s)", parityIndex, sstHash));
                }
                // StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash)[parityIndex].rewind(); 
            } else {
                throw new NullPointerException(String.format("ELECT-ERROR: We receive parity code (%s) from (%s), but cannot find erasure codes for sstable (%s)", parityHash, message.from(), sstHash));
            }
        }

    }

}
