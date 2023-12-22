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

public class ECResponseDataVerbHandler implements IVerbHandler<ECResponseData> {
    public static final ECResponseDataVerbHandler instance = new ECResponseDataVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECResponseDataVerbHandler.class);

    @Override
    public void doVerb(Message<ECResponseData> message) throws IOException {
        String sstHash = message.payload.sstHash;
        byte[] rawData = message.payload.rawData;
        int index = message.payload.index;

        // save it to the map
        if (StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash) != null) {
            if (index > -1 && index < DatabaseDescriptor.getEcDataNodes()) {
                if (StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[index] != null &&
                        StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[index]
                                .remaining() >= rawData.length) {
                    StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[index].put(rawData);
                    logger.debug("ELECT-Debug: get raw data from ({}) for sstable hash ({}), index is ({})",
                            message.from(), sstHash, index);
                } else {
                    throw new IllegalStateException(String.format(
                            "ELECT-ERROR: get an outbound index (%s) when we recovery sstHash (%s), the size of erasure codes (%s), the size of the buffer (%s), raw data length is (%s)",
                            index, sstHash, StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash).length,
                            StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[index].position(),
                            rawData.length));
                }
            } else {
                throw new IllegalStateException(String
                        .format("ELECT-ERROR: get an outbound index (%s) when we recovery sstHash (%s)", index, sstHash));
            }
        } else {
            throw new NullPointerException(
                    String.format("ELECT-ERROR: We cannot find erasure codes for sstable (%s), the request is from (%s)",
                            sstHash, message.from()));
        }

    }

}
