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

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECRequestParity {

    private static final Logger logger = LoggerFactory.getLogger(ECRequestParity.class);
    public static final Serializer serializer = new Serializer();
    public final String parityHash;
    public final String sstHash;
    public final int parityIndex;
    public final boolean isRecovery;
    public final String requestNode;

    public ECRequestParity(String parityHash, String sstHash, int parityIndex, boolean isRecovery, String requestNode) {
        this.parityHash = parityHash;
        this.sstHash = sstHash;
        this.parityIndex = parityIndex;
        this.isRecovery = isRecovery;
        this.requestNode = requestNode;
    }

    public void requestParityCode(InetAddressAndPort target) {
        logger.debug("ELECT-Debug: Request parity code {} from {} to update the old sstable {}",
                this.parityHash, target, this.sstHash);
        Message<ECRequestParity> message = Message.outWithFlag(Verb.ECREQUESTPARITY_REQ, this,
                MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);
    }

    public static final class Serializer implements IVersionedSerializer<ECRequestParity> {

        @Override
        public void serialize(ECRequestParity t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.parityHash);
            out.writeUTF(t.sstHash);
            out.writeInt(t.parityIndex);
            out.writeBoolean(t.isRecovery);
            out.writeUTF(t.requestNode);
        }

        @Override
        public ECRequestParity deserialize(DataInputPlus in, int version) throws IOException {
            String parityHash = in.readUTF();
            String sstHash = in.readUTF();
            int parityIndex = in.readInt();
            boolean isRecovery = in.readBoolean();
            String requestNode = in.readUTF();
            return new ECRequestParity(parityHash, sstHash, parityIndex, isRecovery, requestNode);
        }

        @Override
        public long serializedSize(ECRequestParity t, int version) {
            long size = sizeof(t.parityHash) +
                    sizeof(t.sstHash) +
                    sizeof(t.parityIndex) +
                    sizeof(t.isRecovery) +
                    sizeof(t.requestNode);

            return size;
        }

    }

}
