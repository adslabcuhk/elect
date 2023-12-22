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
public class ECRequestData {
    
    private static final Logger logger = LoggerFactory.getLogger(ECRequestData.class);
    public static final Serializer serializer = new Serializer();
    // public final String parityHash;
    public final String sstHash;
    public final int index;
    public final String requestSSTHash;

    public ECRequestData(String sstHash, String requestSSTHash, int index) {
        // this.parityHash = parityHash;
        this.sstHash = sstHash;
        this.requestSSTHash = requestSSTHash;
        this.index = index;
    }

    public void requestData(InetAddressAndPort target) {
        logger.debug("ELECT-Debug: Request raw data {} from {} for the old sstable {}, index is ({})", this.requestSSTHash, target, this.sstHash, this.index);
        Message<ECRequestData> message = Message.outWithFlag(Verb.ECREQUESTDATA_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);
    }

    public static final class Serializer implements IVersionedSerializer<ECRequestData> {

        @Override
        public void serialize(ECRequestData t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHash);
            out.writeUTF(t.requestSSTHash);
            out.writeInt(t.index);
        }

        @Override
        public ECRequestData deserialize(DataInputPlus in, int version) throws IOException {
            String sstHash = in.readUTF();
            String requestSSTHash = in.readUTF();
            int index = in.readInt();
            return new ECRequestData(sstHash, requestSSTHash, index);
        }

        @Override
        public long serializedSize(ECRequestData t, int version) {
            long size = sizeof(t.sstHash) +
                        sizeof(t.requestSSTHash) +
                        sizeof(t.index);

            return size;
        }
        
    }

}
