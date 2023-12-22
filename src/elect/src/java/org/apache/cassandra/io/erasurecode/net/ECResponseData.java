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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import static org.apache.cassandra.db.TypeSizes.sizeof;
public class ECResponseData {
    public static final Serializer serializer = new Serializer();


    public final String sstHash;
    public final byte[] rawData;
    public final int rawDataSize;
    public final int index;

    public ECResponseData(String sstHash, byte[] rawData, int index) {
        this.sstHash = sstHash;
        this.rawData = rawData;
        this.rawDataSize = rawData.length;
        this.index = index;
    }


    public void responseData(InetAddressAndPort target) {
        
        Message<ECResponseData> message = Message.outWithFlag(Verb.ECRESPONSEDATA_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);
    }

    public static final class Serializer implements IVersionedSerializer<ECResponseData> {

        @Override
        public void serialize(ECResponseData t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHash);
            out.writeInt(t.index);
            out.writeInt(t.rawDataSize);
            out.write(t.rawData);


        }

        @Override
        public ECResponseData deserialize(DataInputPlus in, int version) throws IOException {
            String sstHash = in.readUTF();
            int index = in.readInt();
            int rawDataSize = in.readInt();
            byte[] rawData = new byte[rawDataSize];
            in.readFully(rawData);
            return new ECResponseData(sstHash, rawData, index);
        }

        @Override
        public long serializedSize(ECResponseData t, int version) {
            long size = sizeof(t.sstHash) + 
                        sizeof(t.index) +
                        sizeof(t.rawDataSize) +
                        t.rawDataSize;
            return size;
        }
        
    }


}
