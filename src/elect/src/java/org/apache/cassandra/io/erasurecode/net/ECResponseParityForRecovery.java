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
import java.io.Serializable;
import java.util.List;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.ECMessage.ECMessageContent;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ECResponseParityForRecovery {
    
    private static final Logger logger = LoggerFactory.getLogger(ECResponseParityForRecovery.class);
    public static final Serializer serializer = new Serializer();
    // public final String parityHash;
    public final String sstHash;
    // public final int parityIndex;
    // public final boolean isRecovery;
    // public final String firstParityNode;

    public final List<String> parityHashList;
    byte[] parityHashListInBytes;
    int parityHashListInBytesSize;




    public ECResponseParityForRecovery(String sstHash, List<String> parityHashList) {
        this.sstHash = sstHash;
        this.parityHashList = parityHashList;
    }

    public void responseParityCodesForRecovery(InetAddressAndPort target) {

        try {
            this.parityHashListInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.parityHashList);
            this.parityHashListInBytesSize = this.parityHashListInBytes.length;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        logger.debug("ELECT-Debug: Response parity code {} from {} to update the old sstable {}",
                        this.parityHashList, target, this.sstHash);
        Message<ECResponseParityForRecovery> message = Message.outWithFlag(Verb.ECRESPONSEPARITYFORRECOVERY_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);
    }

    public static final class Serializer implements IVersionedSerializer<ECResponseParityForRecovery> {

        @Override
        public void serialize(ECResponseParityForRecovery t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHash);
            out.writeInt(t.parityHashListInBytesSize);
            out.write(t.parityHashListInBytes);

        }

        @Override
        public ECResponseParityForRecovery deserialize(DataInputPlus in, int version) throws IOException {
            String sstHash = in.readUTF();
            
            int parityHashListInBytesSize = in.readInt();
            byte[] parityHashListInBytes = new byte[parityHashListInBytesSize];
            in.readFully(parityHashListInBytes);



            try {
                List<String> parityHashList = (List<String>) ByteObjectConversion.byteArrayToObject(parityHashListInBytes);
                return new ECResponseParityForRecovery(sstHash, parityHashList);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;

        }

        @Override
        public long serializedSize(ECResponseParityForRecovery t, int version) {
            long size = sizeof(t.sstHash) +
                        sizeof(t.parityHashListInBytesSize) +
                        t.parityHashListInBytesSize;

            return size;
        }
        
    }

}
