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
public class ECRequestParityForRecovery {
    
    private static final Logger logger = LoggerFactory.getLogger(ECRequestParity.class);
    public static final Serializer serializer = new Serializer();
    // public final String parityHash;
    public final String sstHash;
    // public final int parityIndex;
    // public final boolean isRecovery;
    // public final String firstParityNode;

    public final List<String> parityHashList;
    byte[] parityHashListInBytes;
    int parityHashListInBytesSize;
    public final List<InetAddressAndPort> parityNodeList;
    byte[] parityNodeListInBytes;
    int parityNodeListInBytesSize;




    public ECRequestParityForRecovery(String sstHash, List<String> parityHashList, List<InetAddressAndPort> parityNodeList) {
        this.sstHash = sstHash;
        this.parityHashList = parityHashList;
        this.parityNodeList = parityNodeList;
    }

    public void requestParityCodesForRecovery(InetAddressAndPort target) {

        try {
            this.parityHashListInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.parityHashList);
            this.parityHashListInBytesSize = this.parityHashListInBytes.length;
            this.parityNodeListInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.parityNodeList);
            this.parityNodeListInBytesSize = this.parityNodeListInBytes.length;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        logger.debug("ELECT-Debug: Request parity code {} from {} to update the old sstable {}",
                        this.parityHashList, target, this.sstHash);
        Message<ECRequestParityForRecovery> message = Message.outWithFlag(Verb.ECREQUESTPARITYFORRECOVERY_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);
    }

    public static final class Serializer implements IVersionedSerializer<ECRequestParityForRecovery> {

        @Override
        public void serialize(ECRequestParityForRecovery t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHash);
            out.writeInt(t.parityHashListInBytesSize);
            out.write(t.parityHashListInBytes);
            out.writeInt(t.parityNodeListInBytesSize);
            out.write(t.parityNodeListInBytes);

        }

        @Override
        public ECRequestParityForRecovery deserialize(DataInputPlus in, int version) throws IOException {
            String sstHash = in.readUTF();
            
            int parityHashListInBytesSize = in.readInt();
            byte[] parityHashListInBytes = new byte[parityHashListInBytesSize];
            in.readFully(parityHashListInBytes);

            int parityNodeListInBytesSize = in.readInt();
            byte[] parityNodeListInBytes = new byte[parityNodeListInBytesSize];
            in.readFully(parityNodeListInBytes);


            try {
                List<String> parityHashList = (List<String>) ByteObjectConversion.byteArrayToObject(parityHashListInBytes);
                List<InetAddressAndPort> parityNodeList = (List<InetAddressAndPort>) ByteObjectConversion.byteArrayToObject(parityNodeListInBytes);

                return new ECRequestParityForRecovery(sstHash, parityHashList, parityNodeList);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;

        }

        @Override
        public long serializedSize(ECRequestParityForRecovery t, int version) {
            long size = sizeof(t.sstHash) +
                        sizeof(t.parityHashListInBytesSize) +
                        t.parityHashListInBytesSize +
                        sizeof(t.parityNodeListInBytesSize) +
                        t.parityNodeListInBytesSize;

            return size;
        }
        
    }

}
