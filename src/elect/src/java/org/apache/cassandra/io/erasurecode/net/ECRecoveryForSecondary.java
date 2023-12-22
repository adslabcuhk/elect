/**
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
import java.util.List;

import javax.print.attribute.Size2DSyntax;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import static org.apache.cassandra.db.TypeSizes.sizeof;

public class ECRecoveryForSecondary {


    
    private static final Logger logger = LoggerFactory.getLogger(ECRecoveryForSecondary.class);
    public static final Serializer serializer = new Serializer();

    public final String sstHash;
    public final byte[] sstContent;
    public final int sstContentSize;
    public final String cfName;

    
    public ECRecoveryForSecondary(String sstHash, byte[] sstContent, String cfName){
        this.sstHash = sstHash;
        this.sstContent = sstContent;
        this.sstContentSize = sstContent.length;
        this.cfName = cfName;
    }

    public void sendDataToSecondaryNode(InetAddressAndPort target) {

        logger.debug("ELECT-Debug: [Debug recovery] send raw data of sstable ({}) to node ({})", this.sstHash, target);
        Message<ECRecoveryForSecondary> message = Message.outWithFlag(Verb.ECRECOVERYDATA_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);
    }

    public static final class Serializer implements IVersionedSerializer<ECRecoveryForSecondary> {

        @Override
        public void serialize(ECRecoveryForSecondary t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHash);
            out.writeInt(t.sstContentSize);
            out.write(t.sstContent);
            out.writeUTF(t.cfName);
        }

        @Override
        public ECRecoveryForSecondary deserialize(DataInputPlus in, int version) throws IOException {
            String sstHash = in.readUTF();
            int sstContentSize = in.readInt();
            byte[] sstContent = new byte[sstContentSize];
            in.readFully(sstContent);
            String cfName = in.readUTF();
            return new ECRecoveryForSecondary(sstHash, sstContent, cfName);
        }

        @Override
        public long serializedSize(ECRecoveryForSecondary t, int version) {
            long size = sizeof(t.sstHash) +
                        sizeof(t.sstContentSize) +
                        t.sstContentSize +
                        sizeof(t.cfName);
            return size;
        }

    }
}
