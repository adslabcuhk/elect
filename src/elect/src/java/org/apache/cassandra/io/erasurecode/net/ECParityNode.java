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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ECParityNode {


    public static final ECParityNode instance = new ECParityNode(null, null, 0);
    private static final Logger logger = LoggerFactory.getLogger(ECParityNode.class);
    public static final Serializer serializer = new Serializer();

    public final ByteBuffer parityCode;
    public final String parityHash;
    public final int paritySize;

    public ECParityNode(ByteBuffer parityCode, String parityHash, int paritySize) {
        this.parityCode = parityCode;
        this.parityHash = parityHash;
        this.paritySize = paritySize;
    }


    public void distributeCodedDataToParityNodes(ByteBuffer[] parity, List<InetAddressAndPort> parityNodes, List<String> parityHashList) {
        logger.debug("ELECT-Debug: distribute parity codes ({}) to parity nodes ({})", parityHashList, parityNodes);
        Message<ECParityNode> message = null;
        for (int i = 1; i < parityNodes.size(); i++) {
            message = Message.outWithFlag(Verb.ECPARITYNODE_REQ, 
            new ECParityNode(parity[i], parityHashList.get(i), parity[i].capacity()), MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().send(message, parityNodes.get(i));
        }
        
    }

    public static final class Serializer implements IVersionedSerializer<ECParityNode> {

        @Override
        public void serialize(ECParityNode t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.parityHash);

            out.writeInt(t.paritySize);
            byte[] buf = new byte[t.paritySize];
            t.parityCode.get(buf);
            out.write(buf);
        }

        @Override
        public ECParityNode deserialize(DataInputPlus in, int version) throws IOException {
            String parityHash = in.readUTF();

            int paritySize = in.readInt();
            byte[] buf = new byte[paritySize];
            in.readFully(buf);
            ByteBuffer parityCode = ByteBuffer.wrap(buf);

            return new ECParityNode(parityCode, parityHash, paritySize);
        }

        @Override
        public long serializedSize(ECParityNode t, int version) {
            long size = sizeof(t.paritySize) +
                        t.paritySize + 
                        sizeof(t.parityHash);
            return size;
        }
    }

}
