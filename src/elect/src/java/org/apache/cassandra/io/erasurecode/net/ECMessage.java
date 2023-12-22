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

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.CollationElementIterator;
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import java.io.Serializable;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.utils.FBUtilities;
import org.checkerframework.dataflow.qual.TerminatesExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.TestApp;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofLong;

public class ECMessage implements Serializable {

    public static final Serializer serializer = new Serializer();


    private static AtomicInteger GLOBAL_COUNTER = new AtomicInteger(0);

    public ECMessageContent ecMessageContent;
    public byte[] ecMessageContentInBytes;
    public int ecMessageContentInBytesSize;
    
    public final byte[] sstContent;
    public final int sstSize;

    public static class ECMessageContent implements Serializable {
        public final String sstHashID;
        public final String keyspace;
        public final String cfName;
        public final int ecDataNum;
        public final int rf;

        public final int ecParityNum;

        public List<InetAddressAndPort> replicaNodes;
        public List<InetAddressAndPort> parityNodes;

        public ECMessageContent(String sstHashID, String keyspace, String cfName,
                List<InetAddressAndPort> replicaNodes) {

            
            this.sstHashID = sstHashID;
            this.keyspace = keyspace;
            this.cfName = cfName;
            this.ecDataNum = DatabaseDescriptor.getEcDataNodes();
            this.ecParityNum = DatabaseDescriptor.getParityNodes();
            this.rf = Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor().allReplicas;
            
            if(replicaNodes.isEmpty()) {
                this.replicaNodes = new ArrayList<InetAddressAndPort>();
            } else {
                this.replicaNodes = new ArrayList<InetAddressAndPort>(replicaNodes);
            }
            this.parityNodes = new ArrayList<InetAddressAndPort>();
}
    }




    public ECMessage(byte[] sstContent, ECMessageContent ecMessageContent) {
        this.sstContent = sstContent;
        this.sstSize = sstContent.length;
        this.ecMessageContent = ecMessageContent;
    }

    protected static Output output;
    public static final Logger logger = LoggerFactory.getLogger(ECMessage.class);

    /**
     * This method sends selected sstables to parity nodes for EC/
     * 
     * @param sstContent selected sstables
     * @param k          number of parity nodes
     * @param ks         keyspace name of sstables
     * @throws UnknownHostException
     *                              TODO List
     *                              1. implement Verb.ERASURECODE_REQ
     *                              2. implement responsehandler
     */
    public void sendSSTableToParity() throws UnknownHostException {
        logger.debug("ELECT-Debug: this is sendSelectedSSTables");

        // create a Message for sstContent
        Message<ECMessage> message = null;
        // GLOBAL_COUNTER++;

        getTargetEdpoints(this);

        try {
            // logger.debug("this is transform ");
            this.ecMessageContentInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.ecMessageContent);
            this.ecMessageContentInBytesSize = this.ecMessageContentInBytes.length;
            
            // logger.debug("ELECT-Debug: this.ecMessageContentInBytesSize is {}", this.ecMessageContentInBytesSize);
            // logger.debug("ELECT-Debug: this.ecMessageContentInBytes.length {}", this.ecMessageContentInBytes.length);
            if(this.ecMessageContentInBytesSize == 0 || this.ecMessageContentInBytes == null) {
                logger.error("ELECT-ERROR: ecMessageInBytesSize is {}, ecMessageContentInBytes is {}", this.ecMessageContentInBytesSize,
                                                                                                    this.ecMessageContent); 
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (this.ecMessageContent.parityNodes != null) {
            logger.debug("ELECT-Debug: send sstable ({}) to parity node ({}), sstable content size is ({})",
                         this.ecMessageContent.sstHashID, this.ecMessageContent.parityNodes.get(0),
                         this.sstContent.length);
            message = Message.outWithFlag(Verb.ERASURECODE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().sendSSTContentWithoutCallback(message, this.ecMessageContent.parityNodes.get(0));
        } else {
            logger.error("ELECT-ERROR: targetEndpoints is null!");
        }
    }

    /*
     * Get target nodes, use the methods related to nodetool.java and status.java
     */
    protected static void getTargetEdpoints(ECMessage ecMessage) throws UnknownHostException {

        // get all live nodes
        List<InetAddressAndPort> liveEndpoints = new ArrayList<>(Gossiper.instance.getLiveMembers());

        // logger.debug("ELECT-Debug: All living nodes are {}", liveEndpoints);
        // logger.debug("ELECT-Debug: ecMessage.replicaNodes is {}", ecMessage.replicaNodes);

        // select parity nodes from live nodes, suppose all nodes work healthy
        int n = liveEndpoints.size();
        InetAddressAndPort primaryNode = ecMessage.ecMessageContent.replicaNodes.get(0);
        int primaryNodeIndex = liveEndpoints.indexOf(primaryNode);

        int startIndex = ((primaryNodeIndex + n - (GLOBAL_COUNTER.getAndIncrement() % ecMessage.ecMessageContent.ecDataNum+1))%n) - 1;
        if(startIndex == -1) {
            startIndex = n - 1;
        }

        int remaining = DatabaseDescriptor.getParityNodes();
        int index = startIndex;
        while(remaining > 0) {
            if(index >= liveEndpoints.size()) {
                index = 0;
            }
            ecMessage.ecMessageContent.parityNodes.add(liveEndpoints.get(index));
            index++;
            remaining--;
        }

        // for (int i = startIndex; i < ecMessage.ecMessageContent.ecParityNum+startIndex; i++) {
        //     int index = i%n;
        //     if(index==primaryNodeIndex) {
        //         index = (index+1)%n;
        //         i++;
        //     }
        //     ecMessage.ecMessageContent.parityNodes.add(liveEndpoints.get(index));
        //     if(i == (ecMessage.ecMessageContent.ecParityNum + startIndex)
        //           && ecMessage.ecMessageContent.parityNodes.size() < ecMessage.ecMessageContent.ecParityNum) {

        //         startIndex++;
        //     }
        // }
        // logger.debug("ELECT-Debug: ecMessage.parityNodes is {}", ecMessage.parityNodes);

    }

    public static final class Serializer implements IVersionedSerializer<ECMessage> {

        @Override
        public void serialize(ECMessage t, DataOutputPlus out, int version) throws IOException {
            // logger.debug("ELECT-Debug: t.ecMessageContentInBytesSize is {}", t.ecMessageContentInBytesSize);
            // logger.debug("ELECT-Debug: t.ecMessageContentInBytes.length {}", t.ecMessageContentInBytes.length);
            out.writeInt(t.ecMessageContentInBytesSize);
            out.write(t.ecMessageContentInBytes);

            out.writeInt(t.sstSize);
            // byte[] buf = new byte[t.sstSize];
            // t.sstContent.get(buf);
            out.write(t.sstContent);
            // logger.debug("ELECT-Debug: [serialize] write successfully", buf.length);
        }

        @Override
        public ECMessage deserialize(DataInputPlus in, int version) throws IOException {

            // // logger.debug("ELECT-Debug: deserialize.ecMessage.sstHashID is {},ks is: {}, cf is {},repEpString is {},parityNodes are: {}"
            // // , sstHashID,ks, cf,repEpsString,parityNodesString);


            int ecMessageContentInBytesSize = in.readInt();
            byte[] ecMessageContentInBytes = new byte[ecMessageContentInBytesSize];
            in.readFully(ecMessageContentInBytes);

            int sstSize = in.readInt();
            byte[] sstContent = new byte[sstSize];
            in.readFully(sstContent);
            // ByteBuffer sstContent = ByteBuffer.wrap(buf);

            ECMessageContent ecMessage;
            try {
                ecMessage = (ECMessageContent) ByteObjectConversion.byteArrayToObject(ecMessageContentInBytes);
                return new ECMessage(sstContent, ecMessage);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public long serializedSize(ECMessage ecMessage, int version) {            
            
            long size = ecMessage.ecMessageContentInBytesSize +
                        sizeof(ecMessage.ecMessageContentInBytesSize) +
                        ecMessage.sstSize +
                        sizeof(ecMessage.sstSize);

            return size;

        }

    }

}
