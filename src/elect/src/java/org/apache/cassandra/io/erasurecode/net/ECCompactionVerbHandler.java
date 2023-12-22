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
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECCompactionVerbHandler implements IVerbHandler<ECCompaction> {
    /*
     * Secondary nodes receive compaction signal from primary nodes and trigger
     * compaction
     */
    
    private static final Logger logger = LoggerFactory.getLogger(ECCompaction.class);
    public static final ECCompactionVerbHandler instance = new ECCompactionVerbHandler();
    // TODO: optimization: select a specific LSM-tree and compact specific sstables

    @Override
    public void doVerb(Message<ECCompaction> message) throws IOException {

        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null)
            forwardToLocalNodes(message, forwardTo);

        String sstHash = message.payload.sstHash;
        String ksName = message.payload.ksName;
        String cfName = message.payload.cfName;
        String key = message.payload.key;
        String startToken = message.payload.startToken;
        String endToken = message.payload.endToken;
        InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
        List<InetAddressAndPort> replicaNodes = StorageService.instance.getReplicaNodesWithPort(ksName, cfName, key);
        logger.debug("ELECT-Debug: compaction handler, localAddress is {}, replicaNodes is {}", localAddress, replicaNodes);
        int index = replicaNodes.indexOf(localAddress);
        logger.debug("ELECT-Debug: Received compaction request for {}/{}/{}/{}",
         sstHash, ksName, message.payload.cfName, String.valueOf(index));
        

        //TODO: get sstContent and do compaction
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName + String.valueOf(index));
        logger.debug("ELECT-Debug: received startToken is {}, endToken is {}, but current startToken is {}, endToken is {]}",
                            startToken, endToken, cfs.getPartitioner().getMinimumToken(), cfs.getPartitioner().getMaximumToken());
        // cfs.forceCompactionForKey(null);
        
        Collection<Range<Token>> tokenRanges = StorageService.instance.createRepairRangeFrom(startToken, endToken);
        try {
            cfs.forceCompactionForTokenRange(tokenRanges);
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void forwardToLocalNodes(Message<ECCompaction> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECCompaction> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECCompaction> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }

}
