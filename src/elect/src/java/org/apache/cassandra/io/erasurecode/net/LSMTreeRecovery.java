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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class LSMTreeRecovery {
    private static final Logger logger = LoggerFactory.getLogger(LSMTreeRecovery.class);
    public static final Serializer serializer = new Serializer();

    public final String rawCfPath;
    public final String sourceCfName;
    public final String targetCfName;


    public LSMTreeRecovery(String rawCfPath, String sourceCfName, String targetCfName) {
        this.rawCfPath = rawCfPath;
        this.sourceCfName = sourceCfName;
        this.targetCfName = targetCfName;
    }


    public static void recoveryLSMTree(String keyspaceName, String cfName) {

        //Iterable<InetAddressAndPort> allHostsIterable = Iterables.concat(Gossiper.instance.getLiveMembers(),
        //Gossiper.instance.getUnreachableMembers());
        // allHostsIterable.forEach(allHosts::add);

        if(ColumnFamilyStore.getIfExists(keyspaceName, cfName) != null) {
            StorageService.instance.recoveringCFS.put(cfName, currentTimeMillis());

            try {
                if(cfName.equals("usertable0")) {
                    // Recovery usertable0 from the next node's usertable1
                    sendRecoverySignal(keyspaceName, cfName, "usertable1");

                } else if(cfName.equals("usertable1")) {

                    // Recovery usertable1 from the next node's usertable2
                    sendRecoverySignal(keyspaceName, cfName, "usertable2");

                } else if(cfName.equals("usertable2")) {

                    // Recovery usertable2 from the former node's usertable1
                    sendRecoverySignal(keyspaceName, cfName, "usertable1");

                }

                // if(cfName.equals("usertable0")) {

                //     recoveryLSMTree(keyspaceName, cfName, "usertable1");

                //     // Recovery usertable0 from the next node's usertable1
                //     String dataDir = Keyspace.open(keyspaceName).getColumnFamilyStore(cfName).getDataPaths().get(0) + "bak/";
                //     Path path = Paths.get(dataDir);
                //     PathUtils.createDirectoriesIfNotExists(path);
                //     logger.debug("ELECT-Debug: data dir is ({})", dataDir);

                //     LSMTreeRecovery msg = new LSMTreeRecovery(dataDir, cfName, "usertable1");
                //     Message<LSMTreeRecovery> message0 = Message.outWithFlag(Verb.LSMTREERECOVERY_REQ, msg, MessageFlag.CALL_BACK_ON_FAILURE);
                //     MessagingService.instance().send(message0, allHosts.get(nextIndex));

                // } else if(cfName.equals("usertable1")) {

                //     // Recovery usertable1 from the next node's usertable2
                //     recoveryLSMTree(keyspaceName, cfName, "usertable2");
                //     String dataDir = Keyspace.open(keyspaceName).getColumnFamilyStore(cfName).getDataPaths().get(0) + "bak/";
                //     Path path = Paths.get(dataDir);
                //     PathUtils.createDirectoriesIfNotExists(path);
                //     logger.debug("ELECT-Debug: data dir is ({})", dataDir);

                //     LSMTreeRecovery msg = new LSMTreeRecovery(dataDir, cfName, "usertable2");
                //     Message<LSMTreeRecovery> message = Message.outWithFlag(Verb.LSMTREERECOVERY_REQ, msg, MessageFlag.CALL_BACK_ON_FAILURE);
                //     MessagingService.instance().send(message, allHosts.get(nextIndex));

                // } else if(cfName.equals("usertable2")) {

                //     // Recovery usertable2 from the former node's usertable1
                //     recoveryLSMTree(keyspaceName, cfName, "usertable1");
                //     String dataDir = Keyspace.open(keyspaceName).getColumnFamilyStore(cfName).getDataPaths().get(0) + "bak/";
                //     Path path = Paths.get(dataDir);
                //     PathUtils.createDirectoriesIfNotExists(path);
                //     logger.debug("ELECT-Debug: data dir is ({})", dataDir);

                //     LSMTreeRecovery msg = new LSMTreeRecovery(dataDir, cfName, "usertable1");
                //     Message<LSMTreeRecovery> message = Message.outWithFlag(Verb.LSMTREERECOVERY_REQ, msg, MessageFlag.CALL_BACK_ON_FAILURE);
                //     MessagingService.instance().send(message, allHosts.get(formerIndex));

                // }


            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            logger.debug("ELECT-Debug: the cf ({}) is not existed!", cfName);
        }



    }


    private static void sendRecoverySignal(String keyspaceName, String sourceCfName, String targetCfName) throws IOException {

        InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();


        List<InetAddressAndPort> allHosts = new ArrayList<>(Gossiper.getAllNodesBasedOnSeeds());

        InetAddressAndPort targetNode = null;
        if(sourceCfName.equals("usertable2")){
            int formerIndex = allHosts.indexOf(localIP) - 1;
            formerIndex = formerIndex < 0 ? allHosts.size() - 1 : formerIndex;
            targetNode = allHosts.get(formerIndex);
        }
        else {
            int nextIndex = allHosts.indexOf(localIP) + 1;
            nextIndex = nextIndex >= allHosts.size() ? 0 : nextIndex;
            targetNode = allHosts.get(nextIndex);
            
        }


        String dataDir = Keyspace.open(keyspaceName).getColumnFamilyStore(sourceCfName).getDataPaths().get(0) + "bak/";
        Path path = Paths.get(dataDir);
        PathUtils.createDirectoriesIfNotExists(path);
        logger.debug("ELECT-Debug: send recovery request to node ({}), data raw dir is ({}), source cf name ({}), target cf name is ({}) ", targetNode, dataDir, sourceCfName, targetCfName);

        LSMTreeRecovery msg = new LSMTreeRecovery(dataDir, sourceCfName, targetCfName);
        Message<LSMTreeRecovery> message = Message.outWithFlag(Verb.LSMTREERECOVERY_REQ, msg, MessageFlag.CALL_BACK_ON_FAILURE);
        
        MessagingService.instance().send(message, targetNode);
    }


     public static final class Serializer implements IVersionedSerializer<LSMTreeRecovery> {


        @Override
        public void serialize(LSMTreeRecovery t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.rawCfPath);
            out.writeUTF(t.sourceCfName);
            out.writeUTF(t.targetCfName);
        }

        @Override
        public LSMTreeRecovery deserialize(DataInputPlus in, int version) throws IOException {
            String rawCfPath = in.readUTF();
            String sourceCfName = in.readUTF();
            String targetCfName = in.readUTF();
            return new LSMTreeRecovery(rawCfPath, sourceCfName, targetCfName);
        }

        @Override
        public long serializedSize(LSMTreeRecovery t, int version) {
            long size = sizeof(t.rawCfPath) +
                        sizeof(t.sourceCfName) +
                        sizeof(t.targetCfName);

            return size;
        }

     }
}
