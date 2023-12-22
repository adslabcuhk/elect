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
import java.net.Socket;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSMTreeRecoveryVerbHandler implements IVerbHandler<LSMTreeRecovery> {

    private static final Logger logger = LoggerFactory.getLogger(LSMTreeRecoveryVerbHandler.class);
    public static final LSMTreeRecoveryVerbHandler instance = new LSMTreeRecoveryVerbHandler();
    @Override
    public void doVerb(Message<LSMTreeRecovery> message) throws IOException {
        String rawCfPath = message.payload.rawCfPath;
        String sourceCfName = message.payload.sourceCfName;
        String targetCfName = message.payload.targetCfName;
        InetAddressAndPort sourceAddress = message.from();
        logger.debug("ELECT-Debug: Get a recovery LSM tree message, the raw Cf path is ({}), source cf name is ({}), target cf name is ({})",
                     rawCfPath, sourceCfName, targetCfName);
        for (Keyspace keyspace : Keyspace.all()){
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores()) {

                if(cfs.getColumnFamilyName().equals(targetCfName)) {
                    logger.debug("ELECT-Debug: got a matched LSM tree ({}) for recovery signal.", targetCfName);
                    List<String> dataDirs = cfs.getDataPaths();
                    for(String dir : dataDirs) {
                        if(dir.contains(targetCfName)) {
                            String userName = DatabaseDescriptor.getUserName();
                            // String passWd = "yjren";
                            String host = sourceAddress.getHostAddress(false);
                            String targetDir = userName + "@" + host + ":" + rawCfPath;
                            // String script = "sshpass -p \"" + passWd + "\" scp -r " + dir + " " + targetDir;
                            // String script = "ls " + dir; 
                            String script = "rsync -avz --progress -r " + dir + " " + targetDir;                           
                            logger.debug("ELECT-Debug: The script is ({})", script);
                            ProcessBuilder processBuilder = new ProcessBuilder(script.split(" "));
                            Process process = processBuilder.start();

                            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                            
                            String line;
                            while ((line = reader.readLine()) != null) {
                                System.out.println("Script output: " + line);
                            }
            
                            
                            try {
                                int exitCode = process.waitFor();
                                if (exitCode == 0) {
                                    logger.debug("ELECT-Debug: Performing rsync script successfully!");

                                    // send response code back
                                    logger.debug("ELECT-Debug: Copied ({}) files to node ({}), source cfName is ({}), rawPath is ({})", targetCfName, sourceAddress, sourceCfName, rawCfPath);
                                    ResponseLSMTreeRecovery.sendRecoveryIsReadySignal(sourceAddress, rawCfPath, sourceCfName);

                                } else {
                                    logger.debug("ELECT-Debug: Failed to perform rsync script!");
                                }
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            break;
                        }
                    }
                }
                
            }
        }
        
        
    }

    // write a Main function to test this class
    public static void main(String[] args) throws IOException {
        // String rawCfPath = "/home/yjren/cassandra/data/ycsb/usertable1-bak";
        // String sourceCfName = "usertable1";
        // String targetCfName = "usertable2";
        String script = "ls";

        ProcessBuilder processBuilder = new ProcessBuilder(script.split(" "));
        Process process = processBuilder.start();
        try {
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                logger.debug("ELECT-Debug: Performing rsync script successfully!");

                // send response code back
                //logger.debug("ELECT-Debug: Copied ({}) files to node ({}), source cfName is ({}), rawPath is ({})", targetCfName, sourceAddress, sourceCfName, rawCfPath);
                //ResponseLSMTreeRecovery.sendRecoveryIsReadySignal(sourceAddress, rawCfPath, sourceCfName);

            } else {
                logger.debug("ELECT-Debug: Failed to perform rsync script!");
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
