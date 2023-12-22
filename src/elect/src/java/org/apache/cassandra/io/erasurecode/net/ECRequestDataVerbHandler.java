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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.LeveledGenerations;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class ECRequestDataVerbHandler implements IVerbHandler<ECRequestData> {
    public static final ECRequestDataVerbHandler instance = new ECRequestDataVerbHandler();
    private static final int MAX_RETRY_COUNT = 5;

    private static final Logger logger = LoggerFactory.getLogger(ECRequestDataVerbHandler.class);

    @Override
    public void doVerb(Message<ECRequestData> message) {

        String sstHash = message.payload.sstHash;
        int index = message.payload.index;
        String requestSSTHash = message.payload.requestSSTHash;

        int level = LeveledGenerations.getMaxLevelCount() - 1;
        ColumnFamilyStore cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable0");
        Set<SSTableReader> sstables = cfs.getSSTableForLevel(level);

        boolean isFound = false;
        int traverseCount = 0;
        for (SSTableReader sstable : sstables) {
            traverseCount++;
            if (sstable.getSSTableHashID().equals(requestSSTHash)) {
                logger.debug("ELECT-Debug: the requested sstable ({}) is found!", sstable.getSSTableHashID());
                if (sstable.isDataMigrateToCloud()) {
                    logger.debug("ELECT-Debug: the requested sstable ({}) is migrated!", sstable.getSSTableHashID());
                    if (!ECNetutils.getIsDownloaded(sstable.getSSTableHashID())) {
                        long tStart = nanoTime();
                        // reload raw data from cloud
                        int retryCount = 0;
                        if (!StorageService.instance.downloadingSSTables.contains(sstable.getSSTableHashID())) {
                            StorageService.instance.downloadingSSTables.add(sstable.getSSTableHashID());

                            CountDownLatch migrationLatch = new CountDownLatch(1);
                            try {
                                SSTableReader.loadRawDataFromCloud(sstable.descriptor, sstable, migrationLatch);

                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }

                            try {
                                migrationLatch.await();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else {
                            while (StorageService.instance.downloadingSSTables.contains(sstable.getSSTableHashID())
                                    && retryCount < 1000) {
                                try {
                                    logger.debug("ELECT-Debug: [Degraded Read] the sstable ({}) is still downloading!",
                                            sstable.getSSTableHashID());
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                                retryCount++;
                            }
                        }
                        Tracing.trace("[ELECT] Moved SSTable back from cloud {}\u03bcs",
                                "ECRequestDataVerbHandler",
                                (nanoTime() - tStart) / 1000);
                    }
                }

                if (sstable.getColumnFamilyName().equals("usertable0")
                        && ECNetutils.getIsDownloaded(sstable.getSSTableHashID())) {
                    logger.debug("[ELECT] Fetch downloaded sstable for recovery ({}, {})", sstable.getFilename(),
                            sstable.getSSTableHashID());
                    sstable = StorageService.instance.globalDownloadedSSTableMap.get(sstable.getSSTableHashID());
                }

                // ByteBuffer buffer;
                try {
                    // buffer = sstable.getSSTContent();
                    // byte[] rawData = new byte[buffer.remaining()];
                    // buffer.get(rawData);
                    byte[] rawData = sstable.getSSTContent();

                    ECResponseData response = new ECResponseData(sstHash, rawData, index);
                    response.responseData(message.from());
                    isFound = true;
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                break;
            }
        }

        if (!isFound)
            throw new IllegalStateException(
                    String.format("ELECT-ERROR: cannot find sstable (%s) in usertable0 for recovery/update sstable (%s), the traverse count is (%s), sstable count is (%s)",
                                  requestSSTHash, sstHash, traverseCount, sstables.size()));

    }

}
