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
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECRecoveryForSecondaryVerbHandler  implements IVerbHandler<ECRecoveryForSecondary>{
    public static final ECRecoveryForSecondaryVerbHandler instance = new ECRecoveryForSecondaryVerbHandler();

    @Override
    public void doVerb(Message<ECRecoveryForSecondary> message) throws IOException {
        
        String sstHash = message.payload.sstHash;
        String cfName = message.payload.cfName;

        int level = DatabaseDescriptor.getMaxLevelCount() - 1;

        // ColumnFamilyStore cfs = Keyspace.open("ycsb").getColumnFamilyStore(cfName);
        // Set<SSTableReader> sstables = cfs.getSSTableForLevel(level);
        // boolean isFindSSTable = false;
        // for(SSTableReader sstable : sstables) {
        //     if(sstable.isReplicationTransferredToErasureCoding() && sstable.getSSTableHashID().equals(sstHash)) {
                
        //         SSTableReader.loadRawData(ByteBuffer.wrap(message.payload.sstContent), sstable.descriptor);
        //         isFindSSTable = true;
        //         return;
        //     }
        // }


        SSTableReader sstable = StorageService.instance.globalSSTHashToECSSTableMap.get(sstHash);
        if(sstable == null) 
            throw new NullPointerException(String.format("ELECT-ERROR: Cannot get ECSSTable (%s)", sstHash));
            
        SSTableReader.loadRawData(message.payload.sstContent, sstable.descriptor, sstable);
        ECNetutils.setIsRecovered(sstable.getSSTableHashID());


        // if(!isFindSSTable)
        //     throw new IllegalStateException(String.format("ELECT-ERROR:[Recovery signal] we cannot find sstable (%s) in (%s) for recovery signal from (%s)", sstHash, cfName, message.from()));

    }

    
}
