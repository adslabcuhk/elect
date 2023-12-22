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
package org.apache.cassandra.utils;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.LeveledCompactionTask.TransferredSSTableKeyRange;
import org.apache.cassandra.io.erasurecode.net.ECMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.nodetool.DescribeCluster;

import com.google.common.base.Throwables;

public abstract class WrappedRunnable implements Runnable
{
    public final void run()
    {
        try
        {
            runMayThrow();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    // [ELECT]
    public final void run(DecoratedKey first, DecoratedKey last, SSTableReader ecSSTable){
        
        try
        {
            runMayThrow(first, last, ecSSTable);
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    public final void run(List<TransferredSSTableKeyRange> TransferredSSTableKeyRanges){
        
        try
        {
            runMayThrow(TransferredSSTableKeyRanges);
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    public final void run(DecoratedKey first, DecoratedKey last, ECMetadata ecMetadata, String fileNamePrefix, Map<String, DecoratedKey> sourceKeys){
        
        try
        {
            runMayThrow(first, last, ecMetadata, fileNamePrefix, sourceKeys);
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    abstract protected void runMayThrow() throws Exception;
    abstract protected void runMayThrow(DecoratedKey first, DecoratedKey last, SSTableReader ecSSTable) throws Exception;
    abstract protected void runMayThrow(DecoratedKey first, DecoratedKey last, ECMetadata ecMetadata, String fileNamePrefix, Map<String, DecoratedKey> sourceKeys) throws Exception;
    abstract protected void runMayThrow(List<TransferredSSTableKeyRange> TransferredSSTableKeyRanges) throws Exception;
}
