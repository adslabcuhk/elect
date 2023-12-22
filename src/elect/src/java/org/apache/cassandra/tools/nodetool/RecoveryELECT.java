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
package org.apache.cassandra.tools.nodetool;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.io.erasurecode.net.ECCompaction;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "recovery", description = "Recovery one or more tables")
public class RecoveryELECT extends NodeToolCmd {
    private static final Logger logger = LoggerFactory.getLogger(RecoveryELECT.class);
    public final static Set<String> ONLY_EXPLICITLY_REPAIRED = Sets
            .newHashSet(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);

    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe) {
        List<String> keyspaces = parseOptionalKeyspace(args, probe, KeyspaceSet.NON_LOCAL_STRATEGY);
        String[] cfnames = parseOptionalTables(args);
        logger.debug("RecoveryELECT The target keyspaces = {}, tables = {}", keyspaces, cfnames);
        for (String keyspace : keyspaces) {
            if(keyspace.equals("ycsb")) {
                for (String cfsName : cfnames) {
                    logger.debug("RecoveryELECT Runing target keyspace = {}, table = {}", keyspace, cfsName);
                    try {
                        probe.recoveryAsync(probe.output().out, keyspace, cfsName);
                    } catch (Exception e) {
                        throw new RuntimeException("Error occurred during repair", e);
                    }
                }
            }
        }
    }
}
