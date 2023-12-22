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
package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.apache.cassandra.utils.progress.ProgressEventType.*;

public class RecoveryRunner
{
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    private final PrintStream out;
    private final StorageServiceMBean ssProxy;
    private final String keyspace;
    private final String cfsName;
    private final Condition condition = newOneTimeCondition();

    private int cmd;
    private volatile Exception error;

    public RecoveryRunner(PrintStream out, StorageServiceMBean ssProxy, String keyspace, String cfsName)
    {
        this.out = out;
        this.ssProxy = ssProxy;
        this.keyspace = keyspace;
        this.cfsName = cfsName;
    }

    public void run() throws Exception
    {
        ssProxy.recoveryAsync(keyspace, cfsName);
     
    }


}
