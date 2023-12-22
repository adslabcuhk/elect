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

package org.apache.cassandra.db.redundancyTransition;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Sets;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.*;
import org.apache.cassandra.io.erasurecode.*;

public abstract class SSTableEncoding {

    private int ecBlock_n = 4;
    private int ecBlock_k = 3;
    private int ecBlock_m = ecBlock_n - ecBlock_k;

    SSTableEncoding(int n, int k) {
        ecBlock_n = n;
        ecBlock_k = k;
        ecBlock_m = ecBlock_n - ecBlock_k;
    }

    public boolean deserializeSSTables(Set<String> sstablesInStr) {

        return true;
    }

    public boolean performErasureCoding(Set<SSTableReader> sstables) {
        return true;
    }

}
