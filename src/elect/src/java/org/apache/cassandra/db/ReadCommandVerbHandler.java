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
package org.apache.cassandra.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sasi.memory.KeyRangeIterator;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import org.apache.cassandra.tracing.Tracing;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand> {
    public static final ReadCommandVerbHandler instance = new ReadCommandVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);

    public void doVerb(Message<ReadCommand> message) {
        if (StorageService.instance.isBootstrapMode()) {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;

        // String rawKey = (command instanceof SinglePartitionReadCommand
        // ? ((SinglePartitionReadCommand)
        // command).partitionKey().getRawKey(command.metadata())
        // : null);

        long tStart = nanoTime();
        if (command.metadata().keyspace.equals("ycsb")) {

            Token tokenForRead = (command instanceof SinglePartitionReadCommand
                    ? ((SinglePartitionReadCommand) command).partitionKey().getToken()
                    : ((PartitionRangeReadCommand) command).dataRange().keyRange.right.getToken());

            List<InetAddressAndPort> sendRequestAddresses = StorageService.instance
                    .getReplicaNodesWithPortFromTokenForDegradeRead(command.metadata().keyspace, tokenForRead);

            switch (sendRequestAddresses.indexOf(FBUtilities.getBroadcastAddressAndPort())) {
                case 0:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable0").metadata());
                    ColumnFilter newColumnFilter = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter);
                    if (command.isDigestQuery() == true) {
                        logger.error("[ELECT-ERROR] Remote Should not perform digest query on the primary lsm-tree");
                    }
                    break;
                case 1:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable1")
                                    .metadata());
                    ColumnFilter newColumnFilter1 = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter1);
                    if (command.isDigestQuery() == false) {
                        logger.debug(
                                "[ELECT] Remote Should perform online recovery on the secondary lsm-tree usertable 1");
                        command.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                case 2:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable2")
                                    .metadata());
                    ColumnFilter newColumnFilter2 = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter2);
                    if (command.isDigestQuery() == false) {
                        logger.debug(
                                "[ELECT] Remote Should perform online recovery on the secondary lsm-tree usertable 2");
                        command.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                default:
                    logger.error("[ELECT-ERROR] Not support replication factor larger than 3");
                    break;
            }
            logger.debug("[ELECT] For token = {}, read {} from target table = {}, replication group = {}",
                    tokenForRead,
                    command.isDigestQuery() == true ? "digest" : "data",
                    command.metadata().name, sendRequestAddresses);
        }
        Tracing.trace("[ELECT] Executed remote modify read command time {}\u03bcs", "ReadCommandVerbHandler",
                (nanoTime() - tStart) / 1000);
        validateTransientStatus(message);
        MessageParams.reset();

        long timeout = message.expiresAtNanos() - message.createdAtNanos();
        command.setMonitoringTime(message.createdAtNanos(), message.isCrossNode(), timeout,
                DatabaseDescriptor.getSlowQueryTimeout(NANOSECONDS));

        if (message.trackWarnings()) {
            command.trackWarnings();
        }

        ReadResponse response;
        try (ReadExecutionController controller = command.executionController(message.trackRepairedData());
                UnfilteredPartitionIterator iterator = command.executeLocally(controller)) {
            if (iterator == null) {
                // if (command.metadata().keyspace.equals("ycsb") && command.isDigestQuery() ==
                // false) {
                // logger.error(
                // "[ELECT-ERROR] For token = {}, with data query, ReadCommandVerbHandler
                // Error to get response from table {}",
                // tokenForRead,
                // command.metadata().name, FBUtilities.getBroadcastAddressAndPort());
                // }
                response = command.createEmptyResponse();
            } else {
                response = command.createResponse(iterator, controller.getRepairedDataInfo());
                // if (command.metadata().keyspace.equals("ycsb") && command.isDigestQuery() ==
                // false) {
                // ByteBuffer newDigest = response.digest(command);
                // String digestStr = "0x" + ByteBufferUtil.bytesToHex(newDigest);
                // if (digestStr.equals("0xd41d8cd98f00b204e9800998ecf8427e")) {
                // logger.error(
                // "[ELECT-ERROR] For token = {}, with data query, ReadCommandVerbHandler
                // Could not get non-empty response from table {}, address = {}, {}, response =
                // {}, raw key = {}",
                // tokenForRead,
                // command.metadata().name, FBUtilities.getBroadcastAddressAndPort(),
                // "Digest:0x" + ByteBufferUtil.bytesToHex(newDigest), response, rawKey);
                // }
                // }
            }
        } catch (RejectException e) {
            // if (command.metadata().keyspace.equals("ycsb") && command.isDigestQuery() ==
            // false) {
            // logger.error(
            // "[ELECT-ERROR] For token = {}, with data query, ReadCommandVerbHandler from
            // {}, Read Command target table is {}, target key is {}, meet errors",
            // tokenForRead,
            // message.from(),
            // command.metadata().name);
            // }
            if (!command.isTrackingWarnings())
                throw e;

            // make sure to log as the exception is swallowed
            logger.error(e.getMessage());

            response = command.createEmptyResponse();
            Message<ReadResponse> reply = message.responseWith(response);
            reply = MessageParams.addToMessage(reply);

            MessagingService.instance().send(reply, message.from());
            return;
        }

        if (!command.complete()) {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message,
                    message.elapsedSinceCreated(NANOSECONDS),
                    NANOSECONDS);
            return;
        }

        Tracing.trace("Enqueuing response to {}", message.from());
        Message<ReadResponse> reply = message.responseWith(response);
        reply = MessageParams.addToMessage(reply);
        MessagingService.instance().send(reply, message.from());
    }

    private void validateTransientStatus(Message<ReadCommand> message) {
        ReadCommand command = message.payload;
        if (command.metadata().isVirtual())
            return;
        Token token;

        if (command instanceof SinglePartitionReadCommand) {
            token = ((SinglePartitionReadCommand) command).partitionKey().getToken();
        } else {
            token = ((PartitionRangeReadCommand) command).dataRange().keyRange().right.getToken();
        }

        Replica replica = Keyspace.open(command.metadata().keyspace)
                .getReplicationStrategy()
                .getLocalReplicaFor(token);

        if (replica == null) {
            logger.warn("Received a read request from {} for a range that is not owned by the current replica {}.",
                    message.from(),
                    command);
            return;
        }

        if (!command.acceptsTransient() && replica.isTransient()) {
            MessagingService.instance().metrics.recordDroppedMessage(message,
                    message.elapsedSinceCreated(NANOSECONDS),
                    NANOSECONDS);
            throw new InvalidRequestException(
                    String.format("Attempted to serve %s data request from %s node in %s",
                            command.acceptsTransient() ? "transient" : "full",
                            replica.isTransient() ? "transient" : "full",
                            this));
        }
    }
}
