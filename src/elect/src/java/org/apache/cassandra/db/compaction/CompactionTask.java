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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogRecord;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.Iterator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.guieffect.qual.AlwaysSafe;
import org.eclipse.jdt.internal.compiler.ast.TrueLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.LeveledCompactionTask.TransferredSSTableKeyRange;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.erasurecode.net.ECCompaction;
import org.apache.cassandra.io.erasurecode.net.ECMessage;
import org.apache.cassandra.io.erasurecode.net.ECMetadata;
import org.apache.cassandra.io.erasurecode.net.ECMetadataVerbHandler;
import org.apache.cassandra.io.erasurecode.net.ECNetutils;
import org.apache.cassandra.io.erasurecode.net.ECParityNode;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate.SSTableContentWithHashID;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.FBUtilities.now;


public class CompactionTask extends AbstractCompactionTask {
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected final int gcBefore;
    protected final boolean keepOriginals;
    protected static long totalBytesCompacted = 0;
    private ActiveCompactionsTracker activeCompactions;

    // private static final Comparator<UnfilteredRowIterator> partitionComparator = (p1, p2) -> p1.partitionKey().compareTo(p2.partitionKey());

    // Reset
    public static final String RESET = "\033[0m"; // Text Reset

    // Regular Colors
    public static final String WHITE = "\033[0;30m"; // WHITE
    public static final String RED = "\033[0;31m"; // RED
    public static final String GREEN = "\033[0;32m"; // GREEN
    public static final String YELLOW = "\033[0;33m"; // YELLOW
    public static final String BLUE = "\033[0;34m"; // BLUE
    public static final String PURPLE = "\033[0;35m"; // PURPLE
    public static final String CYAN = "\033[0;36m"; // CYAN
    public static final String GREY = "\033[0;37m"; // GREY

                
    private final Comparator<SSTableReader> comparator = new Comparator<SSTableReader>() {

        public int compare(SSTableReader o1, SSTableReader o2) {
            return o1.first.compareTo(o2.first);
        }

    };


    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore) {
        this(cfs, txn, gcBefore, false);
    }

    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean keepOriginals) {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.keepOriginals = keepOriginals;
    }

    public static synchronized long addToTotalBytesCompacted(long bytesCompacted) {
        return totalBytesCompacted += bytesCompacted;
    }

    protected int executeInternal(ActiveCompactionsTracker activeCompactions) {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run();
        return transaction.originals().size();
    }

    // [ELECT]
    protected int executeInternal(ActiveCompactionsTracker activeCompactions, DecoratedKey first, DecoratedKey last, ECMetadata ecMetadata, String fileNamePrefix,
                                  Map<String, DecoratedKey> sourceKeys) {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run(first, last, ecMetadata, fileNamePrefix, sourceKeys);
        return transaction.originals().size();
    }

    protected int executeInternal(ActiveCompactionsTracker activeCompactions, DecoratedKey first, DecoratedKey last, SSTableReader ecSSTable) {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run(first, last, ecSSTable);
        return transaction.originals().size();
    }

    protected int executeInternal(ActiveCompactionsTracker activeCompactions, List<TransferredSSTableKeyRange> TransferredSSTableKeyRanges) {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run(TransferredSSTableKeyRanges);
        return transaction.originals().size();
    }

    @Override
    protected void runMayThrow(DecoratedKey first, DecoratedKey last, SSTableReader ecSSTable) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'runMayThrow'");
    }

    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize) {
        if (partialCompactionsAcceptable() && transaction.originals().size() > 1) {
            // Try again w/o the largest one.
            logger.warn("insufficient space to compact all requested files. {}MiB required, {} for compaction {}",
                    (float) expectedSize / 1024 / 1024,
                    StringUtils.join(transaction.originals(), ", "),
                    transaction.opId());
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the
            // end.
            SSTableReader removedSSTable = cfs.getMaxSizeFile(nonExpiredSSTables);
            transaction.cancel(removedSSTable);
            return true;
        }
        return false;
    }

    /** [ELECT] rewrite the sstables
     *  only for secondary LSM-tree
     */
    protected void runMayThrow(DecoratedKey first, DecoratedKey last, ECMetadata ecMetadata, String fileNamePrefix, Map<String, DecoratedKey> sourceKeys) throws Exception {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;
        Set<SSTableReader> sstables = new HashSet<SSTableReader>(transaction.originals());

        
        int originalSSTableNum = sstables.size();

        logger.debug("[Rewrite SSTables]: {} sstables, original sstbales number is {}, transaction id is ", sstables.size(), transaction.originals().size(), transaction.opId());

        if (sstables.isEmpty())
            return;

        // Note that the current compaction strategy, is not necessarily the one this
        // task was created under.
        // This should be harmless; see comments to CFS.maybeReloadCompactionStrategy.
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();

        if (DatabaseDescriptor.isSnapshotBeforeCompaction()) {
            Instant creationTime = now();
            cfs.snapshotWithoutMemtable(creationTime.toEpochMilli() + "-compact-" + cfs.name, creationTime);
        }

        try (CompactionController controller = getCompactionController(sstables)) {

            // final Set<SSTableReader> fullyExpiredSSTables = controller.getFullyExpiredSSTables();

            // select SSTables to compact based on available disk space.
            // buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables);

            // sanity check: all sstables must belong to the same cfs
            assert !Iterables.any(sstables, new Predicate<SSTableReader>() {
                @Override
                public boolean apply(SSTableReader sstable) {
                    // logger.debug("[Rewrite SSTables]: attempting to read sstables from {}, but got from cf {}", sstable.descriptor.cfname, cfs.name);
                    return !sstable.descriptor.cfname.equals(cfs.name);
                }
            });

            TimeUUID taskId = transaction.opId();

            // new sstables from flush can be added during a compaction, but only the
            // compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining
            // if we're compacting
            // all the sstables (that existed when we started)
            StringBuilder ssTableLoggerMsg = new StringBuilder("[");
            for (SSTableReader sstr : sstables) {
                ssTableLoggerMsg.append(String.format("%s:level=%d, ", sstr.getFilename(), sstr.getSSTableLevel()));
            }
            ssTableLoggerMsg.append("]");

            logger.info("[Rewrite SSTables]: Rewriting ({}) {}", taskId, ssTableLoggerMsg);

            RateLimiter limiter = CompactionManager.instance.getRateLimiter();
            long start = nanoTime();
            long startTime = currentTimeMillis();
            long totalKeysWritten = 0;
            long estimatedKeys = 0;
            long inputSizeBytes;
            long timeSpentWritingKeys;
            long traversedKeys = 0;
            long keysInRange = 0;
            long headKeysNum = 0;
            long tailKeysNum = 0;
            int  checkedSSTableNum = sstables.size();

            boolean isSwitchWriter = false;

            for (SSTableReader sstable : sstables) {
                logger.debug("[Rewrite SSTables]: rewrite sstable name is {}, sstable level is {}, task id is {}", 
                    sstable.getFilename(), sstable.getSSTableLevel(), taskId);
            }
            
            Collection<SSTableReader> newSSTables = new ArrayList<SSTableReader>();
            // Collection<SSTableReader> headNewSStables;
            // Collection<SSTableReader> tailNewSStables;
            // Collection<SSTableReader> rewrittenSStables;

            long[] mergedRowCounts;
            long totalSourceCQLRows;
            String cfName = cfs.name;


            int nowInSec = FBUtilities.nowInSeconds();
            try (Refs<SSTableReader> refs = Refs.ref(sstables);
                    AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(sstables.stream().sorted(comparator).collect(Collectors.toList()));
                    CompactionIterator ci = new CompactionIterator(compactionType, scanners.scanners, controller,
                            nowInSec, taskId)) {
                long lastCheckObsoletion = start;
                inputSizeBytes = scanners.getTotalCompressedSize();
                double compressionRatio = scanners.getCompressionRatio();
                if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                    compressionRatio = 1.0;

                long lastBytesScanned = 0;

                activeCompactions.beginCompaction(ci);
                
                // logger.debug("[Rewrite SSTables]: rewrite SSTable is START, ecSSTable is {},", ecSSTable.descriptor);
                try (// CompactionAwareWriter writer1 = getCompactionAwareWriter(cfs, getDirectories(), transaction, sstables);
                     CompactionAwareWriter writer = getCompactionAwareWriter(cfs, getDirectories(), transaction, sstables)) {
                    // Note that we need to re-check this flag after calling beginCompaction above
                    // to avoid a window
                    // where the compaction does not exist in activeCompactions but the CSM gets
                    // paused.
                    // We already have the sstables marked compacting here so
                    // CompactionManager#waitForCessation will
                    // block until the below exception is thrown and the transaction is cancelled.
                    if (!controller.cfs.getCompactionStrategyManager().isActive())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    // estimatedKeys = writer.estimatedKeys();
                    while (ci.hasNext()) {
                        traversedKeys++;
                        UnfilteredRowIterator row = ci.next();
                        if(!row.partitionKey().equals(row.partitionKey())) {
                            logger.debug("ELECT-Debug: Different! row1 is {}, row2 is {}", 
                                row.partitionKey().getRawKey(cfs.metadata()), row.partitionKey().getRawKey(cfs.metadata()));
                        }

                        // if(sourceKeys.get(row.partitionKey().getRawKey(cfs.metadata())) == null) {
                        //     if(writer.append(row)) {
                        //         tailKeysNum++;
                        //     }
                        // } else {
                        //     keysInRange++;
                        // }

                        if(StorageService.instance.globalCachedKeys.get(row.partitionKey().getRawKey(cfs.metadata())) == null) {
                            if(writer.append(row)) {
                                tailKeysNum++;
                            }
                        } else {
                            StorageService.instance.globalCachedKeys.remove(row.partitionKey().getRawKey(cfs.metadata()));
                            keysInRange++;
                        }

                        // if(row.partitionKey().compareTo(last) > 0) {
                        //     if(!isSwitchWriter) {
                        //         isSwitchWriter = true;
                        //         if(writer.append(row, isSwitchWriter)) {
                        //             // totalKeysWritten++;
                        //             tailKeysNum++;
                        //         }
                        //         logger.debug("[Rewrite SSTables]: switched a new writer, task id is {}", taskId);
                        //     } else {
                        //         if(writer.append(row, false)) {
                        //             // totalKeysWritten++;
                        //             tailKeysNum++;
                        //         }
                        //     }
                        // } else if (row.partitionKey().compareTo(first) < 0) {
                        //     if(writer.append(row, false)) {
                        //         // totalKeysWritten++;
                        //         headKeysNum++;
                        //     }
                        // } else {
                        //     keysInRange++;
                        // }

                        

                        long bytesScanned = scanners.getTotalBytesScanned();

                        // Rate limit the scanners, and account for compression
                        CompactionManager.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned,
                                compressionRatio);

                        lastBytesScanned = bytesScanned;

                        if (nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L)) {
                            controller.maybeRefreshOverlaps();
                            lastCheckObsoletion = nanoTime();
                        }
                    }

                    totalKeysWritten = headKeysNum + tailKeysNum;

                    if(!isSwitchWriter) {
                        logger.warn("[Rewrite SSTables]: task {} did not switch writer!", taskId);
                    }
                    
                    // logger.debug("ELECT-Debug: traversed keys num is {}, totalKeysWritten is {}",traversedKeys, totalKeysWritten);
                    timeSpentWritingKeys = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

                    // point of no return
                    // logger.debug("ELECT-Debug: about writer, capacity is ");
                    // headNewSStables = writer1.finishFirstPhase();
                    // tailNewSStables = writer2.finish();
                    SSTableReader ecSSTable = SSTableReader.openECSSTable(ecMetadata, null, cfs, fileNamePrefix, transaction.opId());

                    if (!ecSSTable.SetIsReplicationTransferredToErasureCoding()) {
                        logger.error("ELECT-ERROR: set IsReplicationTransferredToErasureCoding failed!");
                    }
                    logger.debug("ELECT-Debug: this is rewrite SSTable method, replacing SSTable {}", ecSSTable.getSSTableHashID());
                    
                    transaction.update(ecSSTable, false);
                    // writer.getSSTableWriter().moveStarts(ecSSTable, ecSSTable.last);
                    transaction.checkpoint();
                    // transaction.tracker.addSSTables(Collections.singleton(ecSSTable));
                    // transaction.tracker.apply(View.updateLiveSet(Collections.emptySet(), Collections.singleton(ecSSTable)));
                    newSSTables = writer.finish(ecSSTable);
                    ECMetadataVerbHandler.checkTheBlockedUpdateECMetadata(ecSSTable);
                    // newSSTables = writer.finish();
                    logger.debug("[Rewrite SSTables]: rewrite SSTable is FINISHED, ecSSTable is {},", ecSSTable.getSSTableHashID());
                    // TODO: re-create sstable reader from ecmetadata 

                    // Iterable<SSTableReader> allSStables = cfs.getSSTables(SSTableSet.LIVE);
                    logger.debug(YELLOW+"[Rewrite SSTables]: Rewrite is done!!!! task id is {},  cfName is {}, original sstable number is {}, checkedSSTableNum is {}, new sstables num is {}, head keys num is {}, tail keys num is {}, total traversed keys nums is {}, saved keys is {}, keys num in range is {}, source key num is ({})",
                        taskId, cfName+RESET, originalSSTableNum, checkedSSTableNum, newSSTables.size(), headKeysNum, tailKeysNum, traversedKeys, totalKeysWritten, keysInRange, sourceKeys.size());
                }
                finally
                {
                    activeCompactions.finishCompaction(ci);
                    mergedRowCounts = ci.getMergedRowCounts();
                    totalSourceCQLRows = ci.getTotalSourceCQLRows();
                }
            }

            if (transaction.isOffline())
                return;
            


            // log a bunch of statistics about the result and save to system table
            // compaction_history
            long durationInNano = nanoTime() - start;
            long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
            long startsize = inputSizeBytes;
            long endsize = SSTableReader.getTotalBytes(newSSTables);
            double ratio = (double) endsize / (double) startsize;

            // newSSTables.addAll(headNewSStables);
            // newSSTables.addAll(tailNewSStables);

            StringBuilder newSSTableNames = new StringBuilder();
            for (SSTableReader reader : newSSTables) {
                newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
                ECNetutils.unsetIsSelectedByCompactionOrErasureCodingSSTables(reader.getSSTableHashID());
            }
            long totalSourceRows = 0;
            for (int i = 0; i < mergedRowCounts.length; i++)
                totalSourceRows += mergedRowCounts[i] * (i + 1);

            String mergeSummary = updateCompactionHistory(cfs.keyspace.getName(), cfs.getTableName(), mergedRowCounts,
                    startsize, endsize);
            

            logger.info(String.format(
                    "[Rewrite SSTables]: Rewritten (%s) %d sstables to [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}. Time spent writing keys = %,dms",
                    taskId,
                    transaction.originals().size(),
                    newSSTableNames.toString(),
                    getLevel(),
                    FBUtilities.prettyPrintMemory(startsize),
                    FBUtilities.prettyPrintMemory(endsize),
                    (int) (ratio * 100),
                    dTime,
                    FBUtilities.prettyPrintMemoryPerSecond(startsize, durationInNano),
                    FBUtilities.prettyPrintMemoryPerSecond(endsize, durationInNano),
                    (int) totalSourceCQLRows / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                    totalSourceRows,
                    totalKeysWritten,
                    mergeSummary,
                    timeSpentWritingKeys));
            if (logger.isTraceEnabled()) {
                logger.trace("CF Total Bytes Compacted: {}",
                        FBUtilities.prettyPrintMemory(CompactionTask.addToTotalBytesCompacted(endsize)));
                logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", totalKeysWritten, estimatedKeys,
                        ((double) (totalKeysWritten - estimatedKeys) / totalKeysWritten));
            }
            cfs.getCompactionStrategyManager().compactionLogger.compaction(startTime, transaction.originals(),
                    currentTimeMillis(), newSSTables);

            // update the metrics
            cfs.metric.compactionBytesWritten.inc(endsize);
            logger.debug("Compaction is really done.");
            long timeCost = currentTimeMillis() - startTime;
            StorageService.instance.rewriteTime += timeCost;
        }
    }

    /** [ELECT] compaction for ELECT, only perform on secondary nodes
     * 
     */
    protected void runMayThrow(List<TransferredSSTableKeyRange> TransferredSSTableKeyRanges) throws Exception {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        logger.info("ELECT-Debug[transferred]: This is runMayThrow for secondary node, as this compaction transaction involves transferred sstables");
        assert transaction != null;
        Set<SSTableReader> sstables = new HashSet<SSTableReader>(transaction.originals());
        TimeUUID taskId = transaction.opId();
        logger.debug("[Compaction for ELECT secondary nodes]: rewrite {} sstables, original sstbales number is {}, task id is {}",
                     sstables.size(), transaction.originals().size(), taskId);

        if (transaction.originals().isEmpty())
            return;

        // Note that the current compaction strategy, is not necessarily the one this
        // task was created under.
        // This should be harmless; see comments to CFS.maybeReloadCompactionStrategy.
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();

        if (DatabaseDescriptor.isSnapshotBeforeCompaction()) {
            Instant creationTime = now();
            cfs.snapshotWithoutMemtable(creationTime.toEpochMilli() + "-compact-" + cfs.name, creationTime);
        }

        try (CompactionController controller = getCompactionController(transaction.originals())) {

            final Set<SSTableReader> fullyExpiredSSTables = controller.getFullyExpiredSSTables();

            // select SSTables to compact based on available disk space.
            buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables);

            // sanity check: all sstables must belong to the same cfs
            assert !Iterables.any(transaction.originals(), new Predicate<SSTableReader>() {
                @Override
                public boolean apply(SSTableReader sstable) {
                    return !sstable.descriptor.cfname.equals(cfs.name);
                }
            });


            // new sstables from flush can be added during a compaction, but only the
            // compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining
            // if we're compacting
            // all the sstables (that existed when we started)
            StringBuilder ssTableLoggerMsg = new StringBuilder("[");
            for (SSTableReader sstr : transaction.originals()) {
                ssTableLoggerMsg.append(String.format("%s:level=%d, ", sstr.getFilename(), sstr.getSSTableLevel()));
            }
            ssTableLoggerMsg.append("]");

            logger.info("[Compaction for ELECT secondary nodes] Compacting ({}) {}", taskId, ssTableLoggerMsg);

            RateLimiter limiter = CompactionManager.instance.getRateLimiter();
            long start = nanoTime();
            long startTime = currentTimeMillis();
            long totalKeysWritten = 0;
            long estimatedKeys = 0;
            long inputSizeBytes;
            long timeSpentWritingKeys;

            Set<SSTableReader> actuallyCompact = Sets.difference(transaction.originals(), fullyExpiredSSTables);

            Set<SSTableReader> transferredSSTables = new HashSet<>();
            for (SSTableReader sstable : actuallyCompact) {
                if(sstable.isReplicationTransferredToErasureCoding()) {
                    logger.debug("ELECT-Debug: removing sstable ({}) from actuallyCompact", sstable.descriptor);
                    transferredSSTables.add(sstable);
                }
            }

            if(!transferredSSTables.isEmpty()) {
                transaction.cancel(transferredSSTables);
            }

            actuallyCompact = Sets.difference(actuallyCompact, transferredSSTables);
            

            Collection<SSTableReader> newSStables;

            long[] mergedRowCounts;
            long totalSourceCQLRows;



            int nowInSec = FBUtilities.nowInSeconds();
            try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
                    AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact.stream().sorted(comparator).collect(Collectors.toList()));
                    CompactionIterator ci = new CompactionIterator(compactionType, scanners.scanners, controller,
                            nowInSec, taskId)) {

                if (cfs.getColumnFamilyName().contains("usertable") && !cfs.getColumnFamilyName().equals("usertable0")) {
                    logger.debug("[Compaction for ELECT secondary nodes]: actually compact sstable count is {}, scanners count is {}, task id is {}",
                            actuallyCompact.size(), scanners.scanners.size(), taskId);
                }
                long lastCheckObsoletion = start;
                inputSizeBytes = scanners.getTotalCompressedSize();
                double compressionRatio = scanners.getCompressionRatio();
                if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                    compressionRatio = 1.0;

                long lastBytesScanned = 0;

                activeCompactions.beginCompaction(ci);
                try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, getDirectories(), transaction,
                actuallyCompact)) {
                    // Note that we need to re-check this flag after calling beginCompaction above
                    // to avoid a window
                    // where the compaction does not exist in activeCompactions but the CSM gets
                    // paused.
                    // We already have the sstables marked compacting here so
                    // CompactionManager#waitForCessation will
                    // block until the below exception is thrown and the transaction is cancelled.
                    if (!controller.cfs.getCompactionStrategyManager().isActive())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    estimatedKeys = writer.estimatedKeys();
                    UnfilteredRowIterator prevRow = null;
                    while (ci.hasNext()) {
                        UnfilteredRowIterator row = ci.next();
                        if(prevRow != null) {
                            if(prevRow.partitionKey().compareTo(row.partitionKey()) > 0) {
                                logger.error("ELECT-ERROR: previous partition key {} is larger than current partition key {}!",
                                             prevRow.partitionKey().getToken(), row.partitionKey().getToken());
                            }
                        }
                        if(!isKeyInTransferredSSTableKeyRanges(row.partitionKey(), TransferredSSTableKeyRanges))
                            if (writer.append(row))
                                totalKeysWritten++;
                        prevRow = row;

                        long bytesScanned = scanners.getTotalBytesScanned();

                        // Rate limit the scanners, and account for compression
                        CompactionManager.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned,
                                compressionRatio);

                        lastBytesScanned = bytesScanned;

                        if (nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L)) {
                            controller.maybeRefreshOverlaps();
                            lastCheckObsoletion = nanoTime();
                        }
                    }
                    timeSpentWritingKeys = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

                    
                    // point of no return
                    newSStables = writer.finish();

                    // Iterable<SSTableReader> allSStables = cfs.getSSTables(SSTableSet.LIVE);
                    // for (SSTableReader sst: allSStables) {
                    //     logger.debug(YELLOW+"ELECT-Debug: Compaction is done!!!! sstableHash {}, sstable level {}, sstable name {}, cfName is {}, sstable number is {}",
                    //      stringToHex(sst.getSSTableHashID())+RESET, sst.getSSTableLevel(), sst.getFilename(),
                    //      cfName+RESET, StreamSupport.stream(allSStables.spliterator(), false).count());
                    // }


                }
                finally
                {
                    activeCompactions.finishCompaction(ci);
                    mergedRowCounts = ci.getMergedRowCounts();
                    totalSourceCQLRows = ci.getTotalSourceCQLRows();
                }
            }

            if (transaction.isOffline())
                return;

            // log a bunch of statistics about the result and save to system table
            // compaction_history
            long durationInNano = nanoTime() - start;
            long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
            long startsize = inputSizeBytes;
            long endsize = SSTableReader.getTotalBytes(newSStables);
            double ratio = (double) endsize / (double) startsize;

            StringBuilder newSSTableNames = new StringBuilder();
            for (SSTableReader reader : newSStables) {
                newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
                ECNetutils.unsetIsSelectedByCompactionOrErasureCodingSSTables(reader.getSSTableHashID());
            }
            long totalSourceRows = 0;
            for (int i = 0; i < mergedRowCounts.length; i++)
                totalSourceRows += mergedRowCounts[i] * (i + 1);

            String mergeSummary = updateCompactionHistory(cfs.keyspace.getName(), cfs.getTableName(), mergedRowCounts,
                    startsize, endsize);

            logger.info(String.format(
                    "[Compaction for ELECT secondary nodes]: Compacted (%s) %d sstables to %d [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}. Time spent writing keys = %,dms",
                    taskId,
                    transaction.originals().size() + transferredSSTables.size(),
                    newSStables.size(),
                    newSSTableNames.toString(),
                    getLevel(),
                    FBUtilities.prettyPrintMemory(startsize),
                    FBUtilities.prettyPrintMemory(endsize),
                    (int) (ratio * 100),
                    dTime,
                    FBUtilities.prettyPrintMemoryPerSecond(startsize, durationInNano),
                    FBUtilities.prettyPrintMemoryPerSecond(endsize, durationInNano),
                    (int) totalSourceCQLRows / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                    totalSourceRows,
                    totalKeysWritten,
                    mergeSummary,
                    timeSpentWritingKeys));
            if (logger.isTraceEnabled()) {
                logger.trace("CF Total Bytes Compacted: {}",
                        FBUtilities.prettyPrintMemory(CompactionTask.addToTotalBytesCompacted(endsize)));
                logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", totalKeysWritten, estimatedKeys,
                        ((double) (totalKeysWritten - estimatedKeys) / totalKeysWritten));
            }
            cfs.getCompactionStrategyManager().compactionLogger.compaction(startTime, transaction.originals(),
                    currentTimeMillis(), newSStables);

            // update the metrics
            cfs.metric.compactionBytesWritten.inc(endsize);
            long timeCost = currentTimeMillis() - startTime;
            StorageService.instance.ecSSTableCompactionTime += timeCost;
        }
    }

    private boolean isKeyInTransferredSSTableKeyRanges(DecoratedKey key, List<TransferredSSTableKeyRange> TransferredSSTableKeyRanges) {

        for(TransferredSSTableKeyRange range : TransferredSSTableKeyRanges) {
            if(range.first.compareTo(key)<=0 && range.last.compareTo(key) >= 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * For internal use and testing only. The rest of the system should go through
     * the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     * 
     * For ELECT's primary LSM tree only, we should send parity update signal to parity node if needed.
     */
    protected void runMayThrow() throws Exception {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;
        Set<SSTableReader> sstables = new HashSet<SSTableReader>(transaction.originals());
        TimeUUID taskId = transaction.opId();
        // logger.debug("ELECT-Debug:[Raw Compaction Strategy] rewrite {} sstables, original sstbales number is {}, task id is {}",
        //              sstables.size(), transaction.originals().size(), taskId);

        if (transaction.originals().isEmpty())
            return;


        // Note that the current compaction strategy, is not necessarily the one this
        // task was created under.
        // This should be harmless; see comments to CFS.maybeReloadCompactionStrategy.
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();

        if (DatabaseDescriptor.isSnapshotBeforeCompaction()) {
            Instant creationTime = now();
            cfs.snapshotWithoutMemtable(creationTime.toEpochMilli() + "-compact-" + cfs.name, creationTime);
        }

        try (CompactionController controller = getCompactionController(transaction.originals())) {

            final Set<SSTableReader> fullyExpiredSSTables = controller.getFullyExpiredSSTables();

            // select SSTables to compact based on available disk space.
            buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables);

            // sanity check: all sstables must belong to the same cfs
            assert !Iterables.any(transaction.originals(), new Predicate<SSTableReader>() {
                @Override
                public boolean apply(SSTableReader sstable) {
                    return !sstable.descriptor.cfname.equals(cfs.name);
                }
            });


            // new sstables from flush can be added during a compaction, but only the
            // compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining
            // if we're compacting
            // all the sstables (that existed when we started)
            StringBuilder ssTableLoggerMsg = new StringBuilder("[");
            for (SSTableReader sstr : transaction.originals()) {
                ssTableLoggerMsg.append(String.format("%s:level=%d, ", sstr.getFilename(), sstr.getSSTableLevel()));
            }
            ssTableLoggerMsg.append("]");

            logger.info("Compacting ({}) {}", taskId, ssTableLoggerMsg);

            RateLimiter limiter = CompactionManager.instance.getRateLimiter();
            long start = nanoTime();
            long startTime = currentTimeMillis();
            long totalKeysWritten = 0;
            long estimatedKeys = 0;
            long inputSizeBytes;
            long timeSpentWritingKeys;

            Comparator<SSTableReader> comparator = new Comparator<SSTableReader>() {

                public int compare(SSTableReader o1, SSTableReader o2) {
                    return o1.first.compareTo(o2.first);
                }

            };
            Set<SSTableReader> actuallyCompact = Sets.difference(transaction.originals(), fullyExpiredSSTables);
            
            // [ELECT]
            // store old data before compaction
            int transferredSSTablesNum = 0;
            List<SSTableContentWithHashID> oldTransferredSSTables = new ArrayList<>();
            if(cfs.getColumnFamilyName().equals("usertable0")) {
                for(SSTableReader sstable : actuallyCompact) {
                    if(sstable.isReplicationTransferredToErasureCoding()) {
                        oldTransferredSSTables.add(new SSTableContentWithHashID(sstable.getSSTableHashID(), sstable.getSSTContent()));
                        transferredSSTablesNum++;
                    }
                }
                if(transferredSSTablesNum > DatabaseDescriptor.getMaxStripUpdateSSTables()) {
                    logger.error("ELECT-ERROR: The selected sstable count ({}) is exceed the limit ({})", transferredSSTablesNum, DatabaseDescriptor.getMaxStripUpdateSSTables());
                }
            }


            Collection<SSTableReader> newSStables;

            long[] mergedRowCounts;
            long totalSourceCQLRows;



            int nowInSec = FBUtilities.nowInSeconds();
            try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
                    AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact.stream().sorted(comparator).collect(Collectors.toList()));
                    CompactionIterator ci = new CompactionIterator(compactionType, scanners.scanners, controller,
                            nowInSec, taskId)) {

                // if (cfs.getColumnFamilyName().contains("usertable")) {
                //     logger.debug("ELECT-Debug: actually compact sstable count is {}, scanners count is {}, task id is {}",
                //             actuallyCompact.size(), scanners.scanners.size(), taskId);
                // }
                long lastCheckObsoletion = start;
                inputSizeBytes = scanners.getTotalCompressedSize();
                double compressionRatio = scanners.getCompressionRatio();
                if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                    compressionRatio = 1.0;

                long lastBytesScanned = 0;

                activeCompactions.beginCompaction(ci);
                try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, getDirectories(), transaction,
                actuallyCompact)) {
                    // Note that we need to re-check this flag after calling beginCompaction above
                    // to avoid a window
                    // where the compaction does not exist in activeCompactions but the CSM gets
                    // paused.
                    // We already have the sstables marked compacting here so
                    // CompactionManager#waitForCessation will
                    // block until the below exception is thrown and the transaction is cancelled.
                    if (!controller.cfs.getCompactionStrategyManager().isActive())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    estimatedKeys = writer.estimatedKeys();
                    UnfilteredRowIterator prevRow = null;
                    while (ci.hasNext()) {
                        UnfilteredRowIterator row = ci.next();
                        if(prevRow != null) {
                            if(prevRow.partitionKey().compareTo(row.partitionKey()) > 0) {
                                logger.error("ELECT-ERROR: previous partition key {} is larger than current partition key {}!",
                                             prevRow.partitionKey().getToken(), row.partitionKey().getToken());
                            }
                        }
                        if (writer.append(row))
                            totalKeysWritten++;
                        prevRow = row;

                        long bytesScanned = scanners.getTotalBytesScanned();

                        // Rate limit the scanners, and account for compression
                        CompactionManager.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned,
                                compressionRatio);

                        lastBytesScanned = bytesScanned;

                        if (nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L)) {
                            controller.maybeRefreshOverlaps();
                            lastCheckObsoletion = nanoTime();
                        }
                    }
                    timeSpentWritingKeys = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

                    // point of no return
                    newSStables = writer.finish();

                    // Iterable<SSTableReader> allSStables = cfs.getSSTables(SSTableSet.LIVE);
                    // for (SSTableReader sst: allSStables) {
                    //     logger.debug(YELLOW+"ELECT-Debug: Compaction is done!!!! sstableHash {}, sstable level {}, sstable name {}, cfName is {}, sstable number is {}",
                    //      stringToHex(sst.getSSTableHashID())+RESET, sst.getSSTableLevel(), sst.getFilename(),
                    //      cfName+RESET, StreamSupport.stream(allSStables.spliterator(), false).count());
                    // }


                }
                finally
                {
                    activeCompactions.finishCompaction(ci);
                    mergedRowCounts = ci.getMergedRowCounts();
                    totalSourceCQLRows = ci.getTotalSourceCQLRows();
                }
            }

            if (transaction.isOffline())
                return;
            
            // [CASSADNRAEC]
            // Compaction is done: match the old/new data, and send them to parity node
            if(cfs.getColumnFamilyName().equals("usertable0") && transferredSSTablesNum > 0) {
                // send parity update signal
                // send old data 
                // List<InetAddressAndPort> targets = new ArrayList<>();
                // paritNode -> oldSSTables 
                Map<InetAddressAndPort, List<SSTableContentWithHashID>> oldSSTables = new HashMap<InetAddressAndPort, List<SSTableContentWithHashID>>();
                // Set<InetAddressAndPort> targets = new HashSet<>();
                for (SSTableContentWithHashID oldTransferredSST : oldTransferredSSTables) {
                    String oldSSTHash = oldTransferredSST.sstHash;
                    InetAddressAndPort target = null;
                    try {
                        // target is the first parity nodes
                        target = StorageService.instance.globalSSTHashToParityNodesMap.get(oldSSTHash).get(0);
                        // targets.add(target);
                    } catch (Exception e) {
                        throw new NullPointerException(String.format("ELECT-ERROR: cannot get parity nodes for sstHash (%s)", oldSSTHash));
                    }
                    // targets.add(target);
                    if(oldSSTables.containsKey(target)){
                        oldSSTables.get(target).add(oldTransferredSST);
                    } else {
                        oldSSTables.put(target, new ArrayList<SSTableContentWithHashID>(Collections.singletonList(oldTransferredSST)));
                    }
                }

                // if(targets.size() > 0) {
                //     throw new IllegalStateException(String.format("The parity nodes (%s) of old sstables is not unique!", targets));
                // }

                // handle the new data 
                Collection<SSTableReader> newSStablesForUpdate = new ArrayList<SSTableReader>(newSStables);
                Iterator<SSTableReader> newSSTableIterator = newSStablesForUpdate.iterator();
                for(Map.Entry<InetAddressAndPort, List<SSTableContentWithHashID>> entry : oldSSTables.entrySet()) {
                    // InetAddressAndPort target = entry.getKey();
                    int requireNewSSTableNum = entry.getValue().size();
                    if (requireNewSSTableNum > 0) {
                        List<InetAddressAndPort> parityNodes = new ArrayList<>(StorageService.instance.globalSSTHashToParityNodesMap.get(entry.getValue().get(0).sstHash));
                        List<SSTableContentWithHashID> newSSTableContentWithHashID = new ArrayList<>();
                        while (requireNewSSTableNum-- > 0) {
                            if (newSSTableIterator.hasNext()) {
                                SSTableReader newSSTable = newSSTableIterator.next();
                                newSSTableContentWithHashID.add(new SSTableContentWithHashID(
                                        newSSTable.getSSTableHashID(), newSSTable.getSSTContent()));
                                // set this sstable as updated
                                newSSTable.setIsParityUpdate();
                                newSSTable.SetIsReplicationTransferredToErasureCoding();

                                // Sync selected new sstables to secondary nodes for parity update
                                List<InetAddressAndPort> replicaNodes = StorageService.instance.getReplicaNodesWithPortFromPrimaryNode(
                                                                                                FBUtilities.getBroadcastAddressAndPort(), cfs.keyspace.getName());
                                
                                ECNetutils.syncSSTableWithSecondaryNodes(newSSTable, replicaNodes, newSSTable.getSSTableHashID(), "Parity Update", cfs);
                                StorageService.instance.globalSSTHashToParityNodesMap.put(newSSTable.getSSTableHashID(),
                                                                                          StorageService.instance.globalSSTHashToParityNodesMap.get(entry.getValue().get(0).sstHash));
                                logger.debug("ELECT-Debug: [Parity Update] we map new sstHash ({}) to parity Nodes ({})",
                                        newSSTable.getSSTableHashID(),
                                        StorageService.instance.globalSSTHashToParityNodesMap
                                                .get(entry.getValue().get(0).sstHash));
                                newSSTableIterator.remove();
                            } else {
                                break;
                            }
                        }

                        // Debug: check if the sstables' parity nodes are the same
                        // String logString = "ELECT-Debug: Check the parity nodes of old sstables, ";
                        // for(SSTableContentWithHashID sst : entry.getValue()) {
                        // logString += "the parity nodes of sstable " + sst.sstHash + "is " + StorageService.instance.globalSSTHashToParityNodesMap.get(sst.sstHash) + ",";}
                        // logger.debug(logString);

                        // send the old sstable and new sstable to target parity node
                        // ECParityUpdate parityUpdate = new ECParityUpdate(entry.getValue(),
                        // newSSTables,
                        // StorageService.instance.globalSSTHashToParityNodesMap.get(entry.getValue().get(0).sstHash));
                        

                        for (SSTableContentWithHashID newSSTable : newSSTableContentWithHashID) {
                            logger.debug("ELECT-Debug: send new sstable ({}) to parity node ({})", newSSTable.sstHash, parityNodes.get(0));
                            ECParityUpdate parityUpdate = new ECParityUpdate(newSSTable, false, parityNodes);
                            StorageService.instance.transferredSSTableCount++;
                            parityUpdate.sendParityUpdateSignal();
                        }

                        
                        for (SSTableContentWithHashID oldSSTable : entry.getValue()) {
                            if(parityNodes.isEmpty())
                                parityNodes = StorageService.instance.globalSSTHashToParityNodesMap.get(oldSSTable.sstHash);
                            else {
                                if(!parityNodes.equals(StorageService.instance.globalSSTHashToParityNodesMap.get(oldSSTable.sstHash))) {

                                    String logString = "";
                                    for (SSTableContentWithHashID sst : entry.getValue()) {
                                        logString += "the parity nodes of sstable " + sst.sstHash + "is "
                                                + StorageService.instance.globalSSTHashToParityNodesMap.get(sst.sstHash)
                                                + ",";
                                    }

                                    throw new IllegalStateException(String.format("ELECT-ERROR: The parity nodes are different!!! {%s}", logString));
                                }
                            }
                            StorageService.instance.transferredSSTableCount--;
                            ECParityUpdate parityUpdate = new ECParityUpdate(oldSSTable, true, parityNodes);

                            logger.debug("ELECT-Debug: send old sstable ({}) to parity node ({})", oldSSTable.sstHash, parityNodes.get(0));
                            parityUpdate.sendParityUpdateSignal();

                            // remove the entry to save memory
                            StorageService.instance.globalSSTHashToParityNodesMap.remove(oldSSTable.sstHash);

                        }


                        logger.debug("ELECT-Debug: For compaction task ({}), we send ({}) new sstables and ({}) old sstables to parity node ({}), the sstables count before compaction ({}), after compaction ({}), this txn has ({}) transferred sstables.",
                                         taskId, newSSTableContentWithHashID.size(), entry.getValue().size(), entry.getKey(), transaction.originals().size(), newSStables.size(), transferredSSTablesNum);


                        if(newSSTableContentWithHashID.size() > entry.getValue().size()) {
                            logger.error("ELECT-ERROR: New sstables count ({}) is more than old sstables count ({}), check the code.", newSSTableContentWithHashID.size(), entry.getValue().size());
                        }

                    }
                }
            }

            // log a bunch of statistics about the result and save to system table
            // compaction_history
            long durationInNano = nanoTime() - start;
            long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
            long startsize = inputSizeBytes;
            long endsize = SSTableReader.getTotalBytes(newSStables);
            double ratio = (double) endsize / (double) startsize;


            StringBuilder newSSTableNames = new StringBuilder();
            for (SSTableReader reader : newSStables) {
                newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
                ECNetutils.unsetIsSelectedByCompactionOrErasureCodingSSTables(reader.getSSTableHashID());
            }
            long totalSourceRows = 0;
            for (int i = 0; i < mergedRowCounts.length; i++)
                totalSourceRows += mergedRowCounts[i] * (i + 1);

            String mergeSummary = updateCompactionHistory(cfs.keyspace.getName(), cfs.getTableName(), mergedRowCounts,
                    startsize, endsize);

            logger.info(String.format(
                    "[Raw Compaction] Compacted (%s) %d sstables to %d [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}. Time spent writing keys = %,dms",
                    taskId,
                    transaction.originals().size(),
                    newSStables.size(),
                    newSSTableNames.toString(),
                    getLevel(),
                    FBUtilities.prettyPrintMemory(startsize),
                    FBUtilities.prettyPrintMemory(endsize),
                    (int) (ratio * 100),
                    dTime,
                    FBUtilities.prettyPrintMemoryPerSecond(startsize, durationInNano),
                    FBUtilities.prettyPrintMemoryPerSecond(endsize, durationInNano),
                    (int) totalSourceCQLRows / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                    totalSourceRows,
                    totalKeysWritten,
                    mergeSummary,
                    timeSpentWritingKeys));
            if (logger.isTraceEnabled()) {
                logger.trace("CF Total Bytes Compacted: {}",
                        FBUtilities.prettyPrintMemory(CompactionTask.addToTotalBytesCompacted(endsize)));
                logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", totalKeysWritten, estimatedKeys,
                        ((double) (totalKeysWritten - estimatedKeys) / totalKeysWritten));
            }
            cfs.getCompactionStrategyManager().compactionLogger.compaction(startTime, transaction.originals(),
                    currentTimeMillis(), newSStables);

            // update the metrics
            cfs.metric.compactionBytesWritten.inc(endsize);
            long timeCost = currentTimeMillis() - startTime;
            StorageService.instance.compactionTime += timeCost;

            // logger.debug("[Raw compaction strategy]: After update the metrics, the sstables number remained in transaction {} is {}",
            //  transaction.opId(), transaction.originals().size());
        }
    }

    public static String stringToHex(String str) {
        byte[] bytes = str.getBytes();
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16))
               .append(Character.forDigit((b & 0xF), 16));
        }
        return hex.toString();
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
            Directories directories,
            LifecycleTransaction transaction,
            Set<SSTableReader> nonExpiredSSTables) {
        return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals,
                getLevel());
    }

    public static String updateCompactionHistory(String keyspaceName, String columnFamilyName, long[] mergedRowCounts,
            long startSize, long endSize) {
        StringBuilder mergeSummary = new StringBuilder(mergedRowCounts.length * 10);
        Map<Integer, Long> mergedRows = new HashMap<>();
        for (int i = 0; i < mergedRowCounts.length; i++) {
            long count = mergedRowCounts[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergeSummary.append(String.format("%d:%d, ", rows, count));
            mergedRows.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(keyspaceName, columnFamilyName, currentTimeMillis(), startSize, endSize,
                mergedRows);
        return mergeSummary.toString();
    }

    protected Directories getDirectories() {
        return cfs.getDirectories();
    }

    public static long getMinRepairedAt(Set<SSTableReader> actuallyCompact) {
        long minRepairedAt = Long.MAX_VALUE;
        for (SSTableReader sstable : actuallyCompact)
            minRepairedAt = Math.min(minRepairedAt, sstable.getSSTableMetadata().repairedAt);
        if (minRepairedAt == Long.MAX_VALUE)
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        return minRepairedAt;
    }

    public static TimeUUID getPendingRepair(Set<SSTableReader> sstables) {
        if (sstables.isEmpty()) {
            return ActiveRepairService.NO_PENDING_REPAIR;
        }
        Set<TimeUUID> ids = new HashSet<>();
        for (SSTableReader sstable : sstables)
            ids.add(sstable.getSSTableMetadata().pendingRepair);

        if (ids.size() != 1)
            throw new RuntimeException(String.format(
                    "Attempting to compact pending repair sstables with sstables from other repair, or sstables not pending repair: %s",
                    ids));

        return ids.iterator().next();
    }

    public static boolean getIsTransient(Set<SSTableReader> sstables) {
        if (sstables.isEmpty()) {
            return false;
        }

        boolean isTransient = sstables.iterator().next().isTransient();

        if (!Iterables.all(sstables, sstable -> sstable.isTransient() == isTransient)) {
            throw new RuntimeException("Attempting to compact transient sstables with non transient sstables");
        }

        return isTransient;
    }

    /*
     * Checks if we have enough disk space to execute the compaction. Drops the
     * largest sstable out of the Task until
     * there's enough space (in theory) to handle the compaction. Does not take into
     * account space that will be taken by
     * other compactions.
     */
    protected void buildCompactionCandidatesForAvailableDiskSpace(final Set<SSTableReader> fullyExpiredSSTables) {
        if (!cfs.isCompactionDiskSpaceCheckEnabled() && compactionType == OperationType.COMPACTION) {
            logger.info("Compaction space check is disabled");
            return; // try to compact all SSTables
        }

        final Set<SSTableReader> nonExpiredSSTables = Sets.difference(transaction.originals(), fullyExpiredSSTables);
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        int sstablesRemoved = 0;

        while (!nonExpiredSSTables.isEmpty()) {
            // Only consider write size of non expired SSTables
            long expectedWriteSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);
            long estimatedSSTables = Math.max(1, expectedWriteSize / strategy.getMaxSSTableBytes());

            if (cfs.getDirectories().hasAvailableDiskSpace(estimatedSSTables, expectedWriteSize))
                break;

            if (!reduceScopeForLimitedSpace(nonExpiredSSTables, expectedWriteSize)) {
                // we end up here if we can't take any more sstables out of the compaction.
                // usually means we've run out of disk space

                // but we can still compact expired SSTables
                if (partialCompactionsAcceptable() && fullyExpiredSSTables.size() > 0) {
                    // sanity check to make sure we compact only fully expired SSTables.
                    assert transaction.originals().equals(fullyExpiredSSTables);
                    break;
                }

                String msg = String.format(
                        "Not enough space for compaction, estimated sstables = %d, expected write size = %d",
                        estimatedSSTables, expectedWriteSize);
                logger.warn(msg);
                CompactionManager.instance.incrementAborted();
                throw new RuntimeException(msg);
            }

            sstablesRemoved++;
            logger.warn("Not enough space for compaction, {}MiB estimated.  Reducing scope.",
                    (float) expectedWriteSize / 1024 / 1024);
        }

        if (sstablesRemoved > 0) {
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions(sstablesRemoved);
        }

    }

    protected int getLevel() {
        return 0;
    }

    protected CompactionController getCompactionController(Set<SSTableReader> toCompact) {
        return new CompactionController(cfs, toCompact, gcBefore);
    }

    protected boolean partialCompactionsAcceptable() {
        return !isUserDefined;
    }

    public static long getMaxDataAge(Collection<SSTableReader> sstables) {
        long max = 0;
        for (SSTableReader sstable : sstables) {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }

}
