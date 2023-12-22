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
package org.apache.cassandra.io.sstable.metadata;

import org.apache.cassandra.io.util.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.zip.CRC32;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.FBUtilities.now;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Metadata serializer for SSTables {@code version >= 'na'}.
 *
 * <pre>
 * File format := | number of components (4 bytes) | crc | toc | crc | component1 | c1 crc | component2 | c2 crc | ... |
 * toc         := | component type (4 bytes) | position of component |
 * </pre>
 *
 * IMetadataComponent.Type's ordinal() defines the order of serialization.
 */
public class MetadataSerializer implements IMetadataSerializer {
    private static final Logger logger = LoggerFactory.getLogger(MetadataSerializer.class);

    private static final int CHECKSUM_LENGTH = 4; // CRC32

    public void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Version version)
            throws IOException {
        boolean checksum = version.hasMetadataChecksum();
        CRC32 crc = new CRC32();
        // sort components by type
        List<MetadataComponent> sortedComponents = Lists.newArrayList(components.values());
        Collections.sort(sortedComponents);

        // write number of component
        out.writeInt(components.size());
        // logger.debug("[ELECT] write check sum total number = {}",
        // components.size());
        updateChecksumInt(crc, components.size());
        maybeWriteChecksum(crc, out, version);

        // build and write toc
        int lastPosition = 4 + (8 * sortedComponents.size()) + (checksum ? 2 * CHECKSUM_LENGTH : 0);
        // logger.debug("[ELECT] gen CRC lastPosition = {}", lastPosition);
        // ----------------
        Map<MetadataType, Integer> sizes = new EnumMap<>(MetadataType.class);
        for (MetadataComponent component : sortedComponents) {
            MetadataType type = component.getType();
            // serialize type
            out.writeInt(type.ordinal());
            updateChecksumInt(crc, type.ordinal());
            // serialize position
            out.writeInt(lastPosition);
            updateChecksumInt(crc, lastPosition);
            int size = type.serializer.serializedSize(version, component);
            // if (type == MetadataType.STATS) {
            // size = size * 2;
            // }
            lastPosition += size + (checksum ? CHECKSUM_LENGTH : 0);
            sizes.put(type, size);
        }
        maybeWriteChecksum(crc, out, version);
        // serialize components
        for (MetadataComponent component : sortedComponents) {
            byte[] bytes;
            try (DataOutputBuffer dob = new DataOutputBuffer(sizes.get(component.getType()))) {
                component.getType().serializer.serialize(version, component, dob);
                bytes = dob.getData();
            }
            Boolean isSizeCorrectFlag;
            if (bytes.length != sizes.get(component.getType())) {
                // logger.debug(
                // "[ELECT] the component = {}, generated serialized size = {}, the actual get
                // serialized size = {}",
                // component.getType(),
                // sizes.get(component.getType()),
                // bytes.length);
                isSizeCorrectFlag = false;
            } else {
                isSizeCorrectFlag = true;
            }
            if (isSizeCorrectFlag) {
                out.write(bytes);
                crc.reset();
                crc.update(bytes);
            } else {
                byte[] byteCutToSize = new byte[sizes.get(component.getType())];
                System.arraycopy(byteCutToSize, 0, bytes, 0, sizes.get(component.getType()));
                out.write(byteCutToSize);
                crc.reset();
                crc.update(byteCutToSize);
            }

            maybeWriteChecksum(crc, out, version);
            MetadataType type = component.getType();
            // logger.debug(
            // "[ELECT] gen CRC for metadata type = {}, type in int = [{}], crc = [{}],
            // the serialized size = {}, the generated serialized size = {}",
            // type,
            // type.ordinal(),
            // (int) crc.getValue(), bytes.length, sizes.get(component.getType()));
        }
    }

    private static void maybeWriteChecksum(CRC32 crc, DataOutputPlus out, Version version) throws IOException {
        if (version.hasMetadataChecksum())
            out.writeInt((int) crc.getValue());
    }

    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, EnumSet<MetadataType> types)
            throws IOException {
        Map<MetadataType, MetadataComponent> components;
        logger.trace("Load metadata for {}", descriptor);
        File statsFile = new File(descriptor.filenameFor(Component.STATS));
        if (!statsFile.exists()) {
            logger.trace("No sstable stats for {}", descriptor);
            components = new EnumMap<>(MetadataType.class);
            components.put(MetadataType.STATS, MetadataCollector.defaultStatsMetadata());
        } else {
            try (RandomAccessReader r = RandomAccessReader.open(statsFile)) {
                components = deserialize(descriptor, r, types);
            }
        }
        return components;
    }

    public MetadataComponent deserialize(Descriptor descriptor, MetadataType type) throws IOException {
        return deserialize(descriptor, EnumSet.of(type)).get(type);
    }

    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor,
            FileDataInput in,
            EnumSet<MetadataType> selectedTypes)
            throws IOException {
        boolean isChecksummed = descriptor.version.hasMetadataChecksum();
        CRC32 crc = new CRC32();

        /*
         * Read TOC
         */

        int length = (int) in.bytesRemaining();

        int count = in.readInt();
        // logger.debug("[ELECT] read check sum total number = {}, total length is =
        // {}", count, length);

        updateChecksumInt(crc, count);
        maybeValidateChecksum(crc, in, descriptor);

        int[] ordinals = new int[count];
        int[] offsets = new int[count];
        int[] lengths = new int[count];

        for (int i = 0; i < count; i++) {
            ordinals[i] = in.readInt();
            updateChecksumInt(crc, ordinals[i]);

            offsets[i] = in.readInt();
            updateChecksumInt(crc, offsets[i]);
            // logger.debug("[ELECT] read check sum ordinals[{}] = {}, offsets[{}] = {}",
            // i, ordinals[i], i, offsets[i]);
        }
        maybeValidateChecksum(crc, in, descriptor);

        lengths[count - 1] = length - offsets[count - 1];
        for (int i = 0; i < count - 1; i++)
            lengths[i] = offsets[i + 1] - offsets[i];

        /*
         * Read components
         */

        MetadataType[] allMetadataTypes = MetadataType.values();
        // logger.debug("[ELECT] Target process metadata type number = {}, include
        // {}", allMetadataTypes.length,
        // allMetadataTypes);
        Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);

        for (int i = 0; i < count; i++) {
            MetadataType type = allMetadataTypes[ordinals[i]];

            if (!selectedTypes.contains(type)) {
                in.skipBytes(lengths[i]);
                continue;
            }
            // logger.debug("[ELECT] Process metadata type = {}, length = {}", type,
            // isChecksummed ? lengths[i] - CHECKSUM_LENGTH : lengths[i]);
            byte[] buffer = new byte[isChecksummed ? lengths[i] - CHECKSUM_LENGTH : lengths[i]];
            in.readFully(buffer);

            crc.reset();
            crc.update(buffer);
            maybeValidateChecksum(crc, in, descriptor, type);
            try (DataInputBuffer dataInputBuffer = new DataInputBuffer(buffer)) {
                components.put(type, type.serializer.deserialize(descriptor.version, dataInputBuffer));
            }
        }

        return components;
    }

    private static void maybeValidateChecksum(CRC32 crc, FileDataInput in, Descriptor descriptor) throws IOException {
        if (!descriptor.version.hasMetadataChecksum())
            return;

        int actualChecksum = (int) crc.getValue();
        int expectedChecksum = in.readInt();
        String filename = descriptor.filenameFor(Component.STATS);
        if (actualChecksum != expectedChecksum) {
            logger.error(
                    "[ELECT-ERROR] get original check sum [{}], the actual check sum is [{}], file name = {}, actual file size = {}",
                    expectedChecksum,
                    actualChecksum, filename, (new File(filename)).length());
        }
        // if (actualChecksum != expectedChecksum) {
        // String filename = descriptor.filenameFor(Component.STATS);
        // throw new CorruptSSTableException(new IOException("Checksums do not match for
        // " + filename), filename);
        // }
        return;

    }

    private static void maybeValidateChecksum(CRC32 crc, FileDataInput in, Descriptor descriptor, MetadataType type)
            throws IOException {
        if (!descriptor.version.hasMetadataChecksum())
            return;

        int actualChecksum = (int) crc.getValue();
        int expectedChecksum = in.readInt();

        String filename = descriptor.filenameFor(Component.STATS);
        if (actualChecksum != expectedChecksum) {
            logger.error("ELECT-ERROR: actual checksum is {}, expected checksum is {}, check type is {}, file name is {}",
                    actualChecksum, expectedChecksum, type, filename);
            throw new CorruptSSTableException(new IOException("Checksums do not match for " + filename), filename);
        } else if (!filename.contains("usertable0-")) {
            // logger.info("ELECT-Info: file name {} for type {} checksum is correct",
            // filename, type);
        }
    }

    @Override
    public void mutate(Descriptor descriptor, String description, UnaryOperator<StatsMetadata> transform)
            throws IOException {
        if (logger.isTraceEnabled())
            logger.trace("Mutating {} to {}", descriptor.filenameFor(Component.STATS), description);

        mutate(descriptor, transform);
    }

    @Override
    public void mutateLevel(Descriptor descriptor, int newLevel) throws IOException {
        if (logger.isTraceEnabled())
            logger.trace("Mutating {} to level {}", descriptor.filenameFor(Component.STATS), newLevel);

        mutate(descriptor, stats -> stats.mutateLevel(newLevel));
    }

    public void setIsTransferredToErasureCoding(Descriptor descriptor, String sstHash, boolean isTransferredToErasureCoding) throws IOException {
        // if (logger.isTraceEnabled())
        logger.debug("Set {}, {} to isTransferredToErasureCoding as {}", descriptor.filenameFor(Component.STATS), sstHash, isTransferredToErasureCoding);

        mutateTransferredFlag(descriptor, sstHash, isTransferredToErasureCoding);
    }

    public void setIsDataMigrateToCloud(Descriptor descriptor, String sstHash, boolean flag) throws IOException{
        logger.debug("Set {}, {} to IsDataMigrateToCloud as {}", descriptor.filenameFor(Component.STATS), sstHash, flag);
        // mutate(descriptor, stats -> stats.setIsDataMigrateToCloud(flag));
        mutateMigrationFlag(descriptor, sstHash, flag);
    }

    @Override
    public void mutateRepairMetadata(Descriptor descriptor, long newRepairedAt, TimeUUID newPendingRepair,
            boolean isTransient) throws IOException {
        if (logger.isTraceEnabled())
            logger.trace("Mutating {} to repairedAt time {} and pendingRepair {}",
                    descriptor.filenameFor(Component.STATS), newRepairedAt, newPendingRepair);

        mutate(descriptor, stats -> stats.mutateRepairedMetadata(newRepairedAt, newPendingRepair, isTransient));
    }


    private void mutateTransferredFlag(Descriptor descriptor, String sstHash, boolean isTransferredToErasureCoding) throws IOException {
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor,
                EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);

        currentComponents.put(MetadataType.STATS, new StatsMetadata(stats.estimatedPartitionSize, 
                                                                    stats.estimatedCellPerPartitionCount,
                                                                    stats.commitLogIntervals,
                                                                    now().getLong(ChronoField.MILLI_OF_SECOND),
                                                                    stats.minTimestamp,
                                                                    stats.maxTimestamp,
                                                                    stats.minLocalDeletionTime,
                                                                    stats.maxLocalDeletionTime,
                                                                    stats.minTTL,
                                                                    stats.maxTTL,
                                                                    stats.compressionRatio,
                                                                    stats.estimatedTombstoneDropTime,
                                                                    stats.sstableLevel,
                                                                    stats.minClusteringValues,
                                                                    stats.maxClusteringValues,
                                                                    stats.hasLegacyCounterShards,
                                                                    stats.repairedAt,
                                                                    stats.totalColumnsSet,
                                                                    stats.totalRows,
                                                                    stats.originatingHostId,
                                                                    stats.pendingRepair,
                                                                    stats.isTransient,
                                                                    stats.isDataMigrateToCloud,
                                                                    isTransferredToErasureCoding,
                                                                    stats.hashID,
                                                                    stats.dataFileSize));
        logger.debug("ELECT-Debug: before rewriteSSTableMetadata method, the transferred flag of sstable ({}) is ({}), isTransferredToErasureCoding ({})", 
                            sstHash, ((StatsMetadata)currentComponents.get(MetadataType.STATS)).isReplicationTransferToErasureCoding, isTransferredToErasureCoding);
        rewriteSSTableMetadata(descriptor, currentComponents);
        logger.debug("ELECT-Debug: after rewriteSSTableMetadata method, the transferred flag of sstable ({}) is ({})", sstHash, ((StatsMetadata)currentComponents.get(MetadataType.STATS)).isReplicationTransferToErasureCoding);
    }

    private void mutateMigrationFlag(Descriptor descriptor, String sstHash, boolean migrationFlag) throws IOException {
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor,
                EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);

        currentComponents.put(MetadataType.STATS, new StatsMetadata(stats.estimatedPartitionSize, 
                                                                    stats.estimatedCellPerPartitionCount,
                                                                    stats.commitLogIntervals,
                                                                    now().getLong(ChronoField.MILLI_OF_SECOND),
                                                                    stats.minTimestamp,
                                                                    stats.maxTimestamp,
                                                                    stats.minLocalDeletionTime,
                                                                    stats.maxLocalDeletionTime,
                                                                    stats.minTTL,
                                                                    stats.maxTTL,
                                                                    stats.compressionRatio,
                                                                    stats.estimatedTombstoneDropTime,
                                                                    stats.sstableLevel,
                                                                    stats.minClusteringValues,
                                                                    stats.maxClusteringValues,
                                                                    stats.hasLegacyCounterShards,
                                                                    stats.repairedAt,
                                                                    stats.totalColumnsSet,
                                                                    stats.totalRows,
                                                                    stats.originatingHostId,
                                                                    stats.pendingRepair,
                                                                    stats.isTransient,
                                                                    migrationFlag,
                                                                    stats.isReplicationTransferToErasureCoding,
                                                                    stats.hashID,
                                                                    stats.dataFileSize));
        logger.debug("ELECT-Debug: before rewriteSSTableMetadata method, the migrationFlag of sstable ({}) is ({}), migrationFlag ({})", 
                            sstHash, ((StatsMetadata)currentComponents.get(MetadataType.STATS)).isDataMigrateToCloud, migrationFlag);
        rewriteSSTableMetadata(descriptor, currentComponents);
        logger.debug("ELECT-Debug: after rewriteSSTableMetadata method, the migrationFlag of sstable ({}) is ({})", sstHash, ((StatsMetadata)currentComponents.get(MetadataType.STATS)).isDataMigrateToCloud);
    }
    private void mutate(Descriptor descriptor, UnaryOperator<StatsMetadata> transform) throws IOException {
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor,
                EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);

        currentComponents.put(MetadataType.STATS, transform.apply(stats));
        rewriteSSTableMetadata(descriptor, currentComponents);
    }

    public void rewriteSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> currentComponents)
            throws IOException {
        String filePath = descriptor.tmpFilenameFor(Component.STATS);
        try (DataOutputStreamPlus out = new FileOutputStreamPlus(filePath)) {
            serialize(currentComponents, out, descriptor.version);
            out.flush();
        } catch (IOException e) {
            Throwables.throwIfInstanceOf(e, FileNotFoundException.class);
            throw new FSWriteError(e, filePath);
        }
        FileUtils.renameWithConfirm(filePath, descriptor.filenameFor(Component.STATS));
    }
}
