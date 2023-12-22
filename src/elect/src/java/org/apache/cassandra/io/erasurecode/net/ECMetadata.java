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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate.SSTableContentWithHashID;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to generate ECMetadata and distribute metadata to the replica
 * nodes
 * 
 * @param stripeId               the global unique id of ECMetadata, generated
 *                               from the {@value sstHashList}
 * @param ecMetadataContent      the content of ECMetadata
 * @param ecMetadataContentBytes the serialized data of ecMetadataContent
 * @param ecMetadataSize         the size of the content of ecMetadata
 * 
 *                               ecMetadataContent is consist of the following
 *                               parameters:
 * @param keyspace
 * @param cfName
 * @param sstHashList            the hash code list of the data blocks
 * @param parityHashList         the hash code list of the parity blocks
 * @param primaryNodes
 * @param secondaryNodes
 * @param parityNodes            Note that the parity nodes are the same among
 *                               each entry
 * 
 * 
 *                               There are two key methods:
 * @method generateMetadata
 * @method distributeEcMetadata
 */

public class ECMetadata implements Serializable {
    // TODO: improve the performance
    // public String stripeId;

    public ECMetadataContent ecMetadataContent;

    public byte[] ecMetadataContentBytes;
    public int ecMetadataContentBytesSize;

    private static final Logger logger = LoggerFactory.getLogger(ECMetadata.class);
    public static final Serializer serializer = new Serializer();

    public static class ECMetadataContent implements Serializable {

        public String stripeId;
        public String keyspace;
        public String cfName;
        public List<String> sstHashIdList;
        public List<String> parityHashList;
        public Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap;
        public List<InetAddressAndPort> primaryNodes;
        public Set<InetAddressAndPort> secondaryNodes;
        public List<InetAddressAndPort> parityNodes;

        // The following properties is only for parity update
        public boolean isParityUpdate;
        public int targetIndex;
        public String oldSSTHashForUpdate;

        // Properties for recovery
        public int zeroChunksNum;

        public ECMetadataContent(String stripeId, String ks, String cf, List<String> sstHashIdList,
                List<String> parityHashList,
                List<InetAddressAndPort> primaryNodes, Set<InetAddressAndPort> secondaryNodes,
                List<InetAddressAndPort> parityNodes,
                Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap, String oldSSTHashForUpdate,
                boolean isParityUpdate, int targetIndex, int zeroChunkNum) {
            this.stripeId = stripeId;
            this.keyspace = ks;
            this.cfName = cf;
            this.sstHashIdList = sstHashIdList;
            this.parityHashList = parityHashList;
            this.primaryNodes = primaryNodes;
            this.secondaryNodes = secondaryNodes;
            this.parityNodes = parityNodes;
            this.sstHashIdToReplicaMap = sstHashIdToReplicaMap;
            this.oldSSTHashForUpdate = oldSSTHashForUpdate;
            this.isParityUpdate = isParityUpdate;
            this.targetIndex = targetIndex;
            this.zeroChunksNum = zeroChunkNum;
        }
    }

    public ECMetadata(ECMetadataContent ecMetadataContent) {
        // this.stripeId = stripeId;
        this.ecMetadataContent = ecMetadataContent;
    }

    /**
     * This method is to generate the metadata for the given messages
     * 
     * @param messages     this is the data block for erasure coding, this size is
     *                     equal to k
     * @param parityCode   this is the parity block for erasure coding, size is m
     * @param parityHashes
     */
    public synchronized void generateAndDistributeMetadata(ECMessage[] messages, List<String> parityHashes) {
        logger.debug("ELECT-Debug: this generateMetadata method");
        // get stripe id, sst content hashes and primary nodes
        String connectedSSTHash = "";
        for (ECMessage msg : messages) {
            String sstContentHash = msg.ecMessageContent.sstHashID;
            this.ecMetadataContent.sstHashIdList.add(sstContentHash);
            connectedSSTHash += sstContentHash;
            if (!msg.ecMessageContent.replicaNodes.isEmpty()) {
                this.ecMetadataContent.sstHashIdToReplicaMap.put(sstContentHash, msg.ecMessageContent.replicaNodes);
                this.ecMetadataContent.primaryNodes.add(msg.ecMessageContent.replicaNodes.get(0));
            } else {
                this.ecMetadataContent.primaryNodes.add(FBUtilities.getBroadcastAddressAndPort());
            }
        }

        this.ecMetadataContent.stripeId = String.valueOf(connectedSSTHash.hashCode());
        this.ecMetadataContent.keyspace = messages[0].ecMessageContent.keyspace;
        this.ecMetadataContent.cfName = messages[0].ecMessageContent.cfName.substring(0,
                messages[0].ecMessageContent.cfName.length() - 1);

        // generate parity code hash
        this.ecMetadataContent.parityHashList = parityHashes;

        // get related nodes
        // if everything goes well, each message has the same parity code
        this.ecMetadataContent.parityNodes.addAll(messages[0].ecMessageContent.parityNodes);

        // initialize the secondary nodes
        for (ECMessage msg : messages) {
            if (!msg.ecMessageContent.replicaNodes.isEmpty()) {
                for (int i = 1; i < msg.ecMessageContent.replicaNodes.size(); i++) {
                    this.ecMetadataContent.secondaryNodes.add(msg.ecMessageContent.replicaNodes.get(i));
                }
            }
            // for(InetAddressAndPort pns : msg.ecMessageContent.replicaNodes) {
            // if(!this.ecMetadataContent.primaryNodes.contains(pns))
            // this.ecMetadataContent.secondaryNodes.add(pns);
            // }
        }

        try {
            this.ecMetadataContentBytes = ByteObjectConversion.objectToByteArray((Serializable) this.ecMetadataContent);
            this.ecMetadataContentBytesSize = this.ecMetadataContentBytes.length;
            if (this.ecMetadataContentBytes.length == 0) {
                logger.error("ELECT-ERROR: no metadata content");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // dispatch to related nodes
        distributeECMetadata(this);

        // store ecMetadata locally
        StorageService.instance.globalStripIdToECMetadataMap.put(this.ecMetadataContent.stripeId,
                this.ecMetadataContent);

        mapSSTHashToStripIdAndSelectOldSSTableFromWaitingListIfNeeded(this.ecMetadataContent.sstHashIdList,
                this.ecMetadataContent.stripeId, this.ecMetadataContent.sstHashIdToReplicaMap);

    }

    /**
     * This method is to update the metadata for the given messages
     * 
     * @param parityHashes
     */
    public synchronized void updateAndDistributeMetadata(List<String> newParityHashes, boolean isParityUpdate,
            String oldSSTHash, String newSSTHash, int targetIndex,
            List<InetAddressAndPort> oldReplicaNodes, List<InetAddressAndPort> newReplicaNodes) {
        // update isParityUpdate
        this.ecMetadataContent.isParityUpdate = isParityUpdate;
        // update the old sstable hash
        this.ecMetadataContent.oldSSTHashForUpdate = oldSSTHash;
        // update sstable hash list
        if (this.ecMetadataContent.sstHashIdList.get(targetIndex).equals(oldSSTHash)) {
            this.ecMetadataContent.sstHashIdList.set(targetIndex, newSSTHash);
        } else {
            throw new IllegalStateException(String.format(
                    "ELECT-ERROR: Unexpect state, the target index is (%s), but the truely needed index is (%s)",
                    targetIndex,
                    this.ecMetadataContent.sstHashIdList.indexOf(oldSSTHash)));
        }

        // update parity code hash
        this.ecMetadataContent.parityHashList = newParityHashes;
        // modify sstHashIdToReplicaMap
        this.ecMetadataContent.sstHashIdToReplicaMap.put(newSSTHash, newReplicaNodes);
        this.ecMetadataContent.sstHashIdToReplicaMap.remove(oldSSTHash);
        // update the target index
        this.ecMetadataContent.targetIndex = targetIndex;

        if (!oldReplicaNodes.equals(newReplicaNodes)) {
            logger.warn("ELECT-Debug: new replication nodes {} are different from old replication nodes {}",
                    newReplicaNodes, oldReplicaNodes);
            // update primary node list
            this.ecMetadataContent.primaryNodes.set(targetIndex, newReplicaNodes.get(0));

            // update secondary node list
            this.ecMetadataContent.secondaryNodes = new HashSet<InetAddressAndPort>();
            // update secondary nodes
            for (Map.Entry<String, List<InetAddressAndPort>> entry : this.ecMetadataContent.sstHashIdToReplicaMap
                    .entrySet()) {
                for (int i = 1; i < entry.getValue().size(); i++) {
                    this.ecMetadataContent.secondaryNodes.add(entry.getValue().get(i));
                }
            }

        }

        // remove the ECMetadata from memory
        // StorageService.instance.globalStripIdToECMetadataMap.remove(this.stripeId);

        // update strip id
        String connectedSSTHash = "";
        for (String sstHash : this.ecMetadataContent.sstHashIdList) {
            connectedSSTHash += sstHash;
        }
        String oldStripId = this.ecMetadataContent.stripeId;
        this.ecMetadataContent.stripeId = ECNetutils.stringToHex(String.valueOf(connectedSSTHash.hashCode()));
        logger.debug("ELECT-Debug: Update old strip id ({}) with a new one ({})", oldStripId,
                this.ecMetadataContent.stripeId);

        logger.debug(
                "ELECT-Debug: this update ECMetadata method, we update old sstable ({}) with new sstable ({}) for strip id ({})",
                oldSSTHash, newSSTHash, this.ecMetadataContent.stripeId);

        try {
            this.ecMetadataContentBytes = ByteObjectConversion.objectToByteArray((Serializable) this.ecMetadataContent);
            this.ecMetadataContentBytesSize = this.ecMetadataContentBytes.length;
            if (this.ecMetadataContentBytes.length == 0) {
                logger.error("ELECT-ERROR: no metadata content");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // dispatch to related nodes
        distributeECMetadata(this);

        mapSSTHashToStripIdAndSelectOldSSTableFromWaitingListIfNeeded(this.ecMetadataContent.sstHashIdList,
                this.ecMetadataContent.stripeId, this.ecMetadataContent.sstHashIdToReplicaMap);
        // StorageService.instance.globalSSTHashToStripIDMap.remove(oldSSTHash);

        // store ecMetadata locally
        StorageService.instance.globalStripIdToECMetadataMap.put(this.ecMetadataContent.stripeId,
                this.ecMetadataContent);
        StorageService.instance.globalStripIdToECMetadataMap.remove(oldStripId);

        // parity update is finished, we update the globalUpdatingStripList
        StorageService.instance.globalUpdatingStripList.remove(oldStripId);
        logger.debug("ELECT-Debug: We replaced the oldStrip {} with the newStrip {} successfully.", oldStripId,
                this.ecMetadataContent.stripeId);
        // StorageService.instance.globalUpdatingStripList.compute(this.stripeId, (key,
        // oldValue) -> oldValue - 1);

    }

    /**
     * This method is called when a new EC Strip generated, we will do the following
     * operations:
     * 1. Traverse the sstHashList of the given EC Strip, and map each of them to
     * strip id.
     * 2. Check if there is any sstHash in the waiting list:
     * globalPendingOldSSTableForECStripUpdateMap,
     * if yes, we can only select one pending update signal and mark current strip
     * id in the updating list.
     */
    private synchronized void mapSSTHashToStripIdAndSelectOldSSTableFromWaitingListIfNeeded(List<String> newSSTHashList,
            String newStripId, Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap) {

        boolean isSelectedOneOldSSTable = false;
        for (String sstHash : newSSTHashList) {
            StorageService.instance.globalSSTHashToStripIDMap.put(sstHash, newStripId);
            logger.debug("ELECT-Debug:[ErasureCoding] In node {}, we map sstHash {} to stripID {}",
                    FBUtilities.getBroadcastAddressAndPort(),
                    sstHash,
                    newStripId);

            if (isSelectedOneOldSSTable) {
                continue;
            }

            // we move this sstable to the globalReadyOldSSTableForECStripUpdateMap
            if (StorageService.instance.globalPendingOldSSTableForECStripUpdateMap.containsKey(sstHash)) {
                isSelectedOneOldSSTable = true;
            } else {
                continue;
            }

            StorageService.instance.globalUpdatingStripList.add(newStripId);
            InetAddressAndPort primaryNode = sstHashIdToReplicaMap.get(sstHash).get(0);
            SSTableContentWithHashID oldSSTableForParityUpdate = StorageService.instance.globalPendingOldSSTableForECStripUpdateMap
                    .get(sstHash);
            logger.debug("ELECT-Debug: We move the old sstable {} from pending list to ready list to update strip {}",
                    sstHash, newStripId);

            ECNetutils.addOldSSTableForECStripeUpdateToReadyList(primaryNode, oldSSTableForParityUpdate);

            // StorageService.instance.globalReadyOldSSTableForECStripUpdateMap.get(primaryNode).add(oldSSTableForParityUpdate);
            StorageService.instance.globalPendingOldSSTableForECStripUpdateMap.remove(sstHash);
        }

    }

    /**
     * [In parity] Distribute ecMetadata to secondary nodes
     */
    public void distributeECMetadata(ECMetadata ecMetadata) {
        logger.debug(
                "ELECT-Debug: [In parity node ({})] This distributeEcMetadata method, we should send stripId ({}) with sstables list ({}) to node ({}), the sstHashToRelicaMap is ({}), old sstable hash is ({}). We can check that the primary nodes are ({}), parity nodes are ({})",
                FBUtilities.getBroadcastAddressAndPort(), ecMetadata.ecMetadataContent.stripeId,
                ecMetadata.ecMetadataContent.sstHashIdList, ecMetadata.ecMetadataContent.secondaryNodes,
                ecMetadata.ecMetadataContent.sstHashIdToReplicaMap, ecMetadata.ecMetadataContent.oldSSTHashForUpdate,
                ecMetadata.ecMetadataContent.primaryNodes, ecMetadata.ecMetadataContent.parityNodes);
        Message<ECMetadata> message = Message.outWithFlag(Verb.ECMETADATA_REQ, ecMetadata,
                MessageFlag.CALL_BACK_ON_FAILURE);

        // send to secondary nodes
        int rf = 3;
        logger.debug("ELECT-Debug: For strip id ({}), we should record ecSSTable ({}) times in total",
                ecMetadata.ecMetadataContent.stripeId, DatabaseDescriptor.getEcDataNodes() * (rf - 1));
        for (InetAddressAndPort node : ecMetadata.ecMetadataContent.secondaryNodes) {
            MessagingService.instance().send(message, node);
        }

        // send to remote parity nodes
        // for (InetAddressAndPort node : ecMetadata.ecMetadataContent.parityNodes) {
        // if(!node.equals(FBUtilities.getBroadcastAddressAndPort())) {
        // MessagingService.instance().send(message, node);
        // }
        // }

        logger.debug("ELECT-Debug: store stripID {} in node {}", ecMetadata.ecMetadataContent.stripeId,
                FBUtilities.getBroadcastAddressAndPort());
    }

    public static final class Serializer implements IVersionedSerializer<ECMetadata> {

        @Override
        public void serialize(ECMetadata t, DataOutputPlus out, int version) throws IOException {

            // out.writeUTF(t.ecMetadataContent.stripeId);
            out.writeInt(t.ecMetadataContentBytesSize);
            out.write(t.ecMetadataContentBytes);
        }

        @Override
        public ECMetadata deserialize(DataInputPlus in, int version) throws IOException {
            // TODO: Correct data types, and revise the Constructor
            // String stripeId = in.readUTF();
            int ecMetadataContentBytesSize = in.readInt();
            byte[] ecMetadataContentBytes = new byte[ecMetadataContentBytesSize];
            in.readFully(ecMetadataContentBytes);

            try {
                ECMetadataContent eMetadataContent = (ECMetadataContent) ByteObjectConversion
                        .byteArrayToObject(ecMetadataContentBytes);
                return new ECMetadata(eMetadataContent);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                logger.error("ERROR: get sstables in bytes error!");
            }
            return null;
        }

        @Override
        public long serializedSize(ECMetadata t, int version) {
            long size = // sizeof(t.stripeId) +
                    sizeof(t.ecMetadataContentBytesSize) +
                            t.ecMetadataContentBytesSize;
            return size;
        }

    }

    // public static byte[] readBytesFromFile(String fileName) throws IOException
    // {
    // // String fileName = descriptor.filenameFor(Component.DATA);
    // File file = new File(fileName);
    // long fileLength = file.length();
    // FileInputStream fileStream = new FileInputStream(fileName);
    // byte[] buffer = new byte[(int)fileLength];
    // int offset = 0;
    // int numRead = 0;
    // while (offset < buffer.length && (numRead = fileStream.read(buffer, offset,
    // buffer.length - offset)) >= 0) {
    // offset += numRead;
    // }
    // if (offset != buffer.length) {
    // throw new IOException(String.format("Could not read %s, only read %d bytes",
    // fileName, offset));
    // }
    // fileStream.close();
    // logger.debug("ELECT-Debug: read file {} successfully!", fileName);
    // return buffer;
    // // return ByteBuffer.wrap(buffer);
    // }

    // public static Object byteArrayToObject(byte[] bytes) throws Exception {
    // ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    // ObjectInputStream ois = new ObjectInputStream(bis);
    // Object obj = ois.readObject();
    // bis.close();
    // ois.close();
    // return obj;
    // }
    public static void main(String[] args) throws Exception {
        String ecMetadataFile = "./nb-627-big-EC.db";
        byte[] ecMetadataInBytes = ECNetutils.readBytesFromFile(ecMetadataFile);
        ECMetadataContent ecMetadata = (ECMetadataContent) ByteObjectConversion.byteArrayToObject(ecMetadataInBytes);
        logger.debug("ELECT-Debug: [Debug recovery] read ecmetadata ({}) for old sstable ({})", ecMetadata.stripeId);

    }

}
