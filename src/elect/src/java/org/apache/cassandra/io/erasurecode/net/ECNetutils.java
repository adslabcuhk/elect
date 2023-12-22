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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.StorageHook;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index.Indexer;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate.SSTableContentWithHashID;
import org.apache.cassandra.io.erasurecode.net.ECSyncSSTable.SSTablesInBytes;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.ext.italianStemmer;
import org.tartarus.snowball.ext.porterStemmer;

public final class ECNetutils {
    private static final Logger logger = LoggerFactory.getLogger(ECNetutils.class);

    private static final String dataForRewriteDir = System.getProperty("user.dir") + "/data/tmp/";
    private static final String receivedParityCodeDir = System.getProperty("user.dir") + "/data/receivedParityHashes/";
    private static final String dataDir = System.getProperty("user.dir") + "/data/data/";
    private static final String localParityCodeDir = System.getProperty("user.dir") + "/data/localParityHashes/";
    private static final String scriptsDir = System.getProperty("user.dir") + "/scripts/";
    private static final String inMemoryDataBackupDir = System.getProperty("user.dir") + "/data/inMemoryData/";
    private static final String fullNodeRecoveryLogs = System.getProperty("user.dir") + "/logs/recovery.log";
    private static final int MIGRATION_RETRY_COUNT = 1000;

    public static class ByteObjectConversion {
        public static byte[] objectToByteArray(Serializable obj) throws IOException {
            logger.debug("ELECT-Debug: start to transform");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
            bos.close();
            return bos.toByteArray();
        }

        public static Object byteArrayToObject(byte[] bytes) throws Exception {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object obj = ois.readObject();
            bis.close();
            ois.close();
            return obj;
        }
    }

    public static class StripIDToSSTHashAndParityNodes implements Serializable {
        public final String stripID;
        public final String sstHash;
        public final List<InetAddressAndPort> parityNodes;

        public StripIDToSSTHashAndParityNodes(String stripID, String sstHash, List<InetAddressAndPort> parityNodes) {
            this.stripID = stripID;
            this.sstHash = sstHash;
            this.parityNodes = parityNodes;
        }
    }

    public static class SSTablesInBytesConverter {

        public static byte[] toByteArray(SSTablesInBytes sstables) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(sstables);
            oos.flush();
            return baos.toByteArray();
        }

        public static SSTablesInBytes fromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (SSTablesInBytes) ois.readObject();
        }
    }

    public static class DecoratedKeyComparator implements Comparator<DecoratedKey> {
        public int compare(DecoratedKey o1, DecoratedKey o2) {
            return o2.compareTo(o1);
        }
    };

    public static String getDataForRewriteDir() {

        File dataForRewriteFolder = new File(dataForRewriteDir);
        if (!dataForRewriteFolder.createDirectoriesIfNotExists()) {
            logger.error("ELECT-ERROR: failed to create file dir ({})", dataForRewriteDir);
        }
        return dataForRewriteDir;
    }

    public static String getFullNodeRecoveryLogFile() {
        return fullNodeRecoveryLogs;
    }

    public static String getReceivedParityCodeDir() {
        File receivedParityCodeFolder = new File(receivedParityCodeDir);
        if (!receivedParityCodeFolder.createDirectoriesIfNotExists()) {
            logger.error("ELECT-ERROR: failed to create file dir ({})", receivedParityCodeDir);
        }
        return receivedParityCodeDir;
    }

    public static String getDataDir() {
        return dataDir;
    }

    public static String getLocalParityCodeDir() {
        File localParityCodeFolder = new File(localParityCodeDir);
        if (!localParityCodeFolder.createDirectoriesIfNotExists()) {
            logger.error("ELECT-ERROR: failed to create file dir ({})", localParityCodeDir);
        }
        return localParityCodeDir;
    }

    public static int getMigrationRetryCount() {
        return MIGRATION_RETRY_COUNT;
    }

    public static void recordResults(String fileName, String data) {
        try {
            FileWriter fileWriter = new FileWriter(fileName, true); 
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter); 

            bufferedWriter.write(data); 
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method is to sync a given sstable's file (without Data.db) with
     * secondary nodes during erasure coding and parity update.
     * 
     * @param sstable
     * @param replicaNodes
     * @param sstHashID
     * @return
     * @throws Exception
     */
    public static void syncSSTableWithSecondaryNodes(SSTableReader sstable,
            List<InetAddressAndPort> replicaNodes,
            String sstHashID, String operationType,
            ColumnFamilyStore cfs) throws Exception {

        // Read a given sstable's Filter.db, Index.db, Statistics.db and Summary.db
        byte[] filterFile = readBytesFromFile(sstable.descriptor.filenameFor(Component.FILTER));
        byte[] indexFile = readBytesFromFile(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX));
        byte[] statsFile = readBytesFromFile(sstable.descriptor.filenameFor(Component.STATS));
        byte[] summaryFile = readBytesFromFile(sstable.descriptor.filenameFor(Component.SUMMARY));

        SSTablesInBytes sstInBytes = new SSTablesInBytes(filterFile, indexFile, statsFile, summaryFile);
        List<String> allKeys = new ArrayList<>(sstable.getAllKeys());
        String firstKey = sstable.first.getRawKey(cfs.metadata());
        String lastKey = sstable.last.getRawKey(cfs.metadata());
        InetAddressAndPort locaIP = FBUtilities.getBroadcastAddressAndPort();

        logger.debug("ELECT-Debug: send sstable to secondary nodes, hash id is ({}), descriptor is ({})",
                sstable.getSSTableHashID(), sstable.descriptor);

        for (InetAddressAndPort rpn : replicaNodes) {
            if (!rpn.equals(locaIP)) {
                String targetCfName = "usertable" + replicaNodes.indexOf(rpn);
                ECSyncSSTable ecSync = new ECSyncSSTable(sstHashID, targetCfName, firstKey, lastKey, sstInBytes,
                        allKeys);
                ecSync.sendSSTableToSecondary(rpn);
            }
        }

        if (!allKeys.get(0).equals(firstKey) || !allKeys.get(allKeys.size() - 1).equals(lastKey)) {
            logger.debug(
                    "ELECT-ERROR: keys are different, first key is {}, last key is {}, first entry of allKeys {}, last key {}",
                    firstKey, lastKey, allKeys.get(0), allKeys.get(allKeys.size() - 1));
        }

        logger.debug(
                "ELECT-Debug: [{}] send sstables ({}), replicaNodes are {}, row num is {}, all key num is {}, first key is {}, last key is {}, first entry of allKeys {}, last key {}",
                operationType,
                sstHashID,
                replicaNodes, sstable.getTotalRows(), allKeys.size(),
                firstKey, lastKey, allKeys.get(0), allKeys.get(allKeys.size() - 1));

    }

    public static byte[] readBytesFromFile(String fileName) throws IOException {
        // String fileName = descriptor.filenameFor(Component.DATA);

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

        byte[] byteArray = Files.readAllBytes(Paths.get(fileName));
        return byteArray;

        // return ByteBuffer.wrap(buffer);
    }

    public static void writeBytesToFile(String fileName, byte[] buffer) throws IOException {

        Files.write(Paths.get(fileName), buffer);
        // try (FileOutputStream outputStream = new FileOutputStream(fileName)) {
        // outputStream.write(buffer);
        // } catch (Exception e) {
        // logger.error("ELECT-ERROR: failed to write bytes to file, {}", e);
        // }
    }

    public static Optional<Path> findDirectoryByPrefix(Path parentDirectory, String prefix) throws IOException {
        return Files.list(parentDirectory)
                .filter(path -> Files.isDirectory(path))
                .filter(path -> path.getFileName().toString().startsWith(prefix))
                .findFirst();
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

    public static void deleteFileByName(String fileName) {
        Path path = Paths.get(fileName);
        try {
            Files.delete(path);
            logger.debug("ELECT-Debug: delete file {} successfully", fileName);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void printStatusCode(int statusCode, String cfName) {
        switch (statusCode) {
            case 1:
                logger.debug(
                        "Aborted rewrite sstables for at least one table in cfs {}, check server logs for more information.",
                        cfName);
                break;
            case 2:
                logger.error(
                        "Failed marking some sstables compacting in cfs {}, check server logs for more information.",
                        cfName);
        }
    }

    public static class SSTableReaderComparator implements Comparator<SSTableReader> {

        @Override
        public int compare(SSTableReader o1, SSTableReader o2) {
            return o1.first.getToken().compareTo(o2.first.getToken());
        }

    }

    public static class SSTableAccessFrequencyComparator implements Comparator<SSTableReader> {

        @Override
        public int compare(SSTableReader o1, SSTableReader o2) {
            return Double.compare(o1.getReadMeter().twoHourRate(), o1.getReadMeter().twoHourRate());
        }

    }

    public static class InetAddressAndPortComparator implements Comparator<InetAddressAndPort> {
        @Override
        public int compare(InetAddressAndPort addr1, InetAddressAndPort addr2) {
            InetAddress ip1 = addr1.getAddress();
            InetAddress ip2 = addr2.getAddress();
            return ip1.getHostAddress().compareTo(ip2.getHostAddress());
        }
    }

    public synchronized static void addOldSSTableForECStripeUpdateToReadyList(InetAddressAndPort primaryNode,
            SSTableContentWithHashID oldSSTable) {
        if (StorageService.instance.globalReadyOldSSTableForECStripUpdateMap.containsKey(primaryNode)) {
            StorageService.instance.globalReadyOldSSTableForECStripUpdateMap.get(primaryNode).add(oldSSTable);
        } else {
            StorageService.instance.globalReadyOldSSTableForECStripUpdateMap.put(primaryNode,
                    new ConcurrentLinkedQueue<SSTableContentWithHashID>(
                            Collections.singleton(oldSSTable)));
        }
        StorageService.instance.globalReadyOldSSTableForECStripUpdateCount++;
        logger.debug("ELECT-Debug: add old sstable ({}) to ready list for primary node ({})", oldSSTable.sstHash,
                primaryNode);
    }

    public synchronized static void addNewSSTableForECStripeUpdateToReadyList(InetAddressAndPort primaryNode,
            SSTableContentWithHashID newSSTable) {
        if (StorageService.instance.globalReadyNewSSTableForECStripUpdateMap.containsKey(primaryNode)) {
            StorageService.instance.globalReadyNewSSTableForECStripUpdateMap.get(primaryNode)
                    .add(newSSTable);
        } else {
            StorageService.instance.globalReadyNewSSTableForECStripUpdateMap.put(primaryNode,
                    new ConcurrentLinkedQueue<SSTableContentWithHashID>(
                            Collections.singleton(newSSTable)));
        }
        logger.debug("ELECT-Debug: add new sstable ({}) to ready list for primary node ({})", newSSTable.sstHash,
                primaryNode);
    }

    public synchronized static ECMessage getDataBlockFromGlobalRecvQueue(InetAddressAndPort addr) {
        StorageService.instance.totalConsumedECMessages++;
        ECMessage message = StorageService.instance.globalRecvQueues.get(addr).poll();
        if (StorageService.instance.globalRecvQueues.get(addr).size() == 0) {
            StorageService.instance.globalRecvQueues.remove(addr);
        }
        return message;
    }

    public synchronized static void saveECMessageToGlobalRecvQueue(InetAddressAndPort primaryNode, ECMessage message) {
        StorageService.instance.totalReceivedECMessages++;
        if (!StorageService.instance.globalRecvQueues.containsKey(primaryNode)) {
            ConcurrentLinkedQueue<ECMessage> recvQueue = new ConcurrentLinkedQueue<ECMessage>();
            recvQueue.add(message);
            StorageService.instance.globalRecvQueues.put(primaryNode, recvQueue);
        } else {
            StorageService.instance.globalRecvQueues.get(primaryNode).add(message);
        }
    }

    public synchronized static boolean isSSTableCompactingOrErasureCoding(String sstableHash) {

        if (StorageService.instance.compactingOrErasureCodingSSTables.contains(sstableHash)) {
            return true;
        } else {
            StorageService.instance.compactingOrErasureCodingSSTables.add(sstableHash);
            return false;
        }

    }

    public synchronized static void unsetIsSelectedByCompactionOrErasureCodingSSTables(String sstableHash) {
        if (StorageService.instance.compactingOrErasureCodingSSTables.contains(sstableHash)) {
            StorageService.instance.compactingOrErasureCodingSSTables.remove(sstableHash);
        }
        // else {
        // logger.error("ELECT-ERROR: we can not find the specified stable hash ({})",
        // sstableHash);
        // }
    }

    public static void printStackTace(String msg) {
        // logger.debug(msg);
        // Throwable throwable =new Throwable();
        // throwable.printStackTrace();
        logger.debug("stack trace {}", new Exception(msg));
    }

    public static void throwError(String msg) throws InterruptedException {

        throw new InterruptedException(msg);
    }

    public static void test() throws Exception {
        printStackTace("test print stack trace method");

        System.out.println("ok");
    }

    public synchronized static void setIsRecovered(String sstHash) {
        if (!StorageService.instance.recoveredSSTables.contains(sstHash)) {
            StorageService.instance.recoveredSSTables.add(sstHash);
        }
    }

    public synchronized static boolean getIsRecovered(String sstHash) {
        if (sstHash == null)
            return false;
        return StorageService.instance.recoveredSSTables.contains(sstHash);
    }

    public synchronized static boolean getIsMigratedToCloud(String sstHash) {
        if (sstHash == null)
            return false;
        return StorageService.instance.migratedSStables.contains(sstHash);
    }

    public synchronized static boolean getIsDownloaded(String sstHash) {
        if (sstHash == null) {
            logger.error("[ELECT-ERROR] check sstable is downloaded state error, sstable hash is null");
            return false;
        } else {
            return StorageService.instance.globalDownloadedSSTableMap.containsKey(sstHash);
        }
    }

    public static void checkTheReplicaPlanIsEqualsToNaturalEndpoint(ReplicaPlan.ForWrite replicaPlan,
            List<InetAddressAndPort> sendRequestAddresses, Token targetReadToken) {
        boolean isReplicaPlanMatchToNaturalEndpointFlag = true;
        for (int i = 0; i < replicaPlan.contacts().endpointList().size(); i++) {
            if (!replicaPlan.contacts().endpointList().get(i).equals(sendRequestAddresses.get(i))) {
                isReplicaPlanMatchToNaturalEndpointFlag = false;
            }
        }
        if (isReplicaPlanMatchToNaturalEndpointFlag == false) {
            throw new IllegalStateException(String.format(
                    "ELECT-ERROR: for key token = {}, the primary node is not the first node in the natural storage node list. The replication plan for read is {}, natural storage node list = {}",
                    targetReadToken,
                    replicaPlan.contacts().endpointList(),
                    sendRequestAddresses));
        }
    }

    public final static EnumSet<TCPService> TCPServices = EnumSet.allOf(TCPService.class);

    public enum TCPService {
        MIGRATION("migration"),
        RECOVERYLSMTREE("recoveryLSMTree");

        final String serviceName;

        TCPService(String serviceName) {
            this.serviceName = serviceName;
        }

        public static TCPService fromRepresentation(String serviceName) {
            for (TCPService svc : TCPServices) {
                if (svc.serviceName != null && Pattern.matches(svc.serviceName, serviceName))
                    return svc;
            }
            return null;
        }
    }

    public static String getScriptsDir() {
        return scriptsDir;
    }

    public static String getInMemoryDataDir() {

        File inMemoryDataBackupDirFolder = new File(inMemoryDataBackupDir);
        if (!inMemoryDataBackupDirFolder.createDirectoriesIfNotExists()) {
            logger.error("ELECT-ERROR: failed to create file dir ({})", inMemoryDataBackupDir);
        }
        return inMemoryDataBackupDir;
    }

    public static void startTCPServer(int port) {
        try {

            ServerSocket serverSocket = new ServerSocket(port);
            // System.out.println("服务器已启动，等待客户端连接...");

            while (true) {

                Socket clientSocket = serverSocket.accept();
                Thread clientThread = new Thread(new TCPClientHandler(clientSocket));
                clientThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class TCPClientHandler implements Runnable {

        private Socket clientSocket;

        public TCPClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {

            BufferedReader in;
            try {
                in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String svcName = in.readLine();

                TCPService svc = TCPService.fromRepresentation(svcName);

                // TODO finish the tcp services
                if (svc.equals(TCPService.MIGRATION)) {

                } else if (svc.equals(TCPService.RECOVERYLSMTREE)) {

                }

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public static void scpCommand(String source, String target) throws IOException {

        String script = "rsync -avz --progress -r " + source + " " + target;
        ProcessBuilder processBuilder = new ProcessBuilder(script.split(" "));
        Process process = processBuilder.start();

        try {
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                logger.debug("ELECT-Debug: Performing rsync script successfully!");

            } else {
                logger.debug("ELECT-Debug: Failed to perform rsync script!");
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void migrateDataToCloud(String cloudIp, String currentIp, String cfName, String dataFileName)
            throws IOException {

        // String userName = "yjren";
        String targetDir = "yjren@" + cloudIp + ":~/" + currentIp + "/" + cfName;
        scpCommand(dataFileName, targetDir);

    }

    public static void retrieveDataFromCloud(String cloudIp, String requestIp, String cfName, String dataFileName,
            String targetDir) throws IOException {
        String sourceFileName = "yjren@" + cloudIp + ":~/" + requestIp + "/" + cfName + "/" + dataFileName;
        scpCommand(sourceFileName, targetDir);
    }

    public static int getNeedMigrateParityCodesCount() {

        if (DatabaseDescriptor.getStorageSavingGrade() == 0) {
            ColumnFamilyStore cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable0");

            int rf = Keyspace.open("ycsb").getAllReplicationFactor();
            int k = DatabaseDescriptor.getEcDataNodes();
            int n = k + DatabaseDescriptor.getParityNodes();
            int totalSSTableCount = cfs.getTracker().getView().liveSSTables().size();
            double tss = DatabaseDescriptor.getTargetStorageSaving();

            return (int) (rf * totalSSTableCount * tss
                    - (rf - n * 1.0 / k) * cfs.getSSTableForLevel(DatabaseDescriptor.getMaxLevelCount() - 1).size());
        } else if (DatabaseDescriptor.getStorageSavingGrade() == 1) {
            return 0;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public static synchronized boolean checkIsParityCodeMigrated(String parityCodeHash) {
        return StorageService.instance.migratedParityCodes.contains(parityCodeHash);
    }

    public static void main(String[] args) {

        // ByteBuffer erasureCodes = ByteBuffer.allocateDirect(100);
        // byte[] data = new byte[10];
        // erasureCodes.put(data);
        // logger.debug("ELECT-Debug: remaining is ({}), position is ({})",
        // erasureCodes.remaining(), erasureCodes.position());

        int rf = 3;
        int totalSSTableCount = 479;
        double tss = 0.5;
        int n = 6;
        int k = 4;
        int needTransferSSTablesCount = (int) (rf * totalSSTableCount * tss * 1.0 / (rf - n * 1.0 / k)); // parameter a
        logger.debug("ELECT-Debug: need transfer sstable count is ({})", needTransferSSTablesCount);

        // try {
        // test();
        // } catch (Exception e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        // try {
        // throwError("test throw interrupted error method");
        // } catch (InterruptedException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        // List<String> tt = new ArrayList<String>();
        // tt.add("11");
        // tt.add("22");
        // logger.debug("{}", tt);

        // System.out.println("ok");

        // String ss = "usertbale";
        // boolean a = false;
        // boolean b = false;
        // System.out.println(a&&b);

        // try {
        // String file = "./test1";
        // FileOutputStream fos = new FileOutputStream(file,true ) ;
        // String str = "Data.db\n";
        // fos.write(str.getBytes()) ;
        // fos.close ();
        // } catch (IOException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        // boolean assertTest = false;
        // assert assertTest;
        // int p1 = 0x01;
        // int p2 = 0x02;
        // int flags = 54;
        // logger.debug("ELECT-Debug: p1&p1 is ({}), p2 & p2 is ({}), p1 & p2 is ({}), p1 &
        // flags is ({}), p2 & flags is ({})", p1 & p1, p2 & p2, p1 & p2, p1 & flags, p2
        // & flags);
        // 110110
    }
}
