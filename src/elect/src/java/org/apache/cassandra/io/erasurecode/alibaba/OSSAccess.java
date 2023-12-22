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

package org.apache.cassandra.io.erasurecode.alibaba;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import javax.xml.crypto.Data;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.net.ECNetutils;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import com.aliyun.oss.ClientBuilderConfiguration;
// import com.aliyun.oss.ClientException;
// import com.aliyun.oss.OSS;
// import com.aliyun.oss.common.auth.*;
// import com.aliyun.oss.common.comm.Protocol;
// import com.aliyun.oss.OSSClientBuilder;
// import com.aliyun.oss.OSSException;
// import com.aliyun.oss.model.PutObjectRequest;
// import com.aliyun.oss.model.PutObjectResult;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.aliyun.oss.model.BucketInfo;
// import com.aliyun.oss.model.DeleteObjectsRequest;
// import com.aliyun.oss.model.DeleteObjectsResult;
// import com.aliyun.oss.model.GetObjectRequest;
// import com.aliyun.oss.model.OSSObject;
// import com.aliyun.oss.model.OSSObjectSummary;
// import com.aliyun.oss.model.ObjectListing;
// import com.aliyun.oss.model.ObjectMetadata;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URLDecoder;

// The following is the code for using Alibaba OSS as the cold tier.
// public class OSSAccess implements AutoCloseable {
//     private static final Logger logger = LoggerFactory.getLogger(OSSAccess.class);

//     private static String endpoint = "http://oss-cn-chengdu.aliyuncs.com";
//     private static String bucketName = "elect-pdm";
//     private static String localIP = FBUtilities.getBroadcastAddressAndPort().toString(false).replace('/', '_');
//     private static OSS ossClient;
//     private final int maxConcurrentDownloads = DatabaseDescriptor.getMaxConcurrentDownload();
//     private final Semaphore semaphore = new Semaphore(maxConcurrentDownloads);

//     public OSSAccess() {
//         EnvironmentVariableCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
//         ClientBuilderConfiguration conf = new ClientBuilderConfiguration();

//         if(DatabaseDescriptor.getEnableProxy()) {            
//             conf.setProxyHost("proxy.cse.cuhk.edu.hk");
//             conf.setProxyPort(8000);
//         }

//         conf.setMaxConnections(200);
//         conf.setSocketTimeout(10000);
//         conf.setConnectionTimeout(10000);
//         conf.setMaxErrorRetry(5);
//         conf.setProtocol(Protocol.HTTP);
//         conf.setUserAgent("aliyun-sdk-java");
//         OSSAccess.ossClient = new OSSClientBuilder().build(endpoint, credentialsProvider, conf);
//         checkBucketStatusAndCreateBucketIfNotExist();
//     }

//     public void close() throws Exception {
//         ossClient.shutdown();
//     }

//     public boolean uploadFileToOSS(String targetFilePath) {
//         String objectName = targetFilePath.replace('/', '_') + localIP;
//         logger.debug("ELECT-Debug: target object name is ({})", objectName);
//         try {
//             InputStream inputStream = new FileInputStream(targetFilePath);
//             // 创建PutObjectRequest对象。
//             PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, inputStream);
//             // 创建PutObject请求。
//             long startTime = System.currentTimeMillis();
//             PutObjectResult result = ossClient.putObject(putObjectRequest);
//             long timeCost = System.currentTimeMillis() - startTime;
//             StorageService.instance.migratedRawSSTableTimeSendCost += timeCost;
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId() + "\nRequest object key:" + objectName);
//             return false;
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//             return false;
//         } catch (Throwable e) {
//             logger.error("Get file input stream error:");
//             e.printStackTrace();
//             return false;
//         }
//         return true;
//     }

//     public boolean uploadFileToOSS(String targetFilePath, byte[] content) {
//         String objectName = targetFilePath.replace('/', '_') + localIP;

//         try {
//             long startUploadTime = System.currentTimeMillis();
//             logger.debug("ELECT-Debug: target file path is ({}), content size is ({}), object name is ({})",
//                     targetFilePath, content.length, objectName);
//             PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName,
//                     new ByteArrayInputStream(content));
//             PutObjectResult result = ossClient.putObject(putObjectRequest);
//             long timeCost = System.currentTimeMillis() - startUploadTime;
//             logger.debug("ELECT-Debug: upload file to OSS cost time is ({}ms), the file size is ({}), file name is ({})", timeCost, content.length, targetFilePath);
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId() + "\nRequest object key:" + objectName);
//             return false;
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//             return false;
//         }
//         return true;
//     }

//     public boolean uploadFileToOSS(String targetFilePath, String content) {
//         String objectName = targetFilePath.replace('/', '_') + localIP;
//         try {
//             PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName,
//                     new ByteArrayInputStream(content.getBytes()));
//             PutObjectResult result = ossClient.putObject(putObjectRequest);
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId() + "\nRequest object key:" + objectName);
//             return false;
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//             return false;
//         }
//         return true;
//     }

//     public boolean downloadFileFromOSS(String originalFilePath, String targetStorePath) {
//         String objectName = originalFilePath.replace('/', '_') + localIP;
//         try {
//             ossClient.getObject(
//                     new GetObjectRequest(bucketName, objectName),
//                     new File(targetStorePath));
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId() + "\nRequest object key:" + objectName);
//             return false;
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//             return false;
//         }
//         return true;
//     }

//     public boolean downloadFileAsByteArrayFromOSS(String originalFilePath, String targetIp) {
//         String objectName = originalFilePath.replace('/', '_') + "_" + targetIp;
//         try {
//             semaphore.acquire();
//             try {
//                 // FileUtils.delete(originalFilePath);
//                 // Varify the file exist or not
//                 ossClient.getObject(
//                         new GetObjectRequest(bucketName, objectName),
//                         new File(originalFilePath));
//             } catch (OSSException oe) {
//                 logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                         + "\nRequest ID:" + oe.getRequestId() + "\nRequest object key:" + objectName);
//                 logger.error("[ELECT-ERROR]: Download original file from OSS failed, file name is ({})",
//                         objectName);
//                 return false;
//             } catch (ClientException ce) {
//                 logger.error("OSS Internet Error Message:" + ce.getMessage());
//                 logger.error("[ELECT-ERROR]: Download original file from OSS failed, file name is ({})",
//                         objectName);
//                 return false;
//             }
//         } catch (InterruptedException e) {
//             Thread.currentThread().interrupt();
//             logger.error("[ELECT-ERROR]: Download original file from OSS failed, file name is ({})", objectName);
//             return false;
//         } finally {
//             semaphore.release();
//         }
//         logger.debug("ELECT-Debug: Downloaded original file from OSS, file name is ({})", objectName);
//         return true;
//     }

//     public boolean deleteSingleFileInOSS(String targetFilePath) {
//         String objectName = targetFilePath.replace('/', '_') + localIP;
//         try {
//             boolean found = ossClient.doesObjectExist(bucketName, objectName);
//             if (found) {
//                 logger.debug("Found target file " + targetFilePath + " in bucket, delete it now");
//                 ossClient.deleteObject(bucketName,
//                         targetFilePath.replace('/', '_') + localIP);
//                 return true;
//             } else {
//                 logger.error("Could not found target file " + targetFilePath + " in bucket");
//                 return false;
//             }
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId() + "\nRequest object key:" + objectName);
//             return false;
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//             return false;
//         }
//     }

//     public void listingFilesInOSS() {
//         // Listing all files from OSS bucket
//         try {
//             ObjectListing objectListing = ossClient.listObjects(bucketName);
//             List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
//             List<String> keys = new ArrayList<String>();
//             for (OSSObjectSummary s : sums) {
//                 logger.debug("Existing file on OSS: {}", s.getKey());
//                 keys.add(s.getKey());
//             }
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId());
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//         }
//     }

//     public void cleanUpOSS() {
//         // Delete all files from OSS bucket
//         try {
//             while (true) {
//                 ObjectListing objectListing = ossClient.listObjects(bucketName);
//                 List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
//                 List<String> keys = new ArrayList<String>();
//                 for (OSSObjectSummary s : sums) {
//                     logger.debug("Existing file on OSS: {}", s.getKey());
//                     keys.add(s.getKey());
//                 }
//                 if (keys.isEmpty()) {
//                     logger.debug("No file on OSS, nothing to delete");
//                     break;
//                 } else {
//                     DeleteObjectsResult deleteObjectsResult = ossClient
//                             .deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys).withEncodingType("url"));
//                     List<String> deletedObjects = deleteObjectsResult.getDeletedObjects();
//                     try {
//                         for (String obj : deletedObjects) {
//                             String deleteObj = URLDecoder.decode(obj, "UTF-8");
//                             logger.debug("Delete file: {}", deleteObj);
//                         }
//                     } catch (UnsupportedEncodingException e) {
//                         e.printStackTrace();
//                     }
//                 }
//             }

//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId());
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//         }
//     }

//     public void checkBucketStatusAndCreateBucketIfNotExist() {
//         try {
//             // Check if the bucket exists then update/download/delete.
//             if (!ossClient.doesBucketExist(bucketName)) {
//                 logger.debug("Bucket not exist, creating：{}", bucketName);
//                 ossClient.createBucket(bucketName);
//             }
//             BucketInfo info = ossClient.getBucketInfo(bucketName);
//             logger.debug(
//                     "Target Bucket is created：{}\n\tBucket Info:\n\tData Center: {}\n\tUser Info: {}\n\tCreate Time: {}",
//                     bucketName, info.getBucket().getLocation(), info.getBucket().getOwner(),
//                     info.getBucket().getCreationDate());
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId());
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//         } finally {
//             if (ossClient != null) {
//                 logger.debug("OSS Client is start and connected");
//             }
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         OSSAccess ossAccess = new OSSAccess();
//         try {
//             ossAccess.cleanUpOSS();
//             // byte[] content = "Hello OSS".getBytes();
//             // ossAccess.uploadFileToOSS("test.md",
//             // content);
//             // ossAccess.downloadFileFromOSS("test.md",
//             // "test.md.d");
//         } catch (OSSException oe) {
//             logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
//                     + "\nRequest ID:" + oe.getRequestId());
//         } catch (ClientException ce) {
//             logger.error("OSS Internet Error Message:" + ce.getMessage());
//         } finally {
//             if (ossAccess != null) {
//                 ossAccess.cleanUpOSS();
//                 System.out.println("OSS cleanup and closed.");
//             }
//         }
//     }
// }

// The following is the code for using a general server node (transfer data via simple socket) as the cold tier.
public class OSSAccess implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OSSAccess.class);

    private static String localIP = FBUtilities.getBroadcastAddressAndPort().toString(false).replace('/', '_');
    private final int maxConcurrentDownloads = DatabaseDescriptor.getMaxConcurrentDownload();
    private final Semaphore semaphore = new Semaphore(maxConcurrentDownloads);

    public OSSAccess() {
        logger.info("[ELECT-INFO]: Utilize general server node as cold tier, IP address: "
                + DatabaseDescriptor.getColdTierIP() + ", Port: " + DatabaseDescriptor.getColdTierPort());
    }

    public void close() throws Exception {
    }

    public boolean uploadFileToOSS(String targetFilePath, byte[] content) {
        String objectName = targetFilePath.replace('/', '_') + localIP;
        logger.debug("ELECT-Debug: target object name is ({})", objectName);
        try (Socket socket = new Socket(DatabaseDescriptor.getColdTierIP(), DatabaseDescriptor.getColdTierPort());
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {
            long startUploadTime = System.currentTimeMillis();
            objectOutputStream.writeUTF("UPLOAD");
            objectOutputStream.writeUTF(objectName);
            objectOutputStream.writeObject(content);
            objectOutputStream.flush();
            long timeCost = System.currentTimeMillis() - startUploadTime;
            logger.debug(
                    "[ELECT-Debug]: upload file to OSS cost time is ({}ms), the file size is ({}), file name is ({})",
                    timeCost, content.length, targetFilePath);
            return objectInputStream.readBoolean(); // Wait and read the response
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean uploadFileToOSS(String targetFilePath, String content) {
        return uploadFileToOSS(targetFilePath, content.getBytes());
    }

    public boolean uploadFileToOSS(String targetFilePath) {
        try {
            long startTime = System.currentTimeMillis();
            byte[] content = Files.readAllBytes(Paths.get(targetFilePath));
            boolean status = uploadFileToOSS(targetFilePath, content);
            long timeCost = System.currentTimeMillis() - startTime;
            StorageService.instance.migratedRawSSTableTimeSendCost += timeCost;
            return status;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean downloadFileFromOSS(String originalFilePath, String targetStorePath) {
        String objectName = originalFilePath.replace('/', '_') + localIP;
        try (Socket socket = new Socket(DatabaseDescriptor.getColdTierIP(), DatabaseDescriptor.getColdTierPort());
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {

            objectOutputStream.writeUTF("DOWNLOAD");
            objectOutputStream.writeUTF(objectName);
            objectOutputStream.flush();
            boolean fileExists = objectInputStream.readBoolean();
            if (fileExists) {
                byte[] content = (byte[]) objectInputStream.readObject();
                Files.write(Paths.get(targetStorePath), content);
                return true;
            } else {
                logger.error(
                        "[ELECT-ERROR] Could not find target object from the cold tier, object name = " + objectName);
                return false;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            logger.error(
                    "[ELECT-ERROR] Could not connected to the cold tier, object name = " + objectName);
            return false;
        }
    }

    public boolean downloadFileAsByteArrayFromOSS(String originalFilePath, String targetIp) {
        String objectName = originalFilePath.replace('/', '_') + "_" + targetIp;
        try {
            semaphore.acquire();
            try (Socket socket = new Socket(DatabaseDescriptor.getColdTierIP(), DatabaseDescriptor.getColdTierPort());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {

                objectOutputStream.writeUTF("DOWNLOAD");
                objectOutputStream.writeUTF(objectName);
                objectOutputStream.flush();
                System.out.println("Try to download from server: " + objectName + ", request sent");
                boolean fileExists = objectInputStream.readBoolean();
                System.out.println("Recv the file exist flag");
                if (fileExists) {
                    byte[] content = (byte[]) objectInputStream.readObject();
                    Files.write(Paths.get(originalFilePath), content);
                    logger.debug("[ELECT-Debug]: Downloaded original file from cold tier, file name is ({})",
                            objectName);
                    return true;
                } else {
                    System.out.println("File not found on server: " + objectName);
                    return false;
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("[ELECT-ERROR]: Download original file from OSS failed, file name is ({})", objectName);
            return false;
        } finally {
            semaphore.release();
        }
    }

    public static void main(String[] args) throws Exception {
        OSSAccess ossAccess = new OSSAccess();
        // try {
        // ossAccess.cleanUpOSS();
        // // byte[] content = "Hello OSS".getBytes();
        // // ossAccess.uploadFileToOSS("test.md",
        // // content);
        // // ossAccess.downloadFileFromOSS("test.md",
        // // "test.md.d");
        // } catch (OSSException oe) {
        // logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" +
        // oe.getErrorCode()
        // + "\nRequest ID:" + oe.getRequestId());
        // } catch (ClientException ce) {
        // logger.error("OSS Internet Error Message:" + ce.getMessage());
        // } finally {
        // if (ossAccess != null) {
        // ossAccess.cleanUpOSS();
        // System.out.println("OSS cleanup and closed.");
        // }
        // }
    }
}
