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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.alibaba.OSSAccess;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECParityNodeVerbHandler implements IVerbHandler<ECParityNode> {

    public static final ECParityNodeVerbHandler instance = new ECParityNodeVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ECParityNodeVerbHandler.class);

    /*
     * TODO List:
     * 1. Receive erasure code from a parity node, and record it.
     */
    @Override
    public synchronized void doVerb(Message<ECParityNode> message) throws IOException {

        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            // logger.debug("ELECT-Debug: this is a forwarding header");
        }

        logger.debug("ELECT-Debug: Received message: {}", message.payload.parityHash);
        String receivedParityCodeDir = ECNetutils.getReceivedParityCodeDir();

        // if (DatabaseDescriptor.getEnableMigration()) {

        //     byte[] parityInBytes = new byte[StorageService.getErasureCodeLength()];
        //     message.payload.parityCode.get(parityInBytes);

        //     if (!StorageService.ossAccessObj.uploadFileToOSS(receivedParityCodeDir + message.payload.parityHash,
        //             parityInBytes)) {
        //         logger.error("[ELECT]: Could not upload parity SSTable: {}",
        //                 receivedParityCodeDir + message.payload.parityHash);
        //     }

        // } else {

        try {
            FileChannel fileChannel = FileChannel.open(Paths.get(receivedParityCodeDir, message.payload.parityHash),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            fileChannel.write(message.payload.parityCode);
            fileChannel.close();
            logger.debug("ELECT-Debug: write the parity code {} successfully!", message.payload.parityHash);
        } catch (IOException e) {
            logger.error("ELECT-ERROR: Failed to write erasure code ({})", message.payload.parityHash);
        }

        //}

    }

    private static void forwardToLocalNodes(Message<ECParityNode> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECParityNode> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECParityNode> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }

    // public static void main(String[] args) {
    // File parityCodeFile = new File(parityCodeDir +
    // String.valueOf("parityCodeTest"));

    // if (!parityCodeFile.exists()) {
    // try {
    // parityCodeFile.createNewFile();
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // }
    // logger.debug("ELECT-Debug: parityCodeFile.getName is {}",
    // parityCodeFile.getAbsolutePath());
    // }

}
