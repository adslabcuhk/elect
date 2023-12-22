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

package org.apache.cassandra.io.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErasureCodeTest {
    private static Logger logger = LoggerFactory.getLogger(ErasureCodeTest.class.getName());

    public static void erasureCodeTest() throws IOException {
        final int k = 4, m = 2;
        int codeLength = 419483300;
        Random random = new Random((long) 123);

        // Generate encoder and decoder
        ErasureCoderOptions ecOptions = new ErasureCoderOptions(k, m);
        ErasureEncoder encoder = new NativeRSEncoder(ecOptions);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);

        // Encoding input and output
        ByteBuffer[] data = new ByteBuffer[k];
        ByteBuffer[] ModifiedData = new ByteBuffer[k];
        ByteBuffer[] parity = new ByteBuffer[m];
        ByteBuffer[] SecondParity = new ByteBuffer[m];
        ByteBuffer[] XORParity = new ByteBuffer[m];
        ByteBuffer[] UpdateParity = new ByteBuffer[m];

        // Decoding input and output
        ByteBuffer[] recoveryOriginalSrc = new ByteBuffer[k];
        ByteBuffer[] recoveryUpdatedSrc = new ByteBuffer[k];
        int[] eraseIndexes = { 0 };
        int[] decodeIndexes = { 4, 1, 2, 3 };
        ByteBuffer[] outputsOriginal = new ByteBuffer[1];
        ByteBuffer[] outputsUpdated = new ByteBuffer[1];

        // Prepare src for encoding
        byte[] tmpArray = new byte[codeLength];
        for (int i = 0; i < k; i++) {
            data[i] = ByteBuffer.allocateDirect(codeLength);
            random.nextBytes(tmpArray);
            data[i].put(tmpArray);
            data[i].rewind();
            ModifiedData[i] = ByteBuffer.allocateDirect(codeLength);
            ModifiedData[i].put(tmpArray);
            ModifiedData[i].rewind();
        }
        // modify data block 0
        random.nextBytes(tmpArray);
        ModifiedData[0].put(tmpArray);
        ModifiedData[0].rewind();

        // Prepare outputsOriginal for encoding
        for (int i = 0; i < m; i++) {
            parity[i] = ByteBuffer.allocateDirect(codeLength);
            SecondParity[i] = ByteBuffer.allocateDirect(codeLength);
            XORParity[i] = ByteBuffer.allocateDirect(codeLength);
            UpdateParity[i] = ByteBuffer.allocateDirect(codeLength);
        }

        // Encode
        logger.debug("ErasureCodeTest - encode() original data !");
        encoder.encode(data, parity);

        logger.debug("ErasureCodeTest - encode() updated data !");
        encoder.encode(ModifiedData, SecondParity);

        // Prepare recoveryOriginalSrc for decoding
        recoveryOriginalSrc[0] = parity[0];
        for (int i = 1; i < k; i++) {
            data[i].rewind();
            recoveryOriginalSrc[i] = data[i];
        }

        recoveryUpdatedSrc[0] = SecondParity[0];
        for (int i = 1; i < k; i++) {
            ModifiedData[i].rewind();
            recoveryUpdatedSrc[i] = ModifiedData[i];
        }

        // Prepare outputsOriginal for decoding
        outputsOriginal[0] = ByteBuffer.allocateDirect(codeLength);
        outputsUpdated[0] = ByteBuffer.allocateDirect(codeLength);

        // Decode
        logger.debug("ErasureCodeTest - decode() original data blocks!");
        decoder.decode(recoveryOriginalSrc, decodeIndexes, eraseIndexes, outputsOriginal);
        logger.debug("ErasureCodeTest - decode() updated data blocks!");
        decoder.decode(recoveryUpdatedSrc, decodeIndexes, eraseIndexes, outputsUpdated);

        data[0].rewind();
        if (outputsOriginal[0].compareTo(data[0]) == 0) {
            logger.debug("ErasureCodeTest(1) - decoding Succeeded, same recovered data!");
        } else {
            logger.debug("ErasureCodeTest(1) - decoding Failed, diffent recovered data ");
        }

        ModifiedData[0].rewind();
        if (outputsUpdated[0].compareTo(ModifiedData[0]) == 0) {
            logger.debug("ErasureCodeTest(2) - decoding Succeeded, same recovered data!");
        } else {
            logger.debug("ErasureCodeTest(2) - decoding Failed, diffent recovered data ");
        }

        // update
        logger.debug("ErasureCodeTest - Perform encode update for data block 0!");
        ByteBuffer[] dataUpdate = new ByteBuffer[2 + m];
        dataUpdate[0] = ByteBuffer.allocateDirect(codeLength);
        data[0].rewind();
        dataUpdate[0] = data[0];
        dataUpdate[0].rewind();
        dataUpdate[1] = ByteBuffer.allocateDirect(codeLength);
        ModifiedData[0].rewind();
        dataUpdate[1] = ModifiedData[0];
        dataUpdate[1].rewind();
        for (int i = 0; i < m; i++) {
            parity[i].rewind();
            dataUpdate[i + 2] = ByteBuffer.allocateDirect(codeLength);
            dataUpdate[i + 2] = parity[i];
            dataUpdate[i + 2].rewind();
        }

        // perform update
        encoder.encodeUpdate(dataUpdate, UpdateParity, 0);

        // recoveryOriginalSrc[0] = null;
        // for (int i = 1; i < k; i++) {
        // data[i].rewind();
        // recoveryOriginalSrc[i] = data[i];
        // // logger.debug("recoveryOriginalSrc[" + i + "]: position() = " +
        // // recoveryOriginalSrc[i].position() + ", remaining() = "
        // // + recoveryOriginalSrc[i].remaining());
        // }
        // recoveryOriginalSrc[0] = parity[0];

        // // Prepare outputsOriginal for decoding
        // outputsOriginal[0] = ByteBuffer.allocateDirect(codeLength);

        // // Decode
        // logger.debug("ErasureCodeTest - second decode()!");
        // decoder.decode(recoveryOriginalSrc, decodeIndexes, eraseIndexes,
        // outputsOriginal);

        // data[0].rewind();
        // if (outputsOriginal[0].compareTo(dataUpdate[0]) == 0) {
        // logger.debug("ErasureCodeTest - decoding Succeeded, same recovered data!");
        // } else {
        // logger.debug("ErasureCodeTest - decoding Failed, diffent recovered data ");
        // }

        encoder.release();
        decoder.release();
    }

    public static void main(String[] args) throws IOException {
        erasureCodeTest();
    }
}
