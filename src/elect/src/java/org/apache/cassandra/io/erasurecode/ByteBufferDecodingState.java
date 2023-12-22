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

import java.nio.ByteBuffer;

import org.apache.cassandra.exceptions.ErasureCodeException;

public class ByteBufferDecodingState {
    ErasureDecoder decoder;
    int decodeLength;
    ByteBuffer[] inputs;
    ByteBuffer[] outputs;
    boolean usingDirectBuffer;
    int[] inputOffsets;
    int[] outputOffsets;
    int[] decodeIndexes;
    int[] erasedIndexes;

    ByteBufferDecodingState(ErasureDecoder decoder, ByteBuffer[] inputs, int[] decodeIndexes, int[] erasedIndexes,
            ByteBuffer[] outputs) {
        this.decoder = decoder;
        this.inputs = inputs;
        this.outputs = outputs;
        this.decodeIndexes = decodeIndexes;
        this.erasedIndexes = erasedIndexes;
        ByteBuffer validInput = CoderUtil.findFirstValidInput(inputs);
        this.decodeLength = validInput.remaining();
        this.usingDirectBuffer = validInput.isDirect();

        this.inputOffsets = new int[inputs.length];
        this.outputOffsets = new int[outputs.length];
        ByteBuffer buffer;
        for (int i = 0; i < inputs.length; ++i) {
            buffer = inputs[i];
            if (buffer != null) {
                inputOffsets[i] = buffer.position();
            }
        }
        for (int i = 0; i < outputs.length; ++i) {
            buffer = outputs[i];
            outputOffsets[i] = buffer.position();
        }

        CoderUtil.checkParameters(inputs, erasedIndexes, outputs,
                decoder.getNumDataUnits(), decoder.getNumParityUnits());
        checkInputBuffers(inputs);
        checkOutputBuffers(outputs);
    }

    /**
     * Check and ensure the buffers are of the desired length and type, direct
     * buffers or not.
     * 
     * @param buffers the buffers to check
     */
    void checkInputBuffers(ByteBuffer[] buffers) {
        int validInputs = 0;

        for (ByteBuffer buffer : buffers) {
            if (buffer == null) {
                continue;
            }

            if (buffer.remaining() != decodeLength) {
                throw new ErasureCodeException(
                        "Invalid buffer, not of length " + decodeLength);
            }
            if (buffer.isDirect() != usingDirectBuffer) {
                throw new ErasureCodeException(
                        "Invalid buffer, isDirect should be " + usingDirectBuffer);
            }

            validInputs++;
        }

        if (validInputs < decoder.getNumDataUnits()) {
            throw new ErasureCodeException(
                    "No enough valid inputs are provided, not recoverable");
        }
    }

    /**
     * Check and ensure the buffers are of the desired length and type, direct
     * buffers or not.
     * 
     * @param buffers the buffers to check
     */
    void checkOutputBuffers(ByteBuffer[] buffers) {
        for (ByteBuffer buffer : buffers) {
            if (buffer == null) {
                throw new ErasureCodeException(
                        "Invalid buffer found, not allowing null");
            }

            if (buffer.remaining() != decodeLength) {
                throw new ErasureCodeException(
                        "Invalid buffer, not of length " + decodeLength);
            }
            if (buffer.isDirect() != usingDirectBuffer) {
                throw new ErasureCodeException(
                        "Invalid buffer, isDirect should be " + usingDirectBuffer);
            }
        }
    }

}
