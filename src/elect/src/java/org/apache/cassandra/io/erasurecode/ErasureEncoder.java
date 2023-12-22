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

import org.antlr.runtime.tree.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ErasureEncoder extends ErasureCoder {
    private static Logger logger = LoggerFactory.getLogger(ErasureEncoder.class.getName());

    public ErasureEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
    }

    /**
     * Encode with inputs and generates outputs
     */
    public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs)
            throws IOException {
        ByteBufferEncodingState bbestate = new ByteBufferEncodingState(this, inputs, outputs, false);
        int dataLen = bbestate.encodeLength;
        if (dataLen == 0) {
            return;
        }
        // Perform encoding
        doEncode(bbestate);
    }

    /**
     * Encode with inputs and generates outputs
     * 
     * @param inputs          the new data for parity update
     * @param outputs         outputs is old parity code when first call this
     *                        method, after execute this method, outputs is new
     *                        parity code
     * @param targetDataIndex the index of the inputs in the coding blocks
     */
    public void encodeUpdate(ByteBuffer[] targetCodingBlock, ByteBuffer[] parityBuffers, int targetDataIndex)
            throws IOException {
        ByteBufferEncodingState bbestate = new ByteBufferEncodingState(this, targetCodingBlock, parityBuffers,
                false);

        int dataLen = bbestate.encodeLength;
        if (dataLen == 0) {
            return;
        }

        // Perform encoding
        doEncodeUpdate(bbestate, targetDataIndex);
        // logger.debug("[ELECT] perform encode update done.");
    }

    /**
     * Perform the real encoding using direct bytebuffer.
     * 
     * @param encodingState, the encoding state.
     * @throws IOException
     */
    protected abstract void doEncode(ByteBufferEncodingState encodingState) throws IOException;

    /**
     * Perform the real encoding using direct bytebuffer.
     * 
     * @param encodingState, the encoding state.
     * @throws IOException
     */
    protected abstract void doEncodeUpdate(ByteBufferEncodingState encodingState, int targetDataIndex)
            throws IOException;

}
