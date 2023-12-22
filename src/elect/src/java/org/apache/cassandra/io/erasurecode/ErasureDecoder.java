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

public abstract class ErasureDecoder extends ErasureCoder {

    public ErasureDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
    }

    public void decode(ByteBuffer[] inputs, int[] decodeIndexes, int[] erasedIndexes, ByteBuffer[] outputs)
            throws IOException {
        ByteBufferDecodingState decodingState = new ByteBufferDecodingState(
                this, inputs, decodeIndexes, erasedIndexes, outputs);

        int dataLen = decodingState.decodeLength;
        if (dataLen == 0) {
            return;
        }
        doDecode(decodingState);
    }

    /**
     * Perform the real decoding using Direct ByteBuffer.
     * 
     * @param decodingState the decoding state
     */
    protected abstract void doDecode(ByteBufferDecodingState decodingState)
            throws IOException;

}
