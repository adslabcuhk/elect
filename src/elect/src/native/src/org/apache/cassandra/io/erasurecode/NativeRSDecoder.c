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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "NativeRSDecoder.h"
#include "jni_common.h"

typedef struct _RSDecoder {
    IsalDecoder decoder;
    unsigned char* inputs[MMAX];
    unsigned char* outputs[MMAX];
} RSDecoder;

JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSDecoder_initImpl(
    JNIEnv* env, jobject thiz, jint numDataUnits, jint numParityUnits)
{
    RSDecoder* rsDecoder = (RSDecoder*)malloc(sizeof(RSDecoder));
    memset(rsDecoder, 0, sizeof(*rsDecoder));
    initDecoder(&rsDecoder->decoder, (int)numDataUnits, (int)numParityUnits);

    setCoder(env, thiz, &rsDecoder->decoder.coder);
}

JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSDecoder_decodeImpl(
    JNIEnv* env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
    jint dataLen, jintArray decodeIndexes, jintArray erasedIndexes, jobjectArray outputs,
    jintArray outputOffsets)
{
    RSDecoder* rsDecoder = (RSDecoder*)getCoder(env, thiz);
    if (!rsDecoder) {
        THROW(env, "java/io/IOException", "NativeRSRawDecoder closed");
    }

    int numDataUnits = rsDecoder->decoder.coder.numDataUnits;
    int numParityUnits = rsDecoder->decoder.coder.numParityUnits;
    int chunkSize = (int)dataLen;
    printf("NativeRSDecoder_decodeImpl, start to process: data unit = %d, parity unit = %d, data block size = %d\n", numDataUnits, numParityUnits, chunkSize);

    int* tmpDecodeIndexes = (int*)(*env)->GetIntArrayElements(env,
        decodeIndexes, NULL);
    int* tmpErasedIndexes = (int*)(*env)->GetIntArrayElements(env,
        erasedIndexes, NULL);
    int numUsedForDecode = (*env)->GetArrayLength(env, decodeIndexes);
    int numErased = (*env)->GetArrayLength(env, erasedIndexes);
    printf("NativeRSDecoder_decodeImpl, read data: target recovery data block number = %d, the first recovery request block ID = %d\n", numErased, tmpErasedIndexes[0]);
    getInputs(env, inputs, inputOffsets, rsDecoder->inputs, numDataUnits);
    getOutputs(env, outputs, outputOffsets, rsDecoder->outputs, numErased);
    printf("NativeRSDecoder_decodeImpl, start do decode\n");
    decode(&rsDecoder->decoder, rsDecoder->inputs, tmpDecodeIndexes, tmpErasedIndexes,
        numErased, rsDecoder->outputs, chunkSize);
}

JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSDecoder_destroyImpl(
    JNIEnv* env, jobject thiz)
{
    RSDecoder* rsDecoder = (RSDecoder*)getCoder(env, thiz);
    if (rsDecoder) {
        free(rsDecoder);
        setCoder(env, thiz, NULL);
    }
}
