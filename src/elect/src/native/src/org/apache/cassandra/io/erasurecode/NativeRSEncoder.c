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

#include "NativeRSEncoder.h"
#include "jni_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct _RSEncoder {
    IsalEncoder encoder;
    unsigned char* inputs[MMAX];
    unsigned char* outputs[MMAX];
} RSEncoder;

JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_initImpl(JNIEnv* env,
    jobject thiz, jint numDataUnits, jint numParityUnits)
{
    RSEncoder* rsEncoder = (RSEncoder*)malloc(sizeof(RSEncoder));
    memset(rsEncoder, 0, sizeof(*rsEncoder));
    initEncoder(&rsEncoder->encoder, (int)numDataUnits, (int)numParityUnits);

    setCoder(env, thiz, &rsEncoder->encoder.coder);
}

JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_encodeImpl(
    JNIEnv* env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
    jint dataLen, jobjectArray outputs, jintArray outputOffsets)
{
    RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);
    if (!rsEncoder) {
        THROW(env, "java/io/IOException", "NativeRSRawEncoder closed");
        return;
    }

    int numDataUnits = rsEncoder->encoder.coder.numDataUnits;
    int numParityUnits = rsEncoder->encoder.coder.numParityUnits;
    int chunkSize = (int)dataLen;

    getInputs(env, inputs, inputOffsets, rsEncoder->inputs, numDataUnits);
    getOutputs(env, outputs, outputOffsets, rsEncoder->outputs, numParityUnits);

    encode(&rsEncoder->encoder, rsEncoder->inputs, rsEncoder->outputs, chunkSize);
}

JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_encodeUpdateImpl(
    JNIEnv* env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
    jint dataLen, jint targetUpdateDataIndex, jobjectArray outputs, jintArray outputOffsets)
{
    RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);
    if (!rsEncoder) {
        THROW(env, "java/io/IOException", "NativeRSRawEncoder closed");
        return;
    }

    int numParityUnits = rsEncoder->encoder.coder.numParityUnits;
    int chunkSize = (int)dataLen;
    int numInputs = (*env)->GetArrayLength(env, inputs);
    // printf("NativeRSEncoder_encodeUpdateImpl, input size = %d, parity number = %d\n", numInputs, numParityUnits);

    getInputs(env, inputs, inputOffsets, rsEncoder->inputs, 2 + numParityUnits);
    getOutputs(env, outputs, outputOffsets, rsEncoder->outputs, numParityUnits);
    // printf("NativeRSEncoder_encodeUpdateImpl, update data block for index = %d\n", (int)targetUpdateDataIndex);
    encodeUpdate(&rsEncoder->encoder, rsEncoder->inputs, (int)targetUpdateDataIndex, rsEncoder->outputs, chunkSize);
    // printf("NativeRSEncoder_encodeUpdateImpl, update data block for index = %d done!!!\n", (int)targetUpdateDataIndex);
}

JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_destroyImpl(
    JNIEnv* env, jobject thiz)
{
    RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);
    if (rsEncoder) {
        free(rsEncoder);
        setCoder(env, thiz, NULL);
    }
}
