/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_apache_cassandra_io_erasurecode_NativeRSEncoder */

#ifndef _Included_org_apache_cassandra_io_erasurecode_NativeRSEncoder
#define _Included_org_apache_cassandra_io_erasurecode_NativeRSEncoder
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_cassandra_io_erasurecode_NativeRSEncoder
 * Method:    initImpl
 * Signature: (II)V
 */
JNIEXPORT void JNICALL Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_initImpl(JNIEnv*, jobject, jint, jint);

/*
 * Class:     org_apache_cassandra_io_erasurecode_NativeRSEncoder
 * Method:    encodeImpl
 * Signature: ([Ljava/nio/ByteBuffer;[II[Ljava/nio/ByteBuffer;[I)V
 */
JNIEXPORT void JNICALL Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_encodeImpl(JNIEnv*, jobject, jobjectArray, jintArray, jint, jobjectArray, jintArray);

/*
 * Class:     org_apache_cassandra_io_erasurecode_NativeRSEncoder
 * Method:    encodeUpdateImpl
 * Signature: ([Ljava/nio/ByteBuffer;[II[Ljava/nio/ByteBuffer;[I)V
 */
JNIEXPORT void JNICALL
Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_encodeUpdateImpl(
    JNIEnv* env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
    jint dataLen, jint targetUpdateDataIndex, jobjectArray outputs, jintArray outputOffsets);

/*
 * Class:     org_apache_cassandra_io_erasurecode_NativeRSEncoder
 * Method:    destroyImpl
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_cassandra_io_erasurecode_NativeRSEncoder_destroyImpl(JNIEnv*, jobject);

#ifdef __cplusplus
}
#endif
#endif
