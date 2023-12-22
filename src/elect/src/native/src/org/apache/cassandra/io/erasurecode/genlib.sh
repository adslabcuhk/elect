#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export PATH=$JAVA_HOME/bin:$PATH

# Generate libec.so
gcc -I ${JAVA_HOME}/include/linux/ -I ${JAVA_HOME}/include/ -I /usr/includ \
    -Wall -g -fPIC -shared -o libec.so \
    jni_common.c erasure_coder.c dump.c \
    NativeRSEncoder.c NativeRSDecoder.c \
    -L/usr/lib -lisal
echo "Complete generating libec.so!"