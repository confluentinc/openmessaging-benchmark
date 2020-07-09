#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


DRIVERS=$1

function runtest() {
  for i in "${TESTS[@]}"
  do
    /opt/benchmark/bin/benchmark --drivers $DRIVERS $i -o $(echo $i | sed s/yaml/json/)
  done
}

###################### 100 byte tests ######################
########### Producer Only tests ###########
TESTS=("100B/0consumer/1-topic-16-partitions-100b-4-producers-10k-rate.yaml" \
"100B/0consumer/1-topic-16-partitions-100b-4-producers-50k-rate.yaml" \
"100B/0consumer/1-topic-16-partitions-100b-4-producers-100k-rate.yaml" \
"100B/0consumer/1-topic-16-partitions-100b-4-producers-200k-rate.yaml" \
"100B/0consumer/1-topic-16-partitions-100b-4-producers-500k-rate.yaml" \
"100B/0consumer/1-topic-16-partitions-100b-4-producers-1000k-rate.yaml")
runtest

########### Producer + Consumer tests ###########
TESTS=("100B/1consumer/1-topic-16-partitions-100b-4-producers-10k-rate.yaml" \
"100B/1consumer/1-topic-16-partitions-100b-4-producers-50k-rate.yaml" \
"100B/1consumer/1-topic-16-partitions-100b-4-producers-100k-rate.yaml" \
"100B/1consumer/1-topic-16-partitions-100b-4-producers-200k-rate.yaml" \
"100B/1consumer/1-topic-16-partitions-100b-4-producers-500k-rate.yaml" \
"100B/1consumer/1-topic-16-partitions-100b-4-producers-1000k-rate.yaml")
runtest


###################### 1 KB tests ######################
########### Producer Only tests ###########
TESTS=("1KB/0consumer/1-topic-16-partitions-1kb-4-producers-10k-rate.yaml" \
"1KB/0consumer/1-topic-16-partitions-1kb-4-producers-50k-rate.yaml" \
"1KB/0consumer/1-topic-16-partitions-1kb-4-producers-100k-rate.yaml" \
"1KB/0consumer/1-topic-16-partitions-1kb-4-producers-200k-rate.yaml" \
"1KB/0consumer/1-topic-16-partitions-1kb-4-producers-500k-rate.yaml" \
"1KB/0consumer/1-topic-16-partitions-1kb-4-producers-1000k-rate.yaml")
runtest

########### Producer + Consumer tests ###########
TESTS=("1KB/1consumer/1-topic-16-partitions-1kb-4-producers-10k-rate.yaml" \
"1KB/1consumer/1-topic-16-partitions-1kb-4-producers-50k-rate.yaml" \
"1KB/1consumer/1-topic-16-partitions-1kb-4-producers-100k-rate.yaml" \
"1KB/1consumer/1-topic-16-partitions-1kb-4-producers-200k-rate.yaml" \
"1KB/1consumer/1-topic-16-partitions-1kb-4-producers-500k-rate.yaml" \
"1KB/1consumer/1-topic-16-partitions-1kb-4-producers-1000k-rate.yaml")
runtest
