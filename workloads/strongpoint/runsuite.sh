#!/bin/bash

DRIVERS=$1

function runtest() {
  for i in "${TESTS[@]}"
  do
    bin/benchmark --drivers $DRIVERS $i -o $(echo $i | sed s/yaml/json/)
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