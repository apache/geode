#!/usr/bin/env bash

#Licensed to the Apache Software Foundation (ASF) under one or more contributor license
#agreements. See the NOTICE file distributed with this work for additional information regarding
#copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance with the License. You may obtain a
#copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software distributed under the License
#is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#or implied. See the License for the specific language governing permissions and limitations under
#the License.

COMMAND_REPETITION_COUNT=100000
REDIS_HOST=localhost
REDIS_PORT=6379
NUM_CLIENTS=5
FILE_PREFIX=""

while getopts ":f:c:h:p:n:" opt; do
  case ${opt} in
  c)
    COMMAND_REPETITION_COUNT=${OPTARG}
    ;;
  n)
    NUM_CLIENTS=${OPTARG}
    ;;
  f)
    FILE_PREFIX=${OPTARG}_
    ;;
  h)
    REDIS_HOST=${OPTARG}
    ;;
  p)
    REDIS_PORT=${OPTARG}
    ;;
  \?)
    echo "Usage: ${0} [-n number of clients] [-h host] [-p port] [-c (command repetition count)]"
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

REDIS_BENCHMARK_COMMANDS=("SET" "GET" "INCR" "MSET")

rm -rf grbench-tmpdir
mkdir grbench-tmpdir

echo "Command,Fastest Response Time (Msec),95th-99th Percentile (Msec),Slowest Response Time (Msec),Avg Requests Per Second" > ${FILE_PREFIX}benchmark_summary.csv

for TEST_OP in ${REDIS_BENCHMARK_COMMANDS[@]}; do
  echo "Testing " ${TEST_OP}
  X=0
  while [[ ${X} -lt ${NUM_CLIENTS} ]]; do
    UNIQUE_NAME=${TEST_OP}-${X}
    ./execute-operation.sh -h ${REDIS_HOST} -p ${REDIS_PORT} -o ${TEST_OP} -c ${COMMAND_REPETITION_COUNT} -n ${UNIQUE_NAME} &
    pids[${X}]=$!
    ((X = X + 1))
  done

  for pid in ${pids[@]}; do
    echo "Waiting for " ${X} " commands to complete..."
    wait $pid
    ((X = X - 1))
  done

  ./summarize-batch-results.sh -o ${TEST_OP} -n ${NUM_CLIENTS} >> ${FILE_PREFIX}benchmark_summary.csv
done


echo "Testing commands in parallel..."
X=0
for TEST_OP in ${REDIS_BENCHMARK_COMMANDS[@]}; do
    UNIQUE_NAME=${TEST_OP}-0
    ./execute-operation.sh -h ${REDIS_HOST} -p ${REDIS_PORT} -o ${TEST_OP} -c ${COMMAND_REPETITION_COUNT} -n ${UNIQUE_NAME} &
    otherpids[${X}]=$!
    ((X = X + 1))
done

for pid in ${otherpids[@]}; do
  echo "Waiting for " ${X} " commands to complete..."
  wait $pid
  ((X = X - 1))
done

for TEST_OP in ${REDIS_BENCHMARK_COMMANDS[@]}; do
  ./summarize-batch-results.sh -p "Parallel" -o ${TEST_OP} -n 1 >> ${FILE_PREFIX}benchmark_summary.csv
done

rm -rf grbench-tmpdir