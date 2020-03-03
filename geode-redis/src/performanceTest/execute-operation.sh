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
OPERATION=""
UNIQUE_NAME=""

while getopts ":o:n:c:h:p:" opt; do
  case ${opt} in
  o)
    OPERATION=${OPTARG}
    ;;
  n)
    UNIQUE_NAME=${OPTARG}
    ;;
  c)
    COMMAND_REPETITION_COUNT=${OPTARG}
    ;;
  h)
    REDIS_HOST=${OPTARG}
    ;;
  p)
    REDIS_PORT=${OPTARG}
    ;;
  \?)
    echo "Usage: ${0} -n name -o operation [-h host] [-p port] [-c (command repetition count)]"
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

if [ -z "${OPERATION}" ] || [ -z "${UNIQUE_NAME}" ]; then
  echo "-o and -n arguments are mandatory"
  exit 1
fi

rm -rf grbench-tmpdir/${UNIQUE_NAME}-operation-results.txt

redis-benchmark -h ${REDIS_HOST} -p ${REDIS_PORT} -t ${OPERATION} -n ${COMMAND_REPETITION_COUNT} -r 32767 >> grbench-tmpdir/${UNIQUE_NAME}-operation-results.txt
./summarize-operation-results.sh grbench-tmpdir/${UNIQUE_NAME}-operation-results.txt >> grbench-tmpdir/${UNIQUE_NAME}-run-results.csv

rm -rf grbench-tmpdir/${UNIQUE_NAME}-operation-results.txt
