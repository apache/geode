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

TEST_OP=""
NUM_CLIENTS=""

while getopts ":o:n:p:" opt; do
  case ${opt} in
  o)
    TEST_OP=${OPTARG}
    ;;
  p)
    PREFIX=${OPTARG}-
    ;;
  n)
    NUM_CLIENTS=${OPTARG}
    ;;
  \?)
    echo "Usage: ${0} -n name -o operation [-p operation name prefix]"
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

if [ -z "${TEST_OP}" ] || [ -z "${NUM_CLIENTS}" ]; then
  echo "-o and -n arguments are mandatory"
  exit 1
fi

MINIMUM=1000000
NINETY_FIVE=0
ONE_HUNDRED=0
RPS_SUM=0

X=0
while [[ ${X} -lt ${NUM_CLIENTS} ]]; do
  CURRENT_FILE=grbench-tmpdir/${TEST_OP}-${X}-run-results.csv
  CURRENT_RPS=$(cat ${CURRENT_FILE} | cut -d"," -f 5)
  RPS_SUM=$((${RPS_SUM} + ${CURRENT_RPS%.*}))
  CURRENT_MIN=$(cat ${CURRENT_FILE} | cut -d"," -f 2)
  if ((${CURRENT_MIN} < ${MINIMUM})); then
    MINIMUM=${CURRENT_MIN}
  fi
  CURRENT_95=$(cat ${CURRENT_FILE} | cut -d"," -f 3)
  if ((${CURRENT_95%.*} >= ${NINETY_FIVE})); then
    NINETY_FIVE=${CURRENT_95%.*}
  fi
  CURRENT_100=$(cat ${CURRENT_FILE} | cut -d"," -f 4)
  if ((${CURRENT_100} >= ${ONE_HUNDRED})); then
    ONE_HUNDRED=${CURRENT_100}
  fi
  ((X = X + 1))
  rm ${CURRENT_FILE}
done

RPS_AVG=$((${RPS_SUM} / ${NUM_CLIENTS}))
echo  ${PREFIX}${TEST_OP}","${MINIMUM}","${NINETY_FIVE}","${ONE_HUNDRED}","${RPS_AVG}
