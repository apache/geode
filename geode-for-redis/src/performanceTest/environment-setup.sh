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

SERVER_TYPE=""
TEST_RUN_COUNT=10
COMMAND_REPETITION_COUNT=100000
REDIS_HOST=localhost
REDIS_PORT=6379
FILE_PREFIX=""

while getopts ":f:rgt:c:h:p:" opt; do
  case ${opt} in
  r)
    SERVER_TYPE='redis'
    ;;
  g)
    SERVER_TYPE='geode'
    ;;
  f)
    FILE_PREFIX=${OPTARG}
    ;;
  t)
    TEST_RUN_COUNT=${OPTARG}
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
    echo "Usage: ${0} -r (Redis) | -g (Geode) [-h host] [-p port] [-t (test run count)] [-c (command repetition count)]"
    exit 0
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

if [ -z ${SERVER_TYPE} ]; then
  echo "Please specify native Redis (-r) or Geode for Redis (-g)"
  exit 1
fi

SCRIPT_DIR=$(
  cd $(dirname $0)
  pwd
)

function kill_geode() {
  pkill -9 -f ServerLauncher || true
  pkill -9 -f LocatorLauncher || true
  rm -rf server1
  rm -rf locator1
}

nc -zv ${REDIS_HOST} ${REDIS_PORT} 1>&2
SERVER_NOT_FOUND=$?

if [ ${SERVER_TYPE} == "geode" ]; then
  if [ ${SERVER_NOT_FOUND} -eq 0 ]; then
    echo "Redis server detected already running at port ${REDIS_PORT}}"
    echo "Please stop sever before running this script"
    exit 1
  fi

  GEODE_BASE=$(
    cd $SCRIPT_DIR/../../..
    pwd
  )

  cd $GEODE_BASE

  kill_geode

  ./gradlew devBuild installD

  GFSH=$PWD/geode-assembly/build/install/apache-geode/bin/gfsh

  $GFSH -e "start locator --name=locator1"

  $GFSH -e "start server
          --name=server1
          --log-level=none
          --locators=localhost[10334]
          --server-port=0
          --geode-for-redis-port=6379
          --geode-for-redis-bind-address=127.0.0.1"
else
  if [ ${SERVER_NOT_FOUND} -eq 1 ]; then
    echo "No server compatible with Redis detected on host '${REDIS_HOST}' at port '${REDIS_PORT}'"
    exit 1
  fi
fi

cd ${SCRIPT_DIR}

if [ -z ${FILE_PREFIX} ]; then
  ./benchmark.sh -h ${REDIS_HOST} -p ${REDIS_PORT} -t "${TEST_RUN_COUNT}" -c "${COMMAND_REPETITION_COUNT}"
else
  ./benchmark.sh -f ${FILE_PREFIX} -h ${REDIS_HOST} -p ${REDIS_PORT} -t "${TEST_RUN_COUNT}" -c "${COMMAND_REPETITION_COUNT}"
fi

if [ ${SERVER_TYPE} == "geode" ]; then
  kill_geode
  sleep 1 # Back to back runs need this delay or 'nc' doesn't detect shutdown correctly
fi
