#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


export TERM=${TERM:-dumb}
export PAGER=cat

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"


export GEODE_BUILD=~/geode
export CALLSTACKS_DIR=${GEODE_BUILD}/callstacks

#SLEEP_TIME is in seconds
PARALLEL_DUNIT=${1}
SLEEP_TIME=${2}
COUNT=3
STACK_INTERVAL=5

if [[ -z "${PARALLEL_DUNIT}" ]]; then
  echo "PARALLEL_DUNIT must be set. exiting..."
  exit 1
fi

if [[ -z "${SLEEP_TIME}" ]]; then
  echo "SLEEP_TIME must be set. exiting..."
  exit 1
fi


mkdir -p ${CALLSTACKS_DIR}

sleep ${SLEEP_TIME}

echo "Capturing call stacks"

for (( h=0; h<${COUNT}; h++)); do
    today=`date +%Y-%m-%d-%H-%M-%S`
    logfile=${CALLSTACKS_DIR}/callstacks-${today}.txt
    if [ -n "${PARALLEL_DUNIT}" ]; then
        mapfile -t containers < <(docker ps --format '{{.Names}}')

        for (( i=0; i<${#containers[@]}; i++ )); do
            echo "Container: ${containers[i]}" | tee -a ${logfile};
            mapfile -t processes < <(docker exec ${containers[i]} jps | grep ChildVM | cut -d ' ' -f 1)
            echo "Got past processes."
            for ((j=0; j<${#processes[@]}; j++ )); do
                  echo "********* Dumping stack for process ${processes[j]}:" | tee -a ${logfile}
                      docker exec ${containers[i]} jstack -l ${processes[j]} >> ${logfile}
            done
        done
    else
        mapfile -t processes < <(jps | grep ChildVM | cut -d ' ' -f 1)
        echo "Got past processes."
        for ((j=0; j<${#processes[@]}; j++ )); do
              echo "********* Dumping stack for process ${processes[j]}:" | tee -a ${logfile}
                  jstack -l ${processes[j]} >> ${logfile}
        done

    fi
    sleep ${STACK_INTERVAL}
done

echo "Checking progress files:"

if [ -n "${PARALLEL_DUNIT}" ]; then
    mapfile -t progressfiles < <(find ${GEODE_BUILD} -name "*-progress.txt")
    for (( i=0; i<${#progressfiles[@]}; i++)); do
        echo "Checking progress file: ${progressfiles[i]}"
        /usr/local/bin/dunit-progress hang ${progressfiles[i]} | tee -a ${CALLSTACKS_DIR}/dunit-hangs.txt
    done
fi
