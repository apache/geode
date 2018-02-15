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
export BUILDROOT=$(pwd)
export DEST_DIR=${BUILDROOT}/built-geode
export GEODE_BUILD=${DEST_DIR}/test
export CALLSTACKS_DIR=${GEODE_BUILD}/callstacks

#SLEEP_TIME is in seconds
SLEEP_TIME=${1}
COUNT=3
STACK_INTERVAL=5


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
