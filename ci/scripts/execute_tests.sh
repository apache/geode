#!/usr/bin/env bash

#
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

set -e

BASE_DIR=$(pwd)

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

if [[ -z "${GRADLE_TASK}" ]]; then
  echo "GRADLE_TASK must be set. exiting..."
  exit 1
fi

REPODIR=$(cd geode; git rev-parse --show-toplevel)

if [[ ${PARALLEL_GRADLE:-"true"} == "true" ]]; then
  PARALLEL_GRADLE="--parallel"
else
  PARALLEL_GRADLE=""
fi
DEFAULT_GRADLE_TASK_OPTIONS="${PARALLEL_GRADLE} --console=plain --no-daemon -x javadoc -x spotlessCheck -x rat"


SSHKEY_FILE="instance-data/sshkey"
SSH_OPTIONS="-i ${SSHKEY_FILE} -o ConnectionAttempts=60 -o StrictHostKeyChecking=no"

INSTANCE_NAME="$(cat instance-data/instance-name)"
INSTANCE_IP_ADDRESS="$(cat instance-data/instance-ip-address)"
PROJECT="$(cat instance-data/project)"
ZONE="$(cat instance-data/zone)"

scp ${SSH_OPTIONS} ${SCRIPTDIR}/capture-call-stacks.sh geode@${INSTANCE_IP_ADDRESS}:.



if [[ -n "${PARALLEL_DUNIT}" && "${PARALLEL_DUNIT}" == "true" ]]; then
  PARALLEL_DUNIT="-PparallelDunit -PdunitDockerUser=geode"
  if [ -n "${DUNIT_PARALLEL_FORKS}" ]; then
    DUNIT_PARALLEL_FORKS="-PdunitParallelForks=${DUNIT_PARALLEL_FORKS}"
  fi
else
  PARALLEL_DUNIT=""
  DUNIT_PARALLEL_FORKS=""
fi


if [ -v CALL_STACK_TIMEOUT ]; then
  ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "tmux new-session -d -s callstacks; tmux send-keys  ~/capture-call-stacks.sh\ ${PARALLEL_DUNIT}\ ${CALL_STACK_TIMEOUT} C-m"
fi

case $ARTIFACT_SLUG in
  windows*)
    JAVA_BUILD_PATH=C:/java${JAVA_BUILD_VERSION}
    JAVA_TEST_PATH=C:/java${JAVA_TEST_VERSION}
    ;;
  *)
    JAVA_BUILD_PATH=/usr/lib/jvm/java-${JAVA_BUILD_VERSION}-openjdk-amd64
    JAVA_TEST_PATH=/usr/lib/jvm/java-${JAVA_TEST_VERSION}-openjdk-amd64
    ;;
esac

GRADLE_ARGS="-PtestJVM=${JAVA_TEST_PATH} \
    ${PARALLEL_DUNIT} \
    ${DUNIT_PARALLEL_FORKS} \
    -PdunitDockerImage=\$(docker images --format '{{.Repository}}:{{.Tag}}') \
    ${DEFAULT_GRADLE_TASK_OPTIONS} \
    ${GRADLE_TASK} \
    ${GRADLE_TASK_OPTIONS}"

case $ARTIFACT_SLUG in
  windows*)
    EXEC_COMMAND="bash -c 'export JAVA_HOME=${JAVA_BUILD_PATH}; echo Building with:; "'"'"${JAVA_BUILD_PATH}\bin\java.exe"'"'" -version; echo Testing with:; "'"'"${JAVA_TEST_PATH}\bin\java.exe"'"'" -version; cd geode; ./gradlew ${GRADLE_ARGS}'"
    ;;
  *)
    EXEC_COMMAND="bash -c 'export JAVA_HOME=${JAVA_BUILD_PATH} && echo Building with: && ${JAVA_BUILD_PATH}/bin/java -version && echo Testing with: && ${JAVA_TEST_PATH}/bin/java -version && cd geode && ./gradlew ${GRADLE_ARGS}'"
    ;;
esac

echo "${EXEC_COMMAND}"
ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "${EXEC_COMMAND}"
