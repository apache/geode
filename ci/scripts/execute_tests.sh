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

. ${SCRIPTDIR}/shared_utilities.sh
is_source_from_pr_testable "geode" "$(get_geode_pr_exclusion_dirs)" || exit 0

REPODIR=$(cd geode; git rev-parse --show-toplevel)

if [[ ${PARALLEL_GRADLE:-"true"} == "true" ]]; then
  PARALLEL_GRADLE="--parallel"
else
  PARALLEL_GRADLE="--no-parallel"
fi
DEFAULT_GRADLE_TASK_OPTIONS="${PARALLEL_GRADLE} --console=plain --no-daemon"
GRADLE_SKIP_TASK_OPTIONS="-x javadoc -x spotlessCheck -x rat"

SSHKEY_FILE="instance-data/sshkey"
SSH_OPTIONS="-i ${SSHKEY_FILE} -o ConnectionAttempts=60 -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ServerAliveCountMax=5"

INSTANCE_IP_ADDRESS="$(cat instance-data/instance-ip-address)"

scp ${SSH_OPTIONS} ${SCRIPTDIR}/capture-call-stacks.sh geode@${INSTANCE_IP_ADDRESS}:.



if [[ -n "${PARALLEL_DUNIT}" && "${PARALLEL_DUNIT}" == "true" ]]; then
  PARALLEL_DUNIT="-PparallelDunit -PdunitDockerUser=geode -PdunitDockerImage=\$(docker images --format '{{.Repository}}:{{.Tag}}')"
  if [ -n "${DUNIT_PARALLEL_FORKS}" ]; then
    DUNIT_PARALLEL_FORKS="-PdunitParallelForks=${DUNIT_PARALLEL_FORKS}"
  fi
else
  PARALLEL_DUNIT=""
  DUNIT_PARALLEL_FORKS=""
fi


case $ARTIFACT_SLUG in
  windows*)
    JAVA_BUILD_PATH=C:/java${JAVA_BUILD_VERSION}
    JAVA_TEST_PATH=C:/java${JAVA_TEST_VERSION}
    SEP=";"
    ;;
  *)
    JAVA_BUILD_PATH=/usr/lib/jvm/java-${JAVA_BUILD_VERSION}-openjdk-amd64
    JAVA_TEST_PATH=/usr/lib/jvm/java-${JAVA_TEST_VERSION}-openjdk-amd64
    SEP="&&"
    ;;
esac


if [ -v CALL_STACK_TIMEOUT ]; then
  ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "export JAVA_HOME=${JAVA_TEST_PATH} && tmux new-session -d -s callstacks; tmux send-keys  ~/capture-call-stacks.sh\ ${PARALLEL_DUNIT}\ ${CALL_STACK_TIMEOUT} C-m"
fi


GRADLE_ARGS=" \
    -PcompileJVM=${JAVA_BUILD_PATH} \
    -PcompileJVMVer=${JAVA_BUILD_VERSION} \
    -PtestJVM=${JAVA_TEST_PATH} \
    -PtestJVMVer=${JAVA_TEST_VERSION} \
    ${PARALLEL_DUNIT} \
    ${DUNIT_PARALLEL_FORKS} \
    ${DEFAULT_GRADLE_TASK_OPTIONS} \
    ${GRADLE_SKIP_TASK_OPTIONS} \
    ${GRADLE_TASK} \
    ${GRADLE_TASK_OPTIONS} \
    ${GRADLE_GLOBAL_ARGS}"

EXEC_COMMAND="bash -c 'echo Building with: $SEP \
  ${JAVA_BUILD_PATH}/bin/java -version $SEP \
  echo Testing with: $SEP \
  ${JAVA_TEST_PATH}/bin/java -version $SEP \
  mkdir -p /tmp $SEP \
  cp geode/ci/scripts/attach_sha_to_branch.sh /tmp/ $SEP \
  /tmp/attach_sha_to_branch.sh geode ${BUILD_BRANCH} $SEP \
  cd geode $SEP \
  cp gradlew gradlewStrict $SEP \
  sed -e 's/JAVA_HOME/GRADLE_JVM/g' -i.bak gradlewStrict $SEP \
  GRADLE_JVM=${JAVA_BUILD_PATH} JAVA_TEST_PATH=${JAVA_TEST_PATH} ./gradlewStrict ${GRADLE_ARGS}'"
echo "${EXEC_COMMAND}"
ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "${EXEC_COMMAND}"
