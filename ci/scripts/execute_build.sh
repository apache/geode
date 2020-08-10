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
source ${SCRIPTDIR}/shared_utilities.sh

is_source_from_pr_testable "geode" "$(get_geode_pr_exclusion_dirs)" || exit 0

if [[ -z "${GRADLE_TASK}" ]]; then
  echo "GRADLE_TASK must be set. exiting..."
  exit 1
fi

ROOT_DIR=$(pwd)
BUILD_DATE=$(date +%s)

# Precheckin does not get a geode-build-version
if [ -e "${ROOT_DIR}/geode-build-version" ] ; then
  GEODE_BUILD_VERSION_FILE=${ROOT_DIR}/geode-build-version/number
  GEODE_BUILD_DIR=/tmp/geode-build
  GEODE_PULL_REQUEST_ID_FILE=${ROOT_DIR}/geode/.git/resource/version.json

  if [ -e "${GEODE_PULL_REQUEST_ID_FILE}" ]; then
    GEODE_PULL_REQUEST_ID=$(cat ${GEODE_PULL_REQUEST_ID_FILE} | jq --raw-output '.["pr"]')
    FULL_PRODUCT_VERSION="geode-pr-${GEODE_PULL_REQUEST_ID}"
  else
    CONCOURSE_VERSION=$(cat ${GEODE_BUILD_VERSION_FILE})
    echo "Concourse VERSION is ${CONCOURSE_VERSION}"
    # Rebuild version, zero-padded
    FULL_PRODUCT_VERSION=$(get-full-version ${CONCOURSE_VERSION})
    BUILD_ID=$(get-geode-build-id-padded ${CONCOURSE_VERSION} 2> /dev/null)
  fi
fi

if [[ ${PARALLEL_GRADLE:-"true"} == "true" ]]; then
  PARALLEL_GRADLE="--parallel"
else
  PARALLEL_GRADLE=""
fi
DEFAULT_GRADLE_TASK_OPTIONS="${PARALLEL_GRADLE} --console=plain --no-daemon"
GRADLE_SKIP_TASK_OPTIONS=""

SSHKEY_FILE="instance-data/sshkey"
SSH_OPTIONS="-i ${SSHKEY_FILE} -o ConnectionAttempts=60 -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ServerAliveCountMax=5"

INSTANCE_IP_ADDRESS="$(cat instance-data/instance-ip-address)"


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

SET_JAVA_HOME="export JAVA_HOME=/usr/lib/jvm/java-${JAVA_BUILD_VERSION}-openjdk-amd64"

if [ -v CALL_STACK_TIMEOUT ]; then
  ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "${SET_JAVA_HOME} && tmux new-session -d -s callstacks; tmux send-keys  ~/capture-call-stacks.sh\ ${PARALLEL_DUNIT}\ ${CALL_STACK_TIMEOUT} C-m"
fi

if [ -z "${FULL_PRODUCT_VERSION}" ] ; then
  FULL_PRODUCT_VERSION="0.0.0-UndefinedVersion"
fi

GRADLE_ARGS="\
    ${DEFAULT_GRADLE_TASK_OPTIONS} \
    ${GRADLE_SKIP_TASK_OPTIONS} \
    ${GRADLE_GLOBAL_ARGS} \
    -Pversion=${FULL_PRODUCT_VERSION} \
    -PbuildId=${BUILD_ID} \
    build install javadoc spotlessCheck rat checkPom resolveDependencies pmdMain -x test"

EXEC_COMMAND="mkdir -p tmp \
  && cp geode/ci/scripts/attach_sha_to_branch.sh /tmp/ \
  && /tmp/attach_sha_to_branch.sh geode ${BUILD_BRANCH} \
  && cd geode \
  && ${SET_JAVA_HOME} \
  && ./gradlew ${GRADLE_ARGS}"
echo "${EXEC_COMMAND}"
ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "${EXEC_COMMAND}"
