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

ROOT_DIR=$(pwd)
BUILD_DATE=$(date +%s)

# Precheckin does not get a geode-build-version
if [ -e "${ROOT_DIR}/geode-build-version" ] ; then
  GEODE_BUILD_VERSION_FILE=${ROOT_DIR}/geode-build-version/number
  GEODE_RESULTS_VERSION_FILE=${ROOT_DIR}/results/number
  GEODE_BUILD_DIR=/tmp/geode-build
  GEODE_PULL_REQUEST_ID_FILE=${ROOT_DIR}/geode/.git/id

  if [ -e "${GEODE_PULL_REQUEST_ID_FILE}" ]; then
    GEODE_PULL_REQUEST_ID=$(cat ${GEODE_PULL_REQUEST_ID_FILE})
    FULL_PRODUCT_VERSION="geode-pr-${GEODE_PULL_REQUEST_ID}"
  else
    # semver resource, e.g., "1.9.0-SNAPSHOT.325"
    CONCOURSE_VERSION=$(cat ${GEODE_BUILD_VERSION_FILE})
    # Prune all after '-', yielding e.g., "1.9.0"
    GEODE_PRODUCT_VERSION=${CONCOURSE_VERSION%%-*}
    # Prune all before '-', yielding e.g., "SNAPSHOT.325"
    CONCOURSE_BUILD_SLUG=${CONCOURSE_VERSION##*-}
    # Prune all before '.', yielding e.g., "SNAPSHOT"
    SNAPSHOT_SLUG=${CONCOURSE_BUILD_SLUG##*.}
    # Prune all before '.', yielding e.g., "325"
    BUILD_ID=$(printf "%04d" ${CONCOURSE_VERSION##*.})

    # Rebuild version, zero-padded
    FULL_PRODUCT_VERSION=${GEODE_PRODUCT_VERSION}-${SNAPSHOT_SLUG}.${BUILD_ID}

    echo "Concourse VERSION is ${CONCOURSE_VERSION}"
    echo "Geode product VERSION is ${GEODE_PRODUCT_VERSION}"
    echo "Full product VERSION is ${FULL_PRODUCT_VERSION}"
    echo "Build ID is ${BUILD_ID}"
  fi

  echo -n "${FULL_PRODUCT_VERSION}" > ${GEODE_RESULTS_VERSION_FILE}
fi

if [[ ${PARALLEL_GRADLE:-"true"} == "true" ]]; then
  PARALLEL_GRADLE="--parallel"
else
  PARALLEL_GRADLE=""
fi
DEFAULT_GRADLE_TASK_OPTIONS="${PARALLEL_GRADLE} --console=plain --no-daemon"
GRADLE_SKIP_TASK_OPTIONS=""

SSHKEY_FILE="instance-data/sshkey"
SSH_OPTIONS="-i ${SSHKEY_FILE} -o ConnectionAttempts=60 -o StrictHostKeyChecking=no -o ServerAliveInterval=60"

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

GRADLE_ARGS="\
    ${DEFAULT_GRADLE_TASK_OPTIONS} \
    ${GRADLE_SKIP_TASK_OPTIONS} \
    ${GRADLE_GLOBAL_ARGS} \
    -Pversion=${FULL_PRODUCT_VERSION} \
    -PbuildId=${BUILD_ID} \
    build install javadoc spotlessCheck rat checkPom resolveDependencies -x test"

EXEC_COMMAND="mkdir -p tmp && cd geode && ${SET_JAVA_HOME} && ./gradlew ${GRADLE_ARGS}"
echo "${EXEC_COMMAND}"
ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "${EXEC_COMMAND}"
