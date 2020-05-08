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

set -ex

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

DEFAULT_GRADLE_TASK_OPTIONS="--console=plain --no-daemon"

SSHKEY_FILE="instance-data/sshkey"
SSH_OPTIONS="-i ${SSHKEY_FILE} -o ConnectionAttempts=60 -o StrictHostKeyChecking=no"

INSTANCE_IP_ADDRESS="$(cat instance-data/instance-ip-address)"

GEODE_VERSION=$(jq -r .semver geode-passing-tokens/*.json)

SET_JAVA_HOME="export JAVA_HOME=/usr/lib/jvm/java-${JAVA_BUILD_VERSION}-openjdk-amd64"

GRADLE_COMMAND="./gradlew \
    ${DEFAULT_GRADLE_TASK_OPTIONS} \
    -PgeodeRepositoryUrl=${MAVEN_SNAPSHOT_BUCKET} \
    -PgeodeVersion=${GEODE_VERSION} \
    clean runAll"

echo "${GRADLE_COMMAND}"
ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "set -x  && mkdir -p tmp && cd geode-examples && ${SET_JAVA_HOME} && ${GRADLE_COMMAND}"
