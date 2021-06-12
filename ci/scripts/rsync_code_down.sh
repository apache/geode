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

. ${SCRIPTDIR}/shared_utilities.sh
is_source_from_pr_testable "geode" "$(get_geode_pr_exclusion_dirs)" || exit 0

SSHKEY_FILE="instance-data/sshkey"

test -e ${SSHKEY_FILE}
SSH_OPTIONS="-i ${SSHKEY_FILE} -o ConnectTimeout=5 -o ConnectionAttempts=60 -o StrictHostKeyChecking=no"

INSTANCE_IP_ADDRESS="$(cat instance-data/instance-ip-address)"

OUTPUT_DIR=${BASE_DIR}/geode-results

case $ARTIFACT_SLUG in
  windows*)
    JAVA_BUILD_PATH=C:/java8
    del=";"
    ;;
  *)
    JAVA_BUILD_PATH=/usr/lib/jvm/bellsoft-java${JAVA_BUILD_VERSION}-amd64
    del="&&"
    ;;
esac

EXEC_COMMAND="bash -c 'export JAVA_HOME=${JAVA_BUILD_PATH} \
  ${del} cd geode \
  ${del} ./gradlew --no-daemon combineReports \
  ${del} ./gradlew --stop \
  ${del} cd .. \
  ${del} rm -rf .gradle/caches .gradle/wrapper'"

time ssh ${SSH_OPTIONS} geode@${INSTANCE_IP_ADDRESS} "${EXEC_COMMAND}"

time ssh ${SSH_OPTIONS} "geode@${INSTANCE_IP_ADDRESS}" tar -czf - geode .gradle | tar -C "${OUTPUT_DIR}" -zxf -

mv "${OUTPUT_DIR}/.gradle" "${OUTPUT_DIR}/geode/.gradle_logs"

set +x
