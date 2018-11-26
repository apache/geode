#!/usr/local/bin/tini-wrapper /bin/bash

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
set -x

export TERM=${TERM:-dumb}
export BUILDROOT=$(pwd)
export DEST_DIR=${BUILDROOT}/built-geode
export GRADLE_TASK=${1}
export BASE_FILENAME=${2}
export GRADLE_TEST_CATEGORY=${3}
export GEODE_BUILD=${DEST_DIR}/test

printf "\nUsing the following JDK:"
if [ -n "${JAVA_HOME}" ]; then
  ${JAVA_HOME}/bin/java -version
else
  java -version
fi
printf "\n\n"

directories_file=${DEST_DIR}/artifact_directories

echo "GRADLE_TASK = ${GRADLE_TASK}"
echo "BASE_FILENAME = ${BASE_FILENAME}"

DEFAULT_GRADLE_TASK_OPTIONS="--no-daemon -x javadoc -x spotlessCheck"

if [[ -n "${GRADLE_TEST_CATEGORY}" ]]; then
  GRADLE_TASK_OPTIONS="-PtestCategory=${GRADLE_TEST_CATEGORY}"
fi

mkdir -p ${GEODE_BUILD}
if [ -v CALL_STACK_TIMEOUT ]; then
  geode-ci/ci/scripts/capture-call-stacks.sh  ${CALL_STACK_TIMEOUT} &
fi

pushd geode
  tar cf - * | (cd ${GEODE_BUILD}; tar xpf -)
popd

if [[ -n "${PARALLEL_DUNIT}" && "${PARALLEL_DUNIT}" == "true" ]]; then
  PARALLEL_DUNIT="-PparallelDunit"
  if [ -n "${DUNIT_PARALLEL_FORKS}" ]; then
    DUNIT_PARALLEL_FORKS="-PdunitParallelForks=${DUNIT_PARALLEL_FORKS}"
  fi
else
  PARALLEL_DUNIT=""
  DUNIT_PARALLEL_FORKS=""
fi

pushd ${GEODE_BUILD}
  set +e
  echo "Running tests"
  set -x

#    ./gradlew --no-daemon -x javadoc -x spotlessCheck :geode-assembly:acceptanceTest --tests org.apache.geode.management.internal.cli.commands.PutCommandWithJsonTest
  ./gradlew ${PARALLEL_DUNIT} \
      ${DUNIT_PARALLEL_FORKS} \
      ${DUNIT_DOCKER_IMAGE} \
      ${DEFAULT_GRADLE_TASK_OPTIONS} \
      ${GRADLE_TASK_OPTIONS} \
      ${GRADLE_TASK}
  export GRADLE_EXIT_STATUS=$?
  set +x
popd

echo "*************************************************************"
echo "Results information is located in the 'archive-results' task"
echo "*************************************************************"

echo "GRADLE_EXIT_STATUS is ${GRADLE_EXIT_STATUS}"


if [[ "${GRADLE_EXIT_STATUS}" != "0" && "${GRADLE_TASK}" == "test" ]]; then
  error_exit
fi
exit ${GRADLE_EXIT_STATUS}
