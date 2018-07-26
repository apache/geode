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

export TERM=${TERM:-dumb}
export BUILDROOT=$(pwd)
export DEST_DIR=${BUILDROOT}/built-geode
export GRADLE_TASK=${1}
export BASE_FILENAME=${2}
export TMPDIR=${DEST_DIR}/tmp
export GEODE_BUILD=${DEST_DIR}/test
export GEODE_BUILD_VERSION_NUMBER=$(grep "versionNumber *=" geode/gradle.properties | awk -F "=" '{print $2}' | tr -d ' ')

GEODE_BUILD_VERSION_FILE=${BUILDROOT}/geode-build-version/number
GEODE_PULL_REQUEST_ID_FILE=${BUILDROOT}/geode/.git/id
if [ -e "${GEODE_PULL_REQUEST_ID_FILE}" ]; then
  GEODE_PULL_REQUEST_ID=$(cat ${GEODE_PULL_REQUEST_ID_FILE})
fi

if [ ! -e "${GEODE_BUILD_VERSION_FILE}" ] && [ -z "${GEODE_PULL_REQUEST_ID}" ]; then
  echo "${GEODE_BUILD_VERSION_FILE} file does not exist. Concourse is probably not configured correctly."
  exit 1
fi
if [ -z ${MAINTENANCE_VERSION+x} ]; then
  echo "MAINTENANCE_VERSION is unset. Check your pipeline configuration and make sure this script is called properly."
  exit 1
fi

EMAIL_SUBJECT="${BUILDROOT}/built-geode/subject"
EMAIL_BODY="${BUILDROOT}/built-geode/body"

echo "Geode unit tests  '\${BUILD_PIPELINE_NAME}/\${BUILD_JOB_NAME}' took too long to execute" > $EMAIL_SUBJECT
echo "Pipeline results can be found at:" >$EMAIL_BODY
echo "" >>$EMAIL_BODY
echo "Concourse: \${ATC_EXTERNAL_URL}/teams/\${BUILD_TEAM_NAME}/pipelines/\${BUILD_PIPELINE_NAME}/jobs/\${BUILD_JOB_NAME}/builds/\${BUILD_NAME}" >>$EMAIL_BODY
echo "" >>$EMAIL_BODY

# Called by trap when the script is exiting
function error_exit() {
  echo "Geode unit tests completed in '\${BUILD_PIPELINE_NAME}/\${BUILD_JOB_NAME}' with non-zero exit code" > $EMAIL_SUBJECT
  echo "Pipeline results can be found at:" >$EMAIL_BODY
  echo "" >>$EMAIL_BODY
  echo "Concourse: \${ATC_EXTERNAL_URL}/teams/\${BUILD_TEAM_NAME}/pipelines/\${BUILD_PIPELINE_NAME}/jobs/\${BUILD_JOB_NAME}/builds/\${BUILD_NAME}" >>$EMAIL_BODY
  echo "" >>$EMAIL_BODY
}

trap error_exit ERR
if [ -z "${GEODE_PULL_REQUEST_ID}" ]; then
  CONCOURSE_VERSION=$(cat ${GEODE_BUILD_VERSION_FILE})
  CONCOURSE_PRODUCT_VERSION=${CONCOURSE_VERSION%%-*}
  GEODE_PRODUCT_VERSION=${GEODE_BUILD_VERSION_NUMBER}
  CONCOURSE_BUILD_SLUG=${CONCOURSE_VERSION##*-}
  BUILD_ID=${CONCOURSE_VERSION##*.}
  FULL_PRODUCT_VERSION=${GEODE_PRODUCT_VERSION}-${CONCOURSE_BUILD_SLUG}
  echo "Concourse VERSION is ${CONCOURSE_VERSION}"
  echo "Product VERSION is ${FULL_PRODUCT_VERSION}"
  echo "Build ID is ${BUILD_ID}"
else
  FULL_PRODUCT_VERSION="geode-pr-${GEODE_PULL_REQUEST_ID}"
fi

printf "\nUsing the following JDK:"
java -version
printf "\n\n"

directories_file=${DEST_DIR}/artifact_directories
mkdir -p ${TMPDIR}

echo "TMPDIR = ${TMPDIR}"
echo "GRADLE_TASK = ${GRADLE_TASK}"
echo "BASE_FILENAME = ${BASE_FILENAME}"

DOCKER_RESOURCE="docker-geode-build-image"
DOCKER_PIDFILE="/var/run/docker.pid"

if [ -e ${DOCKER_RESOURCE}/rootfs.tar ]; then
  if [ -e /usr/local/bin/initdocker ]; then
    echo "Initializing Docker environment..."
    /usr/local/bin/initdocker || true

    # Stuff like ENV settings don't automatically get imported
    CHANGE=()
    for i in $(jq -r '.env | .[]' ${DOCKER_RESOURCE}/metadata.json); do
      CHANGE+=( $(echo "$i" | awk -F= '{printf("--change \"ENV %s %s\"", $1, $2)}') )
    done

    REPO=$(cat ${DOCKER_RESOURCE}/repository)
    echo "Importing Docker image..."
    eval "docker import ${CHANGE[@]} ${DOCKER_RESOURCE}/rootfs.tar $REPO"
    DUNIT_DOCKER_IMAGE="-PdunitDockerImage=${REPO}"
    echo "Docker initialization complete."
  fi
fi

DEFAULT_GRADLE_TASK_OPTIONS="--no-daemon -x javadoc -x spotlessCheck"

mkdir -p ${GEODE_BUILD}
if [ -v CALL_STACK_TIMEOUT ]; then
  geode-ci/ci/scripts/capture-call-stacks.sh  ${CALL_STACK_TIMEOUT} &
fi

pushd geode
  tar cf - * | (cd ${GEODE_BUILD}; tar xpf -)
popd

export FILENAME=${BASE_FILENAME}-${FULL_PRODUCT_VERSION}.tgz

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
  ./gradlew ${PARALLEL_DUNIT} ${DUNIT_PARALLEL_FORKS} ${DUNIT_DOCKER_IMAGE} \
      --system-prop "java.io.tmpdir=${TMPDIR}" ${DEFAULT_GRADLE_TASK_OPTIONS} ${GRADLE_TASK_OPTIONS} ${GRADLE_TASK}
  export GRADLE_EXIT_STATUS=$?
  set +x
popd

echo "*************************************************************"
echo "Results information is located in the 'archive-results' task"
echo "*************************************************************"

echo "GRADLE_EXIT_STATUS is ${GRADLE_EXIT_STATUS}"


if [ -e ${DOCKER_PIDFILE} ]; then
  kill $(cat ${DOCKER_PIDFILE})
fi

if [[ "${GRADLE_EXIT_STATUS}" != "0" && "${GRADLE_TASK}" == "test" ]]; then
  error_exit
fi
exit ${GRADLE_EXIT_STATUS}
