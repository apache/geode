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

if [ ! -e "${GEODE_BUILD_VERSION_FILE}" ]; then
  echo "${GEODE_BUILD_VERSION_FILE} file does not exist. Concourse is probably not configured correctly."
  exit 1
fi
if [ -z ${MAINTENANCE_VERSION+x} ]; then
  echo "MAINTENANCE_VERSION is unset. Check your pipeline configuration and make sure this script is called properly."
  exit 1
fi

EMAIL_SUBJECT="${BUILDROOT}/built-geode/subject"
EMAIL_BODY="${BUILDROOT}/built-geode/body"

# Called by trap when the script is exiting
function error_exit() {
  echo "Geode unit tests completed in pipeline ${PIPELINE_NAME} with non-zero exit code" > $EMAIL_SUBJECT
  echo "Pipeline results can be found at:" >$EMAIL_BODY
  echo "" >>$EMAIL_BODY
  echo "Concourse: \${ATC_EXTERNAL_URL}/teams/\${BUILD_TEAM_NAME}/pipelines/\${BUILD_PIPELINE_NAME}/jobs/\${BUILD_JOB_NAME}/builds/\${BUILD_NAME}" >>$EMAIL_BODY
  echo "" >>$EMAIL_BODY
}

trap error_exit ERR

CONCOURSE_VERSION=$(cat ${GEODE_BUILD_VERSION_FILE})
CONCOURSE_PRODUCT_VERSION=${CONCOURSE_VERSION%%-*}
GEODE_PRODUCT_VERSION=${GEODE_BUILD_VERSION_NUMBER}
CONCOURSE_BUILD_SLUG=${CONCOURSE_VERSION##*-}
BUILD_ID=${CONCOURSE_VERSION##*.}
FULL_PRODUCT_VERSION=${GEODE_PRODUCT_VERSION}-${CONCOURSE_BUILD_SLUG}

echo "Concourse VERSION is ${CONCOURSE_VERSION}"
echo "Geode product VERSION is ${GEODE_PRODUCT_VERSION}"
echo "Build ID is ${BUILD_ID}"


directories_file=${DEST_DIR}/artifact_directories
mkdir -p ${TMPDIR}

echo "TMPDIR = ${TMPDIR}"
echo "GRADLE_TASK = ${GRADLE_TASK}"
echo "BASE_FILENAME = ${BASE_FILENAME}"

gcloud config set account ${SERVICE_ACCOUNT}


export FILENAME=${BASE_FILENAME}-${FULL_PRODUCT_VERSION}.tgz

pushd ${GEODE_BUILD}

  set +e
  ./gradlew combineReports
  find . -type d -name "reports" > ${directories_file}
  find . -type d -name "test-results" >> ${directories_file}
  (find . -type d -name "*Test" | grep "build/[^/]*Test$") >> ${directories_file}
  find . -name "*-progress*txt" >> ${directories_file}
  echo "Collecting the following artifacts..."
  cat ${directories_file}
  echo ""
  tar zcf ${DEST_DIR}/${FILENAME} -T ${directories_file}
popd

ARTIFACTS_DESTINATION="${PUBLIC_BUCKET}/builds/${FULL_PRODUCT_VERSION}"
TEST_RESULTS_DESTINATION="${ARTIFACTS_DESTINATION}/test-results/${GRADLE_TASK}/"
TEST_ARTIFACTS_DESTINATION="${ARTIFACTS_DESTINATION}/test-artifacts/"
FULL_BUILD_ARCHIVE_DESTINATION="${ARTIFACTS_DESTINATION}/geodefiles-${FULL_PRODUCT_VERSION}.tgz"
BUILD_ARTIFACTS_FILENAME=geode-build-artifacts-${FULL_PRODUCT_VERSION}.tgz
BUILD_ARTIFACTS_DESTINATION="${ARTIFACTS_DESTINATION}/${BUILD_ARTIFACTS_FILENAME}"


if [ ! -d "${GEODE_BUILD}/build/reports/combined" ]; then
    echo "No tests exist, compile failed."
    mkdir -p ${GEODE_BUILD}/build/reports/combined
    echo "<html><head><title>No Test Results Were Captured</title></head><body><h1>No Test Results Were Captured</h1></body></html>" > ${GEODE_BUILD}/build/reports/combined/index.html
fi

pushd ${GEODE_BUILD}/build/reports/combined
gsutil -q -m cp -r * gs://${TEST_RESULTS_DESTINATION}
popd

echo ""
printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=  Test Results Website =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
printf "\033[92mhttp://${TEST_RESULTS_DESTINATION}\033[0m\n"
printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
printf "\n"

gsutil cp ${DEST_DIR}/${FILENAME} gs://${TEST_ARTIFACTS_DESTINATION}

printf "\033[92mTest artifacts from this job are available at:\033[0m\n"
printf "\n"
printf "\033[92mhttp://${TEST_ARTIFACTS_DESTINATION}${FILENAME}\033[0m\n"
