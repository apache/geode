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


set -e
ROOT_DIR=$(pwd)
BUILD_DATE=$(date +%s)
EMAIL_SUBJECT="results/subject"
EMAIL_BODY="results/body"

GEODE_BUILD_VERSION_FILE=${ROOT_DIR}/geode-build-version/number
GEODE_RESULTS_VERSION_FILE=${ROOT_DIR}/results/number
GEODE_BUILD_VERSION_NUMBER=$(grep "versionNumber *=" geode/gradle.properties | awk -F "=" '{print $2}' | tr -d ' ')
GEODE_BUILD_DIR=/tmp/geode-build

if [ ! -e "${GEODE_BUILD_VERSION_FILE}" ]; then
  echo "${GEODE_BUILD_VERSION_FILE} file does not exist. Concourse is probably not configured correctly."
  exit 1
fi
if [ -z ${MAINTENANCE_VERSION+x} ]; then
  echo "MAINTENANCE_VERSION is unset. Check your pipeline configuration and make sure this script is called properly."
  exit 1
fi
if [ -z ${SERVICE_ACCOUNT+x} ]; then
  echo "SERVICE_ACCOUNT is unset. Check your pipeline configuration and make sure this script is called properly."
  exit 1
fi

if [ -z ${GEODE_BUILD_VERSION_NUMBER+x} ]; then
  echo "gradle.properties does not seem to contain a valid versionNumber. Please check the source tree."
  exit 1
fi

CONCOURSE_VERSION=$(cat ${GEODE_BUILD_VERSION_FILE})
CONCOURSE_PRODUCT_VERSION=${CONCOURSE_VERSION%%-*}
GEODE_PRODUCT_VERSION=${GEODE_BUILD_VERSION_NUMBER}
CONCOURSE_BUILD_SLUG=${CONCOURSE_VERSION##*-}
BUILD_ID=${CONCOURSE_VERSION##*.}
FULL_PRODUCT_VERSION=${GEODE_PRODUCT_VERSION}-${CONCOURSE_BUILD_SLUG}
echo -n "${FULL_PRODUCT_VERSION}" > ${GEODE_RESULTS_VERSION_FILE}

echo "Concourse VERSION is ${CONCOURSE_VERSION}"
echo "Geode product VERSION is ${GEODE_PRODUCT_VERSION}"
echo "Build ID is ${BUILD_ID}"

printf "\nUsing the following JDK:"
java -version
printf "\n\n"

gcloud config set account ${SERVICE_ACCOUNT}

export TERM=${TERM:-dumb}
export DEST_DIR=${ROOT_DIR}/built-geode
export TMPDIR=${DEST_DIR}/tmp
mkdir -p ${TMPDIR}
export BUILD_ARTIFACTS_DIR=${DEST_DIR}/test-artifacts
mkdir -p ${BUILD_ARTIFACTS_DIR}

ln -s ${ROOT_DIR}/geode ${GEODE_BUILD_DIR}

pushd ${GEODE_BUILD_DIR}
set +e
./gradlew --no-daemon -PbuildId=${BUILD_ID} --system-prop "java.io.tmpdir=${TMPDIR}" build
GRADLE_EXIT_STATUS=$?
set -e

popd
ARTIFACTS_DESTINATION="files.apachegeode-ci.info/builds/${FULL_PRODUCT_VERSION}"
TEST_RESULTS_DESTINATION="${ARTIFACTS_DESTINATION}/test-results/"
FULL_BUILD_ARCHIVE_DESTINATION="${ARTIFACTS_DESTINATION}/geodefiles-${FULL_PRODUCT_VERSION}.tgz"
BUILD_ARTIFACTS_FILENAME=geode-build-artifacts-${FULL_PRODUCT_VERSION}.tgz
BUILD_ARTIFACTS_DESTINATION="${ARTIFACTS_DESTINATION}/${BUILD_ARTIFACTS_FILENAME}"

function sendSuccessfulJobEmail {
  echo "Sending job success email"

  cat <<EOF >${EMAIL_SUBJECT}
Build for version ${PRODUCT_VERSION} of Apache Geode succeeded.
EOF

  cat <<EOF >${EMAIL_BODY}
=================================================================================================

The build job for Apache Geode version ${FULL_PRODUCT_VERSION} has completed successfully.


Build artifacts are available at:
http://${BUILD_ARTIFACTS_DESTINATION}

Test results are available at:
http://${TEST_RESULTS_DESTINATION}


=================================================================================================
EOF

}

function sendFailureJobEmail {
  echo "Sending job failure email"

  cat <<EOF >${EMAIL_SUBJECT}
Build for version ${PRODUCT_VERSION} of Apache Geode failed.
EOF

  cat <<EOF >${EMAIL_BODY}
=================================================================================================

The build job for Apache Geode version ${FULL_PRODUCT_VERSION} has failed.


Build artifacts are available at:
http://${BUILD_ARTIFACTS_DESTINATION}

Test results are available at:
http://${TEST_RESULTS_DESTINATION}


Job: \${ATC_EXTERNAL_URL}/teams/\${BUILD_TEAM_NAME}/pipelines/\${BUILD_PIPELINE_NAME}/jobs/\${BUILD_JOB_NAME}/builds/\${BUILD_NAME}

=================================================================================================
EOF

}

if [ ! -d "geode/build/reports/combined" ]; then
    echo "No tests exist, compile failed."
    mkdir -p geode/build/reports/combined
    echo "<html><head><title>No Test Results Were Captured</title></head><body><h1>No Test Results Were Captured</h1></body></html>" > geode/build/reports/combined/index.html
fi

pushd geode/build/reports/combined
gsutil -q -m cp -r * gs://${TEST_RESULTS_DESTINATION}
popd

echo ""
printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=  Test Results Website =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
printf "\033[92mhttp://${TEST_RESULTS_DESTINATION}\033[0m\n"
printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
printf "\n"

tar zcf ${DEST_DIR}/geodefiles-${FULL_PRODUCT_VERSION}.tgz geode
gsutil cp ${DEST_DIR}/geodefiles-${FULL_PRODUCT_VERSION}.tgz gs://${FULL_BUILD_ARCHIVE_DESTINATION}
cp -r geode/geode-assembly/build/distributions ${BUILD_ARTIFACTS_DIR}/
cp -r geode/build/reports/rat ${BUILD_ARTIFACTS_DIR}/
cp -r geode/build/reports/combined ${BUILD_ARTIFACTS_DIR}/

directories_file=${DEST_DIR}/artifact_directories

pushd geode
find . -name "*-progress*txt" >> ${directories_file}
echo "Collecting the following artifacts..."
cat ${directories_file}
echo ""
mkdir -p ${BUILD_ARTIFACTS_DIR}/progress
tar cf - -T ${directories_file} | (cd ${BUILD_ARTIFACTS_DIR}/progress; tar xpf -)
popd

pushd ${BUILD_ARTIFACTS_DIR}
tar zcf ${DEST_DIR}/${BUILD_ARTIFACTS_FILENAME} .
popd
gsutil -q cp ${DEST_DIR}/${BUILD_ARTIFACTS_FILENAME} gs://${BUILD_ARTIFACTS_DESTINATION}
printf "\033[92mBuild artifacts from this job are available at:\033[0m\n"
printf "\n"
printf "\033[92mhttp://${BUILD_ARTIFACTS_DESTINATION}\033[0m\n"

if [ ${GRADLE_EXIT_STATUS} -eq 0 ]; then
    sendSuccessfulJobEmail
else
    sendFailureJobEmail
fi

exit ${GRADLE_EXIT_STATUS}
