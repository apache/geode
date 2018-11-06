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
GEODE_PULL_REQUEST_ID_FILE=${ROOT_DIR}/geode/.git/id
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
if [ -z ${SERVICE_ACCOUNT+x} ]; then
  echo "SERVICE_ACCOUNT is unset. Check your pipeline configuration and make sure this script is called properly."
  exit 1
fi

if [ -z ${ARTIFACT_BUCKET+x} ]; then
  echo "ARTIFACT_BUCKET is unset. Check your pipeline configuration and make sure this script is called properly."
  exit 1
fi

if [ -z ${GEODE_BUILD_VERSION_NUMBER+x} ]; then
  echo "gradle.properties does not seem to contain a valid versionNumber. Please check the source tree."
  exit 1
fi

if [ -z "${GEODE_PULL_REQUEST_ID}" ]; then
  CONCOURSE_VERSION=$(cat ${GEODE_BUILD_VERSION_FILE})
  CONCOURSE_PRODUCT_VERSION=${CONCOURSE_VERSION%%-*}
  GEODE_PRODUCT_VERSION=${GEODE_BUILD_VERSION_NUMBER}
  CONCOURSE_BUILD_SLUG=${CONCOURSE_VERSION##*-}
  BUILD_ID=${CONCOURSE_VERSION##*.}
  FULL_PRODUCT_VERSION=${GEODE_PRODUCT_VERSION}-${CONCOURSE_BUILD_SLUG}
  echo "Concourse VERSION is ${CONCOURSE_VERSION}"
  echo "Geode product VERSION is ${GEODE_PRODUCT_VERSION}"
  echo "Build ID is ${BUILD_ID}"
else
  FULL_PRODUCT_VERSION="geode-pr-${GEODE_PULL_REQUEST_ID}"
fi

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
  set -x
  ./gradlew --no-daemon --parallel -PbuildId=${BUILD_ID} publish
  GRADLE_EXIT_STATUS=$?
  set +x
popd

exit ${GRADLE_EXIT_STATUS}
