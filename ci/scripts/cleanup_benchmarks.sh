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

RESULTS_DIR=$(pwd)/results
export PAGER=
pushd geode
GEODE_SHA=$(git rev-parse --verify HEAD)
GEODE_SHA_COMMIT_MESSAGE=$(git log -n 1 ${GEODE_SHA})
popd

source concourse-metadata-resource/concourse_metadata
CLUSTER_TAG="${BUILD_PIPELINE_NAME}-${BUILD_JOB_NAME}-${BUILD_NAME}-${BUILD_ID}${TAG_POSTFIX}"
BENCHMARKS_PREFIX=benchmarks-${CLUSTER_TAG}
BENCHMARKS_ARCHIVE_FILENAME=${BENCHMARKS_PREFIX}.tgz
BENCHMARKS_ARCHIVE_FILE=${RESULTS_DIR}/${BENCHMARKS_ARCHIVE_FILENAME}
BENCHMARKS_ARTIFACTS_DESTINATION="${ARTIFACT_BUCKET}/benchmarks/${BUILD_PIPELINE_NAME}/${GEODE_SHA}"

if [[ "${ARTIFACT_BUCKET}" =~ \. ]]; then
  ARTIFACT_SCHEME="http"
else
  ARTIFACT_SCHEME="gs"
fi

pushd geode-benchmarks/infrastructure/scripts/aws/
./destroy_cluster.sh -t ${CLUSTER_TAG} --ci
popd

pushd geode-benchmarks
  ./infrastructure/scripts/aws/dump_results.sh ${RESULTS_DIR}/benchmarks-*/* | tee ${RESULTS_DIR}/results.txt
popd

pushd ${RESULTS_DIR}
    echo "***** Creating benchmarks archive"
    tar zcf ${BENCHMARKS_ARCHIVE_FILE} *
    echo "***** Copying benchmarks archive to storage"
    gsutil cp ${BENCHMARKS_ARCHIVE_FILE} gs://${BENCHMARKS_ARTIFACTS_DESTINATION}/${BENCHMARKS_ARCHIVE_FILENAME}
    printf "\n"
    printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
    printf "\033[92mThis benchmark run is the result of comparing ${GEODE_SHA} with baseline ${BASELINE_BRANCH:-${BASELINE_VERSION}}\033[0m\n"
    printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- Commit Message =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
    echo "${GEODE_SHA_COMMIT_MESSAGE}"
    printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=-= Benchmark Results URI =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
    printf "\033[92m${ARTIFACT_SCHEME}://${BENCHMARKS_ARTIFACTS_DESTINATION}/${BENCHMARKS_ARCHIVE_FILENAME}\033[0m\n"
    printf "\033[92m=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[0m\n"
    printf "\n"
popd
