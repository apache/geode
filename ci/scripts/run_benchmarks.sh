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

set -e -o pipefail

BASE_DIR=$(pwd)

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

source concourse-metadata-resource/concourse_metadata

CLUSTER_TAG="${BUILD_PIPELINE_NAME}-${BUILD_JOB_NAME}-${BUILD_NAME}-${BUILD_ID}${TAG_POSTFIX}"
RESULTS_DIR=$(pwd)/results/benchmarks-${CLUSTER_TAG}

CLUSTER_COUNT=4
BENCHMARKS_BRANCH=${BENCHMARKS_BRANCH:-develop}

pushd geode
GEODE_SHA=$(git rev-parse --verify HEAD)
popd

pushd geode-benchmarks/infrastructure/scripts/aws/
./launch_cluster.sh -t ${CLUSTER_TAG} -c ${CLUSTER_COUNT} --ci

if [ -z "${BASELINE_VERSION}" ]; then
  ./run_against_baseline.sh -t ${CLUSTER_TAG} -b ${GEODE_SHA} -B ${BASELINE_BRANCH} -e ${BENCHMARKS_BRANCH} -o ${RESULTS_DIR} -m "'source':'geode-ci','benchmark_branch':'${BENCHMARK_BRANCH}','baseline_branch':'${BASELINE_BRANCH}','geode_branch':'${GEODE_SHA}'" --ci -- ${FLAGS} ${TEST_OPTIONS}
else
  ./run_against_baseline.sh -t ${CLUSTER_TAG} -b ${GEODE_SHA} -V ${BASELINE_VERSION} -e ${BENCHMARKS_BRANCH} -o ${RESULTS_DIR} -m "'source':'geode-ci','benchmark_branch':'${BENCHMARK_BRANCH}','baseline_version':'${BASELINE_VERSION}','geode_branch':'${GEODE_SHA}'" --ci -- ${FLAGS} ${TEST_OPTIONS}
fi



popd
