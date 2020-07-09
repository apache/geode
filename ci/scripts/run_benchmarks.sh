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

set -e -x -o pipefail

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

if [[ ! -z "${PURPOSE}" ]]; then
  PURPOSE_OPTION="-p ${PURPOSE}"
fi

CLUSTER_COUNT=4
BENCHMARKS_BRANCH=${BENCHMARKS_BRANCH:-develop}

GEODE_REPO=${GEODE_REPO:-$(cd geode && git remote get-url origin)}
BENCHMARKS_REPO=${BENCHMARKS_REPO:-$(cd geode-benchmarks && git remote get-url origin)}
BASELINE_REPO=${BASELINE_REPO:-${GEODE_REPO}}

pushd geode
GEODE_SHA=$(git rev-parse --verify HEAD)
popd

input="$(pwd)/results/failedTests"

pushd geode-benchmarks/infrastructure/scripts/aws/
./launch_cluster.sh -t ${CLUSTER_TAG} -c ${CLUSTER_COUNT} ${PURPOSE_OPTION} --ci

# test retry loop - Check if any tests have failed. If so, overwrite the TEST_OPTIONS with only the
# failed tests. Test failures only result in an exit code of 1 when on the last iteration of loop.
for i in {1..5}
do
  echo "This is ITERATION ${i} of benchmarking against baseline."

  if [[ -f ${input} ]]; then
    unset TEST_OPTIONS
    TEST_OPTIONS=""
    while IFS= read -r line; do
      test=" --tests $line"
      TEST_OPTIONS=${TEST_OPTIONS}${test}
    done < ${input}

    rm ${input}
  fi

  if [[ ${i} != 5 ]]; then
    set +e
  fi

  if [ -z "${BASELINE_VERSION}" ]; then
    BASELINE_OPTION="-B ${BASELINE_BRANCH} -R ${BASELINE_REPO}"
    METADATA_BASELINE="'benchmark_branch':'${BASELINE_BRANCH}'"
  else
    BASELINE_OPTION="-V ${BASELINE_VERSION}"
    METADATA_BASELINE="'benchmark_version':'${BASELINE_VERSION}'"
  fi

  ./run_on_cluster.sh -t ${CLUSTER_TAG} -- pkill -9 java
  ./run_on_cluster.sh -t ${CLUSTER_TAG} -- rm /home/geode/locator10334view.dat;
  ./run_against_baseline.sh -t ${CLUSTER_TAG} -b ${GEODE_SHA} -r ${GEODE_REPO} -p ${BENCHMARKS_REPO} ${BASELINE_OPTION} -e ${BENCHMARKS_BRANCH} -o ${RESULTS_DIR} -m "'source':'geode-ci',${METADATA_BASELINE},'baseline_branch':'${BASELINE_BRANCH}','geode_branch':'${GEODE_SHA}'" --ci -- ${FLAGS} ${TEST_OPTIONS}

  if [[ $? -eq 0 ]]; then
    break;
  fi

  if [[ i != 5 ]]; then
    set -e
  fi
done

popd
