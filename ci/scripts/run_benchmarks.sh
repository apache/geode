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

source concourse-metadata-resource/concourse_metadata
CLUSTER_TAG="${BUILD_PIPELINE_NAME}-${BUILD_JOB_NAME}-${BUILD_NAME}-${BUILD_ID}"
RESULTS_DIR=$(pwd)/results/benchmarks-${CLUSTER_TAG}

CLUSTER_COUNT=4
BENCHMARKS_BRANCH=develop

pushd geode
GEODE_SHA=$(git rev-parse --verify HEAD)
popd

pushd geode-benchmarks/infrastructure/scripts/aws/
./launch_cluster.sh ${CLUSTER_TAG} ${CLUSTER_COUNT}
./run_against_baseline.sh ${CLUSTER_TAG} ${GEODE_SHA} ${BASELINE_BRANCH} ${BENCHMARKS_BRANCH} ${RESULTS_DIR}

popd
