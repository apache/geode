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

PACKER=${PACKER:-packer}
PACKER_JSON_SCRIPT="$(pwd)/${1}"
PACKER_ARGS="${@:2}"
INTERNAL=${INTERNAL:-false}

PACKERDIR="$( dirname ${PACKER_JSON_SCRIPT} )"

BASE_IMAGE="$(cat base-family/name)"

pushd ${PACKERDIR}

if [[ -n "${CONCOURSE_GCP_KEY}" ]]; then
  dd of=credentials.json <<< "${CONCOURSE_GCP_KEY}"
  export GOOGLE_APPLICATION_CREDENTIALS=${PACKERDIR}/credentials.json
fi

GCP_NETWORK="${GCP_NETWORK:-default}"
GCP_SUBNETWORK="${GCP_SUBNETWORK:-default}"

if [[ -z "${GCP_PROJECT}" ]]; then
  echo "GCP_PROJECT is unset. Cowardly refusing to continue."
  exit 1
fi

HASHED_PIPELINE_PREFIX="i$(uuidgen -n @dns -s -N "${PIPELINE_PREFIX}")-"

echo "Running packer"
PACKER_LOG=1 ${PACKER} build ${PACKER_ARGS} \
  --var "base_image=${BASE_IMAGE}" \
  --var "geode_docker_image=${GEODE_DOCKER_IMAGE}" \
  --var "pipeline_prefix=${PIPELINE_PREFIX}" \
  --var "hashed_pipeline_prefix=${HASHED_PIPELINE_PREFIX}" \
  --var "java_build_version=${JAVA_BUILD_VERSION}" \
  --var "gcp_project=${GCP_PROJECT}" \
  --var "gcp_network=${GCP_NETWORK}" \
  --var "gcp_subnetwork=${GCP_SUBNETWORK}" \
  --var "use_internal_ip=${INTERNAL}" \
  --var "packer_ttl=$(($(date +%s) + 60 * 60 * 12))" \
  ${PACKER_JSON_SCRIPT}
