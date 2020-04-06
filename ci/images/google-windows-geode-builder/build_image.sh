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
PACKER_ARGS="${*}"
INTERNAL=${INTERNAL:-false}
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

pushd ${SCRIPTDIR}

if [[ -n "${CONCOURSE_GCP_KEY}" ]]; then
  dd of=credentials.json <<< "${CONCOURSE_GCP_KEY}"
  export GOOGLE_APPLICATION_CREDENTIALS=${SCRIPTDIR}/credentials.json
fi

GCP_NETWORK="default"
GCP_SUBNETWORK="default"

MY_NAME=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google")
if [[ -n "${MY_NAME}" ]]; then
  MY_ZONE=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/zone" -H "Metadata-Flavor: Google")
  MY_ZONE=${MY_ZONE##*/}
  NETWORK_INTERFACE_INFO="$(gcloud compute instances describe ${MY_NAME} --zone ${MY_ZONE} --format="json(networkInterfaces)")"
  GCP_NETWORK=$(echo ${NETWORK_INTERFACE_INFO} | jq -r '.networkInterfaces[0].network')
  GCP_NETWORK=${GCP_NETWORK##*/}
  GCP_SUBNETWORK=$(echo ${NETWORK_INTERFACE_INFO} | jq -r '.networkInterfaces[0].subnetwork')
  GCP_SUBNETWORK=${GCP_SUBNETWORK##*/}
  INTERNAL=true
fi

if [[ -z "${GCP_PROJECT}" ]]; then
  echo "GCP_PROJECT is unset. Cowardly refusing to continue."
  exit 1
fi

HASHED_PIPELINE_PREFIX="i$(uuidgen -n @dns -s -N "${PIPELINE_PREFIX}")-"

echo "Running packer"
PACKER_LOG=1 ${PACKER} build ${PACKER_ARGS} \
  --var "base_family=${BASE_FAMILY}" \
  --var "geode_docker_image=${GEODE_DOCKER_IMAGE}" \
  --var "pipeline_prefix=${PIPELINE_PREFIX}" \
  --var "hashed_pipeline_prefix=${HASHED_PIPELINE_PREFIX}" \
  --var "java_build_version=${JAVA_BUILD_VERSION}" \
  --var "gcp_project=${GCP_PROJECT}" \
  --var "gcp_network=${GCP_NETWORK}" \
  --var "gcp_subnetwork=${GCP_SUBNETWORK}" \
  --var "use_internal_ip=${INTERNAL}" \
  --var "packer_ttl=$(($(date +%s) + 60 * 60 * 12))" \
  windows-packer.json
