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

source ${BASE_DIR}/concourse-metadata-resource/concourse_metadata

SSHKEY_FILE="instance-data/sshkey"

if [[ -z "${GEODE_FORK}" ]]; then
  echo "GEODE_FORK environment variable must be set for this script to work."
  exit 1
fi

if [[ -z "${GEODE_BRANCH}" ]]; then
  echo "GEODE_BRANCH environment variable must be set for this script to work."
  exit 1
fi




. ${SCRIPTDIR}/../pipelines/shared/utilities.sh
SANITIZED_GEODE_BRANCH=$(getSanitizedBranch ${GEODE_BRANCH})
SANITIZED_GEODE_FORK=$(getSanitizedFork ${GEODE_FORK})

SANITIZED_BUILD_PIPELINE_NAME=$(echo ${BUILD_PIPELINE_NAME} | tr "/" "-" | tr '[:upper:]' '[:lower:]')
SANITIZED_BUILD_JOB_NAME=$(echo ${BUILD_JOB_NAME} | tr "/" "-" | tr '[:upper:]' '[:lower:]')
SANITIZED_BUILD_NAME=$(echo ${BUILD_NAME} | tr "/" "-" | tr '[:upper:]' '[:lower:]')
IMAGE_FAMILY_PREFIX=""

if [[ "${SANITIZED_GEODE_FORK}" != "apache" ]]; then
  IMAGE_FAMILY_PREFIX="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"
fi

INSTANCE_NAME="$(echo "${BUILD_PIPELINE_NAME}-${BUILD_JOB_NAME}-${BUILD_NAME}" | tr '[:upper:]' '[:lower:]')"
PROJECT=apachegeode-ci
ZONE=us-central1-f
echo "${INSTANCE_NAME}" > "instance-data/instance-name"
echo "${PROJECT}" > "instance-data/project"
echo "${ZONE}" > "instance-data/zone"

echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config
RAM_MEGABYTES=$( expr ${RAM} \* 1024 )

while true; do
    TTL=$(($(date +%s) + 60 * 60 * 6))
    LABELS="instance_type=heavy-lifter,time-to-live=${TTL},job-name=${SANITIZED_BUILD_JOB_NAME},pipeline-name=${SANITIZED_BUILD_PIPELINE_NAME},build-name=${SANITIZED_BUILD_NAME}"

    set +e
# Try to kill any existing machine before starting
    gcloud compute --project=${PROJECT} instances delete ${INSTANCE_NAME} --zone=${ZONE} --delete-disks=all -q
    INSTANCE_INFORMATION=$(gcloud compute --project=${PROJECT} instances create ${INSTANCE_NAME} \
      --zone=${ZONE} \
      --machine-type=custom-${CPUS}-${RAM_MEGABYTES} \
      --min-cpu-platform=Intel\ Skylake \
      --network="heavy-lifters" \
      --subnet="heavy-lifters" \
      --image-family="${IMAGE_FAMILY_PREFIX}geode-builder" \
      --image-project=${PROJECT} \
      --boot-disk-size=100GB \
      --boot-disk-type=pd-ssd \
      --labels="${LABELS}" \
      --format=json)
    CREATE_EXIT_STATUS=$?
    set -e

    if [ ${CREATE_EXIT_STATUS} -eq 0 ]; then
        break
    fi

    TIMEOUT=60
    echo "Waiting ${TIMEOUT} seconds..."
    sleep ${TIMEOUT}
done

echo "${INSTANCE_INFORMATION}" > instance-data/instance-information

INSTANCE_IP_ADDRESS=$(echo ${INSTANCE_INFORMATION} | jq -r '.[].networkInterfaces[0].accessConfigs[0].natIP')
echo "${INSTANCE_IP_ADDRESS}" > "instance-data/instance-ip-address"

while ! gcloud compute --project=${PROJECT} ssh geode@${INSTANCE_NAME} --zone=${ZONE} --ssh-key-file=${SSHKEY_FILE} --quiet -- true; do
  echo -n .
done
