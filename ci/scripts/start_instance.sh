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



SANITIZED_GEODE_BRANCH=$(echo ${GEODE_BRANCH} | tr "/" "-" | tr '[:upper:]' '[:lower:]')
IMAGE_FAMILY_PREFIX=""

if [[ "${GEODE_FORK}" != "apache" ]]; then
  IMAGE_FAMILY_PREFIX="${GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"
fi

INSTANCE_NAME="$(echo "geode-builder-${BUILD_PIPELINE_NAME}-${BUILD_JOB_NAME}-${BUILD_NAME}" | tr '[:upper:]' '[:lower:]')"
PROJECT=apachegeode-ci
ZONE=us-central1-f
echo "${INSTANCE_NAME}" > "instance-data/instance-name"
echo "${PROJECT}" > "instance-data/project"
echo "${ZONE}" > "instance-data/zone"

echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config
echo "RAM is ${RAM}"
RAM_MEGABYTES=$( expr ${RAM} \* 1024 )
echo "RAM_MEGABYTES is ${RAM_MEGABYTES}"
INSTANCE_INFORMATION=$(gcloud compute --project=${PROJECT} instances create ${INSTANCE_NAME} \
  --zone=${ZONE} \
  --machine-type=custom-${CPUS}-${RAM_MEGABYTES} \
  --min-cpu-platform=Intel\ Skylake \
  --image-family="${IMAGE_FAMILY_PREFIX}geode-builder" \
  --image-project=${PROJECT} \
  --boot-disk-size=100GB \
  --boot-disk-type=pd-ssd \
  --format=json)
CREATE_EXIT_STATUS=$?


while ! gcloud compute --project=${PROJECT} ssh geode@${INSTANCE_NAME} --zone=${ZONE} --ssh-key-file=${SSHKEY_FILE} --quiet -- true; do
  echo -n .
done
echo "${INSTANCE_INFORMATION}" > instance-data/instance-information

INSTANCE_IP_ADDRESS=$(echo ${INSTANCE_INFORMATION} | jq -r '.[].networkInterfaces[0].accessConfigs[0].natIP')
echo "${INSTANCE_IP_ADDRESS}" > "instance-data/instance-ip-address"
