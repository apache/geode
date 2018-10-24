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

SANITIZED_BUILD_PIPELINE_NAME=$(sanitizeName ${BUILD_PIPELINE_NAME})
SANITIZED_BUILD_JOB_NAME=$(sanitizeName ${BUILD_JOB_NAME})
SANITIZED_BUILD_NAME=$(sanitizeName ${BUILD_NAME})
IMAGE_FAMILY_PREFIX=""
WINDOWS_PREFIX=""

if [[ "${SANITIZED_GEODE_FORK}" != "${UPSTREAM_FORK}" ]]; then
  IMAGE_FAMILY_PREFIX="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"
fi

if [[ "${SANITIZED_BUILD_JOB_NAME}" =~ [Ww]indows ]]; then
  WINDOWS_PREFIX="windows-"
fi

ZONE=us-central1-f

INSTANCE_NAME="heavy-lifter-$(uuidgen -n @dns -s -N "${WINDOWS_PREFIX}${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-${SANITIZED_BUILD_PIPELINE_NAME}-${SANITIZED_BUILD_JOB_NAME}-${SANITIZED_BUILD_NAME}")"

MY_NAME=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google")
MY_ZONE=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/zone" -H "Metadata-Flavor: Google")
MY_ZONE=${MY_ZONE##*/}
NETWORK_INTERFACE_INFO="$(gcloud compute instances describe ${MY_NAME} --zone ${MY_ZONE} --format="json(networkInterfaces)")"
GCP_NETWORK=$(echo ${NETWORK_INTERFACE_INFO} | jq -r '.networkInterfaces[0].network')
GCP_NETWORK=${GCP_NETWORK##*/}
GCP_SUBNETWORK=$(echo ${NETWORK_INTERFACE_INFO} | jq -r '.networkInterfaces[0].subnetwork')
GCP_SUBNETWORK=${GCP_SUBNETWORK##*/}


echo "${INSTANCE_NAME}" > "instance-data/instance-name"
echo "${GCP_PROJECT}" > "instance-data/project"
echo "${ZONE}" > "instance-data/zone"

echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config
RAM_MEGABYTES=$( expr ${RAM} \* 1024 )

TTL=$(($(date +%s) + 60 * 60 * 6))
LABELS="instance_type=heavy-lifter,time-to-live=${TTL},job-name=${SANITIZED_BUILD_JOB_NAME},pipeline-name=${SANITIZED_BUILD_PIPELINE_NAME},build-name=${SANITIZED_BUILD_NAME}"

INSTANCE_INFORMATION=$(gcloud compute --project=${GCP_PROJECT} instances create ${INSTANCE_NAME} \
  --zone=${ZONE} \
  --machine-type=custom-${CPUS}-${RAM_MEGABYTES} \
  --min-cpu-platform=Intel\ Skylake \
  --network="${GCP_NETWORK}" \
  --subnet="${GCP_SUBNETWORK}" \
  --image-family="${IMAGE_FAMILY_PREFIX}${WINDOWS_PREFIX}geode-builder" \
  --boot-disk-size=100GB \
  --boot-disk-type=pd-ssd \
  --labels="${LABELS}" \
  --tags="heavy-lifter" \
  --scopes="default,storage-rw" \
  --format=json)

echo "${INSTANCE_INFORMATION}" > instance-data/instance-information

INSTANCE_IP_ADDRESS=$(echo ${INSTANCE_INFORMATION} | jq -r '.[].networkInterfaces[0].networkIP')
echo "${INSTANCE_IP_ADDRESS}" > "instance-data/instance-ip-address"

if [[ -z "${WINDOWS_PREFIX}" ]]; then
  SSH_TIME=$(($(date +%s) + 60))
  echo -n "Attempting to SSH to instance."
  while ! gcloud compute ssh geode@${INSTANCE_NAME} --zone=${ZONE} --internal-ip --ssh-key-file=${SSHKEY_FILE} --quiet -- true; do
    if [[ $(date +%s) > ${SSH_TIME} ]]; then
      echo "error: ssh attempt timeout exceeded. Quitting"
      exit 1
    fi
    echo -n .
    sleep 5
  done
else
  # Set up ssh access for Windows systems
  echo -n "Setting windows password via gcloud."
  while [[ -z "${PASSWORD}" ]]; do
    PASSWORD=$( yes | gcloud beta compute reset-windows-password ${INSTANCE_NAME} --user=geode --zone=${ZONE} --format json | jq -r .password )
    echo -n .
    sleep 5
  done

  ssh-keygen -N "" -f ${SSHKEY_FILE}

  KEY=$( cat ${SSHKEY_FILE}.pub )

  winrm -hostname ${INSTANCE_IP_ADDRESS} -username geode -password "${PASSWORD}" \
    -https -insecure -port 5986 \
    "powershell -command \"&{ mkdir c:\users\geode\.ssh -force; set-content -path c:\users\geode\.ssh\authorized_keys -encoding utf8 -value '${KEY}' }\""
fi
