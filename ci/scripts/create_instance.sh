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

if [[ -z "${IMAGE_FAMILY_NAME}" ]]; then
  echo "IMAGE_FAMILY_NAME environment variable must be set for this script to work."
  exit 1
fi

if [[ -z "${GCP_NETWORK}" ]]; then
  echo "GCP_NETWORK environment variable must be set for this script to work."
  exit 1
fi

if [[ -z "${GCP_SUBNETWORK}" ]]; then
  echo "GCP_SUBNETWORK environment variable must be set for this script to work."
  exit 1
fi

if [[ -z "${GCP_ZONE}" ]]; then
  echo "GCP_ZONE environment variable must be set for this script to work."
  exit 1
fi

. ${SCRIPTDIR}/shared_utilities.sh
is_source_from_pr_testable "geode" "$(get_geode_pr_exclusion_dirs)" "$(get_geode_pr_exclusion_files)" || exit 0

if [[ -d geode ]]; then
  pushd geode

    GEODE_SHA=$(git rev-parse --verify HEAD)
  popd
else
  GEODE_SHA="unknown"
fi

. ${SCRIPTDIR}/../pipelines/shared/utilities.sh
SANITIZED_GEODE_BRANCH=$(getSanitizedBranch ${GEODE_BRANCH})
SANITIZED_GEODE_FORK=$(getSanitizedFork ${GEODE_FORK})

SANITIZED_BUILD_PIPELINE_NAME=$(sanitizeName ${BUILD_PIPELINE_NAME})
SANITIZED_BUILD_JOB_NAME=$(sanitizeName ${BUILD_JOB_NAME})
SANITIZED_BUILD_NAME=$(sanitizeName ${BUILD_NAME})
IMAGE_FAMILY_PREFIX="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"
WINDOWS_PREFIX=""


if [[ "${SANITIZED_BUILD_JOB_NAME}" =~ [Ww]indows ]]; then
  WINDOWS_PREFIX="windows-"
fi

INSTANCE_NAME_STRING="${BUILD_PIPELINE_NAME}-${BUILD_JOB_NAME}-build${JAVA_BUILD_VERSION}-test${JAVA_TEST_VERSION}-job#${BUILD_NAME}"

INSTANCE_NAME="heavy-lifter-$(uuidgen -n @dns -s -N "${INSTANCE_NAME_STRING}")"
echo "Hashed ${INSTANCE_NAME_STRING} (${#INSTANCE_NAME_STRING} chars) -> ${INSTANCE_NAME} (${#INSTANCE_NAME} chars)"
IMAGE_SELF_LINK=$(cat builder-image/output.json | jq -r '.selfLink')
IMAGE_NAME=$(cat builder-image/output.json | jq -r '.name')

if [[ -z "${IMAGE_NAME}" ]]; then
  echo "Unable to determine proper heavy lifter image to use. Aborting!"
  exit 1
fi

MY_ZONE=${GCP_ZONE}
MY_ZONE=${MY_ZONE##*/}

# Determine and store our attempt number
cp old/attempts new/
echo attempt >> new/attempts
attempts=$(cat new/attempts | wc -l)

PERMITTED_ZONES=($(gcloud compute zones list --filter="name~'us-central.*'" --format=json | jq -r .[].name))
if [ $attempts -eq 1 ]; then
  ZONE=${MY_ZONE}
else
  ZONE=${PERMITTED_ZONES[$((${RANDOM} % 4))]}
fi
echo "Deploying to zone ${ZONE}"

# Ensure no existing instance with this name in any zone
for KILL_ZONE in $(echo ${PERMITTED_ZONES[*]}); do
  gcloud compute instances delete ${INSTANCE_NAME} --zone=${KILL_ZONE} --quiet &>/dev/null || true
done

echo "${INSTANCE_NAME}" > "instance-data/instance-name"
echo "${GCP_PROJECT}" > "instance-data/project"
echo "${ZONE}" > "instance-data/zone"

echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config
RAM_MEGABYTES=$( expr ${RAM} \* 1024 )

MACHINE_PREFIX="e2"

if (( ${RAM} > 128 )) || (( ${CPUS} > 32 )); then
  MACHINE_PREFIX="n1"
fi

if (( ${RAM} == ${CPUS} )); then
  MACHINE_TYPE="${MACHINE_PREFIX}-highcpu-${CPUS}"
elif (( ${RAM} / ${CPUS} == 4 )) && (( ${CPUS} <= 96 )); then
  MACHINE_TYPE="${MACHINE_PREFIX}-standard-${CPUS}"
else
  MACHINE_TYPE="${MACHINE_PREFIX}-custom-${CPUS}-${RAM_MEGABYTES}"
fi

TTL=$(($(date +%s) + 60 * 60 * 12))
LABELS="instance_type=heavy-lifter,time-to-live=${TTL},job-name=${SANITIZED_BUILD_JOB_NAME},pipeline-name=${SANITIZED_BUILD_PIPELINE_NAME},build-name=${SANITIZED_BUILD_NAME},sha=${GEODE_SHA}"
echo "Applying the following labels to the instance: ${LABELS}"
echo "Creating the instance with the following type: ${MACHINE_TYPE}"
set +e
INSTANCE_INFORMATION=$(gcloud compute --project=${GCP_PROJECT} instances create ${INSTANCE_NAME} \
  --zone=${ZONE} \
  --machine-type=${MACHINE_TYPE} \
  --network="${GCP_NETWORK}" \
  --subnet="${GCP_SUBNETWORK}" \
  --image="${IMAGE_NAME}" \
  --boot-disk-size="${DISK}" \
  --boot-disk-type=pd-ssd \
  --labels="${LABELS}" \
  --tags="heavy-lifter" \
  --scopes="default,storage-rw" \
 `[[ ${USE_SCRATCH_SSD} == "true" ]] && echo "--local-ssd interface=scsi"` \
  --format=json)

CREATE_RC=$?
set -e

if [[ ${CREATE_RC} -ne 0 ]]; then
  sleep 30
  exit 1
fi

echo "${INSTANCE_INFORMATION}" > instance-data/instance-information

INSTANCE_IP_ADDRESS=$(echo ${INSTANCE_INFORMATION} | jq -r '.[].networkInterfaces[0].networkIP')
INSTANCE_ID=$(echo ${INSTANCE_INFORMATION} | jq -r '.[].id')

echo "Heavy lifter's Instance ID is: ${INSTANCE_ID}"

echo "${INSTANCE_IP_ADDRESS}" > "instance-data/instance-ip-address"
echo "${INSTANCE_ID}" > "instance-data/instance-id"

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
  INSTANCE_SETUP_FINSHED_LINE="GCEInstanceSetup: Instance setup finished"
  SCRAPE_COMMAND_SETUP_FINSHED="gcloud compute instances get-serial-port-output ${INSTANCE_NAME} --zone=${ZONE} | grep \"${INSTANCE_SETUP_FINSHED_LINE}\" | wc -l"

  while true; do
    # Check that the instance agent has started at least 2x (first boot, plus activation)
    # and that the "GCEInstanceSetup" script completed
    echo -n "Waiting for startup scripts and windows activation to complete"
    while [[ 1 -ne $(eval ${SCRAPE_COMMAND_SETUP_FINSHED} 2> /dev/null) ]]; do
      echo -n .
      sleep 5
    done
    echo ""
    # Get a password
    PASSWORD=$( yes | gcloud beta compute reset-windows-password ${INSTANCE_NAME} --user=geode --zone=${ZONE} --format json | jq -r .password )
    if [[ -n "${PASSWORD}" ]]; then
      break;
    fi
  done

  ssh-keygen -N "" -f ${SSHKEY_FILE}

  KEY=$( cat ${SSHKEY_FILE}.pub )

  winrm -hostname ${INSTANCE_IP_ADDRESS} -username geode -password "${PASSWORD}" \
    -https -insecure -port 5986 \
    "powershell -command \"&{\
      \$authPath = (Join-Path \$env:ProgramData -ChildPath 'ssh') \
    ; \$authFile = (Join-Path \$authPath 'administrators_authorized_keys') \
    ; mkdir \$authPath -force\
    ; add-content -path \$authFile -encoding utf8 -value '${KEY}'\
    ; icacls \$authFile /inheritance:r /grant 'SYSTEM:(F)' /grant 'BUILTIN\Administrators:(F)' }\""

  if [[ ${USE_SCRATCH_SSD} == "true" ]]; then
    set +e
    echo "Setting up local scratch SSD on drive Z"
    winrm -hostname ${INSTANCE_IP_ADDRESS} -username geode -password "${PASSWORD}" \
      -https -insecure -port 5986 \
      "powershell -command \"Get-Disk\""

    winrm -hostname ${INSTANCE_IP_ADDRESS} -username geode -password "${PASSWORD}" \
      -https -insecure -port 5986 \
      "powershell -command \"Get-Disk | Where partitionstyle -eq 'raw' | Initialize-Disk -PartitionStyle MBR -PassThru | New-Partition -DriveLetter Z -UseMaximumSize | Format-Volume -FileSystem NTFS -NewFileSystemLabel \“disk2\” -Confirm:\$false\""

    winrm -hostname ${INSTANCE_IP_ADDRESS} -username geode -password "${PASSWORD}" \
      -https -insecure -port 5986 \
      "powershell -command \"Get-PSDrive\""
    set -e
  fi
fi
