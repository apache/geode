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

. ${SCRIPTDIR}/shared_utilities.sh
is_source_from_pr_testable "geode" "$(get_geode_pr_exclusion_dirs)" "$(get_geode_pr_exclusion_files)" || exit 0

INSTANCE_NAME="$(cat instance-data/instance-name)"
INSTANCE_ID="$(cat instance-data/instance-information | jq -r '.[].id')"
GCP_PROJECT="$(cat instance-data/project)"
INSTANCE_PRICING_JSON="$(gsutil cat gs://${GCP_PROJECT}-infra/pricing/instance_pricing.json)"
PERMITTED_ZONES=($(gcloud compute zones list --filter="name~'us-central.*'" --format=json | jq -r .[].name))

echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config

# Ensure no existing instance with this name in any zone
for KILL_ZONE in $(echo ${PERMITTED_ZONES[*]}); do
  gcloud compute instances delete ${INSTANCE_NAME} --zone=${KILL_ZONE} --quiet &>/dev/null || true
done

while read BLAH ; do
    if [[ -z "${END_TIMESTAMP}" ]]; then
        END_TIMESTAMP=${BLAH}
    else
        START_TIMESTAMP=${BLAH}
    fi
done < <(gcloud logging read "resource.type=gce_instance AND resource.labels.instance_id=${INSTANCE_ID} AND logName=projects/apachegeode-ci/logs/syslog" --format=json | jq -r '.[].timestamp')

START_SECONDS=$(date -d "${START_TIMESTAMP}" +%s)
END_SECONDS=$(date -d "${END_TIMESTAMP}" +%s)
TOTAL_SECONDS=$(expr ${END_SECONDS} - ${START_SECONDS})
FULL_MACHINE_TYPE="$(cat instance-data/instance-information | jq -r '.[].machineType')"
MACHINE_TYPE="${FULL_MACHINE_TYPE##*/}"

case "${MACHINE_TYPE}" in
  custom*)
    CPUS="$(echo "${MACHINE_TYPE}" | rev | cut -d'-' -f 2 | rev)"
    RAM="$(echo "${MACHINE_TYPE}" | rev | cut -d'-' -f 1 | rev)"
    MACHINE_FAMILY="n1"
    CPU_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.custom.cpu")"
    RAM_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.custom.ram")"
    ;;
  *-custom*)
    CPUS="$(echo "${MACHINE_TYPE}" | rev | cut -d'-' -f 2 | rev)"
    RAM="$(echo "${MACHINE_TYPE}" | rev | cut -d'-' -f 1 | rev)"
    MACHINE_FAMILY="$(echo "${MACHINE_TYPE}" | cut -d'-' -f 1)"
    CPU_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.custom.cpu")"
    RAM_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.custom.ram")"
    ;;
  *-standard*)
    CPUS="$(echo "${MACHINE_TYPE}" | rev | cut -d'-' -f 1 | rev)"
    RAM=$(expr ${CPUS} \* 4 )
    MACHINE_FAMILY="$(echo "${MACHINE_TYPE}" | cut -d'-' -f 1)"
    CPU_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.predefined.cpu")"
    RAM_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.predefined.ram")"
    ;;
  *-highcpu*)
    CPUS="$(echo "${MACHINE_TYPE}" | rev | cut -d'-' -f 1 | rev)"
    RAM=${CPUS}
    MACHINE_FAMILY="$(echo "${MACHINE_TYPE}" | cut -d'-' -f 1)"
    CPU_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.predefined.cpu")"
    RAM_COST="$(echo "${INSTANCE_PRICING_JSON}" | jq -r ".[].${MACHINE_FAMILY}.predefined.ram")"
    ;;
  *)
    CPUS=0
    RAM=0
    MACHINE_FAMILY="unknown"
    CPU_COST=0
    RAM_COST=0
    ;;
esac

BUILD_NUMBER="$(cat instance-data/instance-information | jq -r '.[].labels."build-name"')"
JOB_NAME="$(cat instance-data/instance-information | jq -r '.[].labels."job-name"')"
PIPELINE_NAME="$(cat instance-data/instance-information | jq -r '.[].labels."pipeline-name"')"
TOTAL_COST=$(echo "scale = 6; ((${CPUS} * ${CPU_COST}) + (${RAM} * ${RAM_COST})) * ${TOTAL_SECONDS} / 3600" | bc)
echo "Total heavy lifter cost for ${PIPELINE_NAME}/${JOB_NAME} #${BUILD_NUMBER}: $ ${TOTAL_COST}"
cat <<EOF > instance-data/${PIPELINE_NAME}-${JOB_NAME}-${BUILD_NUMBER}.json
{
  "pipeline": "${PIPELINE_NAME}",
  "job": "${JOB_NAME}",
  "machineType": "${MACHINE_TYPE}",
  "cpu": "${CPUS}",
  "ram": "${RAM}",
  "seconds": "${TOTAL_SECONDS}",
  "cpuCost": "${CPU_COST}",
  "ramCost": "${RAM_COST}",
  "totalCost": "${TOTAL_COST}",
}
EOF
cat instance-data/${PIPELINE_NAME}-${JOB_NAME}-${BUILD_NUMBER}.json
