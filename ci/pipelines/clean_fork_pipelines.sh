#!/usr/bin/env bash
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


set -eu
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

META_PROPERTIES=${SCRIPTDIR}/meta/meta.properties
LOCAL_META_PROPERTIES=${SCRIPTDIR}/meta/meta.properties.local

## Load default properties
echo "**************************************************"
echo "Default Environment variables for this deployment:"
cat ${META_PROPERTIES} | grep -v "^#"
source ${META_PROPERTIES}

## Load local overrides properties file
if [[ -f ${LOCAL_META_PROPERTIES} ]]; then
  echo "Local Environment overrides for this deployment:"
  cat ${LOCAL_META_PROPERTIES} | grep -v "^#"
  source ${LOCAL_META_PROPERTIES}
  echo "**************************************************"
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
GEODE_BRANCH=${1:-${CURRENT_BRANCH}}

. ${SCRIPTDIR}/shared/utilities.sh
SANITIZED_GEODE_BRANCH=$(getSanitizedBranch ${GEODE_BRANCH})
SANITIZED_GEODE_FORK=$(getSanitizedFork ${GEODE_FORK})

if [[ -z "${GEODE_FORK}" ]]; then
  echo "No fork provided!"
  exit 1
fi

if [[ "${GEODE_FORK}" == "apache" ]]; then
  echo "This utility is not for primary pipelines."
  exit 1
fi

TARGET=${CONCOURSE_HOST}-${CONCOURSE_TEAM}
CONCOURSE_URL="https://${CONCOURSE_HOST}"
CONCOURSE_TEAM="${CONCOURSE_TEAM:-main}"

fly -t "${TARGET}" status || \
fly -t "${TARGET}" login \
  --team-name "${CONCOURSE_TEAM}" \
  --concourse-url="${CONCOURSE_URL}"

echo "Fork is ${GEODE_FORK}"
echo "Branch is ${GEODE_BRANCH}"
echo "GCP Project is ${GCP_PROJECT}"

echo "Deleting meta pipeline if it exists..."
META_PIPELINE="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-meta"
fly -t "${TARGET}" destroy-pipeline --non-interactive -p "${META_PIPELINE}"

echo "Deleting images pipeline if it exists..."
IMAGES_PIPELINE="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-images"
fly -t "${TARGET}" destroy-pipeline --non-interactive -p "${IMAGES_PIPELINE}"

echo "Deleting reaper pipeline if it exists..."
REAPER_PIPELINE="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-reaper"
fly -t "${TARGET}" destroy-pipeline --non-interactive -p "${REAPER_PIPELINE}"

echo "Deleting build pipeline if it exists..."
BUILD_PIPELINE="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-main"
fly -t "${TARGET}" destroy-pipeline --non-interactive -p "${BUILD_PIPELINE}"

gcloud --project="${GCP_PROJECT}" container images list | grep "${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}" | while IFS= read -r line; do
  echo "Deleting image: ${line}"
  gcloud --project="${GCP_PROJECT}" container images delete ${line}:latest --quiet
  gcloud --project="${GCP_PROJECT}" container images list-tags ${line} --filter='-tags:*'  --format='get(digest)' | while IFS= read -r line2; do
  echo "Deleting image: ${line2}"
  gcloud --project="${GCP_PROJECT}" container images delete ${line}@${line2} --quiet
  done
done
set -x
gcloud --project="${GCP_PROJECT}" compute images list --filter="family ~ ${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}" --format='value(name)' | while IFS= read -r line; do
  echo "Deleting image: ${line}"
  gcloud --project="${GCP_PROJECT}" compute images delete ${line} --quiet
done

