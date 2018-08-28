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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
GEODEBUILDDIR="${SCRIPTDIR}/../geode-build"

set -e

if [ -z "${GEODE_BRANCH}" ]; then
  GEODE_BRANCH=$(git rev-parse --abbrev-ref HEAD)
fi

if [ "${GEODE_BRANCH}" = "HEAD" ]; then
  echo "Unable to determine branch for deployment. Quitting..."
  exit 1
fi

SANITIZED_GEODE_BRANCH=$(echo ${GEODE_BRANCH} | tr "/" "-" | tr '[:upper:]' '[:lower:]')

BIN_DIR=${OUTPUT_DIRECTORY}/bin
TMP_DIR=${OUTPUT_DIRECTORY}/tmp
mkdir -p ${BIN_DIR} ${TMP_DIR}
curl -o ${BIN_DIR}/fly "https://concourse.apachegeode-ci.info/api/v1/cli?arch=amd64&platform=linux"
chmod +x ${BIN_DIR}/fly

PATH=${PATH}:${BIN_DIR}

TARGET="geode"

TEAM=${CONCOURSE_TEAM}

if [[ "${GEODE_FORK}" == "apache" ]]; then
  PIPELINE_PREFIX=""
  DOCKER_IMAGE_PREFIX=""
else
  PIPELINE_PREFIX="${GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"
  DOCKER_IMAGE_PREFIX=${PIPELINE_PREFIX}
fi

PIPELINE_NAME="${PIPELINE_PREFIX}images"

#if [[ "${GEODE_BRANCH}" == "develop" ]] || [[ ${GEODE_BRANCH} =~ ^release/* ]]; then
#  TEAM="main"
#fi

fly login -t ${TARGET} -n ${TEAM} -c https://concourse.apachegeode-ci.info -u ${CONCOURSE_USERNAME} -p ${CONCOURSE_PASSWORD}
set -x
fly -t ${TARGET} set-pipeline \
  --non-interactive \
  --pipeline ${PIPELINE_NAME} \
  --config geode-images-pipeline/ci/pipelines/images/images.yml \
  --var concourse-team=${TEAM} \
  --var geode-fork=${GEODE_FORK} \
  --var geode-build-branch=${GEODE_BRANCH} \
  --var docker-image-prefix=${DOCKER_IMAGE_PREFIX} \
  --yaml-var public-pipelines=${PUBLIC_PIPELINES}

