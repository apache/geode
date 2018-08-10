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

pushd ${SCRIPTDIR}

GEODE_BRANCH=${GEODE_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}
SANITIZED_GEODE_BRANCH=$(echo ${GEODE_BRANCH} | tr "/" "-" | tr '[:upper:]' '[:lower:]')
IMAGE_FAMILY_PREFIX=""
GEODE_DOCKER_IMAGE=${GEODE_DOCKER_IMAGE:-"gcr.io/apachegeode-ci/test-container"}
if [[ -z "${GEODE_FORK}" ]]; then
  echo "GEODE_FORK environment variable must be set for this script to work."
  exit 1
fi


if [[ "${GEODE_FORK}" != "apache" ]]; then
  IMAGE_FAMILY_PREFIX="${GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"
fi

echo "Running packer"
packer build \
  --var "geode_docker_image=${GEODE_DOCKER_IMAGE}" \
  --var "image_family_prefix=${IMAGE_FAMILY_PREFIX}" \
  packer.json
