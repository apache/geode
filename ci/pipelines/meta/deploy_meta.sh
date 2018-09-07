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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

for cmd in Jinja2 PyYAML; do
  if ! [[ $(pip3 list |grep ${cmd}) ]]; then
    echo "${cmd} must be installed for pipeline deployment to work."
    echo " 'pip3 install ${cmd}'"
    echo ""
    exit 1
  fi
done

export GEODE_BRANCH=$(git rev-parse --abbrev-ref HEAD)
TARGET=geode
export GEODE_FORK=${1:-apache}

. ${SCRIPTDIR}/../shared/utilities.sh
SANITIZED_GEODE_BRANCH=$(getSanitizedBranch ${GEODE_BRANCH})
SANITIZED_GEODE_FORK=$(getSanitizedFork ${GEODE_FORK})

PUBLIC=true

echo "Deploying pipline for ${GEODE_FORK}/${GEODE_BRANCH}"

if [[ "${GEODE_FORK}" == "apache" ]]; then
  META_PIPELINE="meta-${SANITIZED_GEODE_BRANCH}"
  PIPELINE_PREFIX=""
else
  PUBLIC=false
  META_PIPELINE="meta-${GEODE_FORK}-${SANITIZED_GEODE_BRANCH}"
  PIPELINE_PREFIX="${GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"
fi


pushd ${SCRIPTDIR} 2>&1 > /dev/null
# Template and output share a directory with this script, but variables are shared in the parent directory.
  python3 ../render.py $(basename ${SCRIPTDIR}) || exit 1

  fly -t ${TARGET} set-pipeline \
    -p ${META_PIPELINE} \
    --config ${SCRIPTDIR}/generated-pipeline.yml \
    --var geode-build-branch=${GEODE_BRANCH} \
    --var sanitized-geode-build-branch=${SANITIZED_GEODE_BRANCH} \
    --var sanitized-geode-fork=${SANITIZED_GEODE_FORK} \
    --var geode-fork=${GEODE_FORK} \
    --var pipeline-prefix=${PIPELINE_PREFIX} \
    --yaml-var public-pipelines=${PUBLIC} 2>&1 |tee flyOutput.log

popd 2>&1 > /dev/null


if [[ "$(tail -n1 flyOutput.log)" == "bailing out" ]]; then
  exit 1
fi

if [[ "${GEODE_FORK}" != "apache" ]]; then
  echo "Disabling unnecessary jobs for forks."
  set -x
  for job in set set-images set-reaper; do
    fly -t ${TARGET} pause-job \
        -j ${META_PIPELINE}/${job}-pipeline
  done

  fly -t ${TARGET} trigger-job \
      -j ${META_PIPELINE}/build-meta-mini-docker-image
  fly -t ${TARGET} unpause-pipeline \
      -p ${META_PIPELINE}
  set +x
fi
