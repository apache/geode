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
#
# ./destroy_meta.sh

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

META_PROPERTIES=${SCRIPTDIR}/meta.properties
LOCAL_META_PROPERTIES=${SCRIPTDIR}/meta.properties.local

## Load default properties
source ${META_PROPERTIES}
echo "**************************************************"
echo "Default Environment variables for this deployment:"
cat ${SCRIPTDIR}/meta.properties | grep -v "^#"
source ${META_PROPERTIES}
GEODE_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo GEODE_BRANCH=${GEODE_BRANCH}
echo "**************************************************"

## Load local overrides properties file
if [[ -f ${LOCAL_META_PROPERTIES} ]]; then
  echo "Local Environment overrides for this deployment:"
  cat ${SCRIPTDIR}/meta.properties.local
  source ${LOCAL_META_PROPERTIES}
  echo "**************************************************"
else
  git remote -v | awk '/fetch/{sub("/[^/]*$","");sub(".*[/:]","");if($0!="apache")print}' | while read fork; do
    echo "to destroy all pipelines for $fork, press x then"
    echo "echo GEODE_FORK=$fork > ${LOCAL_META_PROPERTIES}"
  done
  echo "**************************************************"
fi

read -n 1 -s -r -p "Press any key to continue or x to abort" DESTROY
echo
if [[ "${DESTROY}" == "x" ]]; then
  echo "x pressed, aborting destroy."
  exit 0
fi

if [[ "${CONCOURSE_HOST}" == "concourse.apachegeode-ci.info" ]]; then
  CONCOURSE_SCHEME=https
fi
CONCOURSE_URL=${CONCOURSE_SCHEME:-"http"}://${CONCOURSE_HOST}
FLY_TARGET=${CONCOURSE_HOST}-${CONCOURSE_TEAM}

. ${SCRIPTDIR}/../shared/utilities.sh
SANITIZED_GEODE_BRANCH=$(getSanitizedBranch ${GEODE_BRANCH})
SANITIZED_GEODE_FORK=$(getSanitizedFork ${GEODE_FORK})

echo "Destroying all pipelines associated with ${GEODE_FORK}/${GEODE_BRANCH}"
echo "Please answer Y to each of the following prompts.  Note that some pipelines may not exist."
META_PIPELINE="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-meta"
PIPELINE_PREFIX="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"

function destroyPipelines {
  for PIPELINE; do
    fly destroy-pipeline -t ${FLY_TARGET} -p ${PIPELINE}
  done
}

destroyPipelines ${PIPELINE_PREFIX}main ${PIPELINE_PREFIX}pr ${PIPELINE_PREFIX}images ${PIPELINE_PREFIX}reaper ${PIPELINE_PREFIX}metrics ${PIPELINE_PREFIX}examples ${PIPELINE_PREFIX}meta ${PIPELINE_PREFIX}rc ${PIPELINE_PREFIX}mass-test-run
echo "Destroyed ${CONCOURSE_URL}/teams/${CONCOURSE_TEAM}/pipelines/${PIPELINE_PREFIX}main and all related pipelines"
