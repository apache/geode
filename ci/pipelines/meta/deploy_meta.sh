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
    --var concourse-team=main \
    --yaml-var public-pipelines=${PUBLIC} 2>&1 |tee flyOutput.log

popd 2>&1 > /dev/null


if [[ "$(tail -n1 flyOutput.log)" == "bailing out" ]]; then
  exit 1
fi

if [[ "${GEODE_FORK}" != "apache" ]]; then
  echo "Disabling unnecessary jobs for forks."
  for job in set set-images set-reaper; do
    (set -x ; fly -t ${TARGET} pause-job -j ${META_PIPELINE}/${job}-pipeline)
  done
else
  echo "Disabling unnecessary jobs for release branches."
  echo "*** DO NOT RE-ENABLE THESE META-JOBS ***"
  for job in set set-pr set-images set-reaper set-metrics set-examples; do
    (set -x ; fly -t ${TARGET} pause-job -j ${META_PIPELINE}/${job}-pipeline)
  done
fi

# bootstrap all precursors of the actual Build job 

function jobStatus {
  PIPELINE=$1
  JOB=$2
  fly jobs -t $TARGET -p $PIPELINE|awk "/$JOB/"'{if($2=="yes")print "paused"; else print $3}'
}

function triggerJob {
  PIPELINE=$1
  JOB=$2
  (set -x ; fly trigger-job -t $TARGET -j $PIPELINE/$JOB)
}

function unpauseJob {
  PIPELINE=$1
  JOB=$2
  (set -x ; fly unpause-job -t $TARGET -j $PIPELINE/$JOB)
}

function unpausePipeline {
  PIPELINE=$1
  (set -x ; fly -t ${TARGET} unpause-pipeline -p $PIPELINE)
}

function awaitJob {
  PIPELINE=$1
  JOB=$2
  echo -n "Waiting for $JOB..."
  status="n/a"
  while [ "$status" = "n/a" ] || [ "$status" = "started" ] ; do
    echo -n .
    sleep 5
    status=$(jobStatus $PIPELINE $JOB)
  done
  echo $status
}

function driveToGreen {
  PIPELINE=$1
  [ "$2" = "--to" ] && FINAL_ONLY=true || FINAL_ONLY=false
  [ "$2" = "--to" ] && shift
  JOB=$2
  status=$(jobStatus $PIPELINE $JOB)
  if [ "paused" = "$status" ] ; then
    unpauseJob $PIPELINE $JOB
    status=$(jobStatus $PIPELINE $JOB)
  fi
  if [ "n/a" = "$status" ] ; then
    [ "$FINAL_ONLY" = "true" ] || triggerJob $PIPELINE $JOB
    awaitJob $PIPELINE $JOB
  elif [ "failed" = "$status" ] ; then
    echo "Unexpected $PIPELINE pipeline status: $JOB $status"
    exit 1
  elif [ "started" = "$status" ] ; then
    awaitJob $PIPELINE $JOB
  elif [ "succeeded" = "$status" ] ; then
    echo "$JOB $status"
    return 0
  else
    echo "Unrecognized job status for $PIPELINE/$JOB: $status"
    exit 1
  fi
}

set -e

unpausePipeline ${META_PIPELINE}
driveToGreen $META_PIPELINE build-meta-mini-docker-image
driveToGreen $META_PIPELINE set-images-pipeline
unpausePipeline ${PIPELINE_PREFIX}images
driveToGreen ${PIPELINE_PREFIX}images --to build-google-geode-builder 
driveToGreen $META_PIPELINE set-pipeline
unpausePipeline ${PIPELINE_PREFIX%-}
echo "Successfully deployed https://concourse.apachegeode-ci.info/teams/main/pipelines/${PIPELINE_PREFIX%-}"