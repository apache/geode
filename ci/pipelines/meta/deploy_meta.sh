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
# ./deploy_meta.sh <repo-fork> <repo-name> <upstream-fork> <concourse-host> <artifact bucket> <public true/false>

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
    echo "to deploy a pipeline for $fork, press x then"
    echo "echo GEODE_FORK=$fork > ${LOCAL_META_PROPERTIES}"
  done
  echo "**************************************************"
fi

read -n 1 -s -r -p "Press any key to continue or x to abort" DEPLOY
echo
if [[ "${DEPLOY}" == "x" ]]; then
  echo "x pressed, aborting deploy."
  exit 0
fi
set -e
set -x

if [[ "${CONCOURSE_HOST}" == "concourse.apachegeode-ci.info" ]]; then
  CONCOURSE_SCHEME=https
fi
CONCOURSE_URL=${CONCOURSE_SCHEME:-"http"}://${CONCOURSE_HOST}
FLY_TARGET=${CONCOURSE_HOST}-${CONCOURSE_TEAM}

. ${SCRIPTDIR}/../shared/utilities.sh
SANITIZED_GEODE_BRANCH=$(getSanitizedBranch ${GEODE_BRANCH})
SANITIZED_GEODE_FORK=$(getSanitizedFork ${GEODE_FORK})

echo "Deploying pipline for ${GEODE_FORK}/${GEODE_BRANCH}"

META_PIPELINE="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-meta"
PIPELINE_PREFIX="${SANITIZED_GEODE_FORK}-${SANITIZED_GEODE_BRANCH}-"

if [[ "${GEODE_FORK}" != "${UPSTREAM_FORK}" ]]; then
  PUBLIC=false
fi

pushd ${SCRIPTDIR} 2>&1 > /dev/null
# Template and output share a directory with this script, but variables are shared in the parent directory.

  cat > repository.yml <<YML
repository:
  project: 'geode'
  fork: ${GEODE_FORK}
  branch: ${GEODE_BRANCH}
  upstream_fork: ${UPSTREAM_FORK}
  public: ${REPOSITORY_PUBLIC}
YML

  cat > pipelineProperties.yml <<YML
pipelineProperties:
  public: ${PUBLIC}
YML

  python3 ../render.py jinja.template.yml --variable-file ../shared/jinja.variables.yml repository.yml pipelineProperties.yml --environment ../shared/ --output ${SCRIPTDIR}/generated-pipeline.yml --debug || exit 1

  set -e
  if [[ ${UPSTREAM_FORK} != "apache" ]]; then
    fly -t ${FLY_TARGET} status || \
    fly -t ${FLY_TARGET} login \
           --team-name ${CONCOURSE_TEAM} \
           --concourse-url=${CONCOURSE_URL}
  fi

  fly -t ${FLY_TARGET} sync
  fly -t ${FLY_TARGET} set-pipeline \
    -p ${META_PIPELINE} \
    --config ${SCRIPTDIR}/generated-pipeline.yml \
    --var artifact-bucket=${ARTIFACT_BUCKET} \
    --var concourse-team=${CONCOURSE_TEAM} \
    --var concourse-host=${CONCOURSE_HOST} \
    --var concourse-url=${CONCOURSE_URL} \
    --var gcp-project=${GCP_PROJECT} \
    --var geode-build-branch=${GEODE_BRANCH} \
    --var geode-fork=${GEODE_FORK} \
    --var geode-repo-name=${GEODE_REPO_NAME} \
    --var gradle-global-args="${GRADLE_GLOBAL_ARGS}" \
    --var maven-snapshot-bucket="${MAVEN_SNAPSHOT_BUCKET}" \
    --var pipeline-prefix=${PIPELINE_PREFIX} \
    --var sanitized-geode-build-branch=${SANITIZED_GEODE_BRANCH} \
    --var sanitized-geode-fork=${SANITIZED_GEODE_FORK} \
    --var semver-prerelease-token="${SEMVER_PRERELEASE_TOKEN}" \
    --var upstream-fork=${UPSTREAM_FORK} \
    --yaml-var public-pipelines=${PUBLIC}
    set +e

popd 2>&1 > /dev/null

# bootstrap all precursors of the actual Build job

function jobStatus {
  PIPELINE=$1
  JOB=$2
  fly jobs -t ${FLY_TARGET} -p ${PIPELINE}|awk "/${JOB}/"'{if($2=="yes")print "paused";else if($4!="n/a")print $4; else print $3}'
}

function triggerJob {
  PIPELINE=$1
  JOB=$2
  (set -x ; fly trigger-job -t ${FLY_TARGET} -j ${PIPELINE}/${JOB})
}

function pauseJob {
  PIPELINE=$1
  JOB=$2
  (set -x ; fly pause-job -t ${FLY_TARGET} -j ${PIPELINE}/${JOB})
}

function pauseJobs {
  PIPELINE=$1
  shift
  for JOB; do
    pauseJob $PIPELINE $JOB
  done
}

function pauseNewJobs {
  PIPELINE=$1
  shift
  for JOB; do
    STATUS="$(jobStatus $PIPELINE $JOB)"
    [[ "$STATUS" == "n/a" ]] && pauseJob $PIPELINE $JOB || true
  done
}

function unpauseJob {
  PIPELINE=$1
  JOB=$2
  (set -x ; fly unpause-job -t ${FLY_TARGET} -j ${PIPELINE}/${JOB})
}

function unpauseJobs {
  PIPELINE=$1
  shift
  for JOB; do
    unpauseJob $PIPELINE $JOB
  done
}

function unpausePipeline {
  PIPELINE=$1
  (set -x ; fly -t ${FLY_TARGET} unpause-pipeline -p ${PIPELINE})
}

function exposePipeline {
  PIPELINE=$1
  (set -x ; fly -t ${FLY_TARGET} expose-pipeline -p ${PIPELINE})
}

function exposePipelines {
  for PIPELINE; do
    exposePipeline $PIPELINE
  done
}

function awaitJob {
  PIPELINE=$1
  JOB=$2
  echo -n "Waiting for ${JOB}..."
  status="n/a"
  while [ "$status" = "n/a" ] || [ "$status" = "pending" ] || [ "$status" = "started" ] ; do
    echo -n .
    sleep 5
    status=$(jobStatus ${PIPELINE} ${JOB})
  done
  echo $status
  [ "$status" = "succeeded" ] || return 1
}

function driveToGreen {
  PIPELINE=$1
  JOB=$2
  status=$(jobStatus ${PIPELINE} ${JOB})
  if [ "paused" = "$status" ] ; then
    unpauseJob ${PIPELINE} ${JOB}
    status=$(jobStatus ${PIPELINE} ${JOB})
  fi
  if [ "aborted" = "$status" ] || [ "failed" = "$status" ] || [ "errored" = "$status" ] ; then
    triggerJob ${PIPELINE} ${JOB}
    awaitJob ${PIPELINE} ${JOB}
  elif [ "n/a" = "$status" ] || [ "pending" = "$status" ] || [ "started" = "$status" ] ; then
    awaitJob ${PIPELINE} ${JOB}
  elif [ "succeeded" = "$status" ] ; then
    echo "${JOB} $status"
    return 0
  else
    echo "Unrecognized job status for ${PIPELINE}/${JOB}: $status"
    exit 1
  fi
}

function enableFeature {
  NAME=$1
  driveToGreen $META_PIPELINE set-$NAME-pipeline
  unpausePipeline ${PIPELINE_PREFIX}$NAME
  exposePipeline ${PIPELINE_PREFIX}$NAME
}

set -e
set +x

if [[ "${GEODE_FORK}" != "${UPSTREAM_FORK}" ]]; then
  echo "Disabling unnecessary jobs for forks."
  pauseJobs ${META_PIPELINE} set-reaper-pipeline
  pauseJobs ${META_PIPELINE} set-mass-test-run-pipeline
  pauseNewJobs ${META_PIPELINE} set-metrics-pipeline
elif [[ "$GEODE_FORK" == "${UPSTREAM_FORK}" ]] && [[ "$GEODE_BRANCH" == "develop" ]]; then
  echo "Disabling optional jobs for develop"
  pauseNewJobs ${META_PIPELINE} set-pr-pipeline set-metrics-pipeline set-examples-pipeline
else
  echo "Disabling unnecessary jobs for support branches."
  echo "*** DO NOT RE-ENABLE THESE META-JOBS ***"
  pauseJobs ${META_PIPELINE} set-images-pipeline set-reaper-pipeline
  pauseNewJobs ${META_PIPELINE} set-pr-pipeline set-metrics-pipeline set-examples-pipeline
fi

unpausePipeline ${META_PIPELINE}
driveToGreen $META_PIPELINE build-meta-mini-docker-image
driveToGreen $META_PIPELINE set-images-pipeline
unpausePipeline ${PIPELINE_PREFIX}images
driveToGreen ${PIPELINE_PREFIX}images build-google-geode-builder
driveToGreen ${PIPELINE_PREFIX}images build-google-windows-geode-builder
driveToGreen $META_PIPELINE set-pipeline
unpausePipeline ${PIPELINE_PREFIX}main

if [[ "$GEODE_FORK" == "${UPSTREAM_FORK}" ]]; then
  if [[ "${PUBLIC}" == "true" ]]; then
    exposePipelines ${PIPELINE_PREFIX}main ${PIPELINE_PREFIX}images
    enableFeature examples
  fi
  if [[ "$GEODE_BRANCH" == "develop" ]]; then
    enableFeature pr
  fi
fi

echo "Successfully deployed ${CONCOURSE_URL}/teams/${CONCOURSE_TEAM}/pipelines/${PIPELINE_PREFIX}main"

rm -f ${SCRIPTDIR}/generated-pipeline.yml
rm -f ${SCRIPTDIR}/pipelineProperties.yml
rm -f ${SCRIPTDIR}/repository.yml
