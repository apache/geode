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


echo "Sanitized Geode Fork = ${SANITIZED_GEODE_FORK}"
echo "Sanitized Geode Branch = ${SANITIZED_GEODE_BRANCH}"

MY_NAME=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google")
MY_ZONE=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/zone" -H "Metadata-Flavor: Google")
MY_ZONE=${MY_ZONE##*/}
NETWORK_INTERFACE_INFO="$(gcloud compute instances describe ${MY_NAME} --zone ${MY_ZONE} --format="json(networkInterfaces)")"
GCP_NETWORK=$(echo ${NETWORK_INTERFACE_INFO} | jq -r '.networkInterfaces[0].network')
GCP_NETWORK=${GCP_NETWORK##*/}
GCP_SUBNETWORK=$(echo ${NETWORK_INTERFACE_INFO} | jq -r '.networkInterfaces[0].subnetwork')
GCP_SUBNETWORK=${GCP_SUBNETWORK##*/}
ENV_ID=$(echo ${GCP_NETWORK} | awk -F- '{ print $1}')

#echo "DEBUG INFO *****************************"
#echo "Pipeline prefix = ${PIPELINE_PREFIX}"
#echo "Docker image prefix = ${DOCKER_IMAGE_PREFIX}"
pushd ${SCRIPTDIR} 2>&1 > /dev/null

  cat > repository.yml <<YML
repository:
  project: 'geode'
  fork: ${GEODE_FORK}
  branch: ${GEODE_BRANCH}
  upstream_fork: ${UPSTREAM_FORK}
  public: ${REPOSITORY_PUBLIC}
  sanitized_fork: ${SANITIZED_GEODE_FORK}
YML

  python3 ../render.py jinja.template.yml --variable-file ../shared/jinja.variables.yml repository.yml --environment ../shared/ --output ${SCRIPTDIR}/generated-pipeline.yml || exit 1

popd 2>&1 > /dev/null
cp ${SCRIPTDIR}/generated-pipeline.yml ${OUTPUT_DIRECTORY}/generated-pipeline.yml

grep -n . ${OUTPUT_DIRECTORY}/generated-pipeline.yml

cat > ${OUTPUT_DIRECTORY}/pipeline-vars.yml <<YML
geode-build-branch: ${GEODE_BRANCH}
geode-fork: ${GEODE_FORK}
geode-repo-name: ${GEODE_REPO_NAME}
upstream-fork: ${UPSTREAM_FORK}
pipeline-prefix: "${PIPELINE_PREFIX}"
public-pipelines: ${PUBLIC_PIPELINES}
gcp-project: ${GCP_PROJECT}
artifact-bucket: ${ARTIFACT_BUCKET}
gradle-global-args: ${GRADLE_GLOBAL_ARGS}
semver-prerelease-token: ${SEMVER_PRERELEASE_TOKEN}
concourse-url: ${CONCOURSE_URL}
concourse-team: ${CONCOURSE_TEAM}
YML


