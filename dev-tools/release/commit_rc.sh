#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

usage() {
    echo "Usage: print_rc_email.sh -v version_number -m maven_repo_id"
    echo "  -v   The #.#.#.RC# version number"
    echo "  -m   The 4 digit id of the nexus maven repo"
    exit 1
}

FULL_VERSION=""
MAVEN=""

while getopts ":v:m:" opt; do
  case ${opt} in
    v )
      FULL_VERSION=$OPTARG
      ;;
    m )
      MAVEN=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${FULL_VERSION} == "" ]] || [[ ${MAVEN} == "" ]]; then
    usage
fi

if [[ $FULL_VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+)$ ]]; then
    VERSION=${BASH_REMATCH[1]}
else
    echo "Malformed version number ${FULL_VERSION}. Example valid version: 1.9.0.RC1"
    exit 1
fi

set -x
WORKSPACE=$PWD/release-${VERSION}-workspace
GEODE=$WORKSPACE/geode
GEODE_EXAMPLES=$WORKSPACE/geode-examples
GEODE_NATIVE=$WORKSPACE/geode-native
GEODE_BENCHMARKS=$WORKSPACE/geode-benchmarks
SVN_DIR=$WORKSPACE/dist/dev/geode
set +x

if [ -d "$GEODE" ] && [ -d "$GEODE_EXAMPLES" ] && [ -d "$GEODE_NATIVE" ] && [ -d "$GEODE_BENCHMARKS" ] && [ -d "$SVN_DIR" ] ; then
    true
else
    echo "Please run this script from the same working directory as you initially ran prepare_rc.sh"
    exit 1
fi


function failMsg {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 80-$(( errln - 1 ))) and try again"
}
trap 'failMsg $LINENO' ERR


echo ""
echo "============================================================"
echo "Publishing artifacts to apache release location..."
echo "============================================================"
set -x
cd ${SVN_DIR}
svn commit -m "Releasing Apache Geode ${FULL_VERSION} distribution"
set +x


echo ""
echo "============================================================"
echo "Adding temporary commit for geode-examples to build against staged ${FULL_VERSION}..."
echo "============================================================"
set -x
cd ${GEODE_EXAMPLES}
set +x
sed -e 's#^geodeRepositoryUrl *=.*#geodeRepositoryUrl = https://repository.apache.org/content/repositories/orgapachegeode-'"${MAVEN}#" \
    -e 's#^geodeReleaseUrl *=.*#geodeReleaseUrl = https://dist.apache.org/repos/dist/dev/geode/'"${FULL_VERSION}#" -i.bak gradle.properties
rm gradle.properties.bak
set -x
git add gradle.properties
git diff --staged
git commit -m "temporarily point to staging repo for CI purposes"
git push
set +x


echo ""
echo "============================================================"
echo "Pushing tags..."
echo "============================================================"

for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git push origin rel/v${FULL_VERSION}
    set +x
done


echo ""
echo "============================================================"
echo "Done publishing the release candidate!  Next steps:"
echo "============================================================"
cd ${GEODE}/../..
echo "1. ${0%/*}/deploy_rc_pipeline.sh -v ${VERSION}"
echo "2. Monitor https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-release-${VERSION//./-}-rc until all green"
echo "3. Send the following email to announce the RC:"
echo "To: dev@geode.apache.org"
echo "Subject: [VOTE] Apache Geode ${FULL_VERSION}"
${0%/*}/print_rc_email.sh -v ${FULL_VERSION} -m ${MAVEN}
echo ""
which pbcopy >/dev/null && ${0%/*}/print_rc_email.sh -v ${FULL_VERSION} -m ${MAVEN} | pbcopy && echo "(copied to clipboard)"
