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
    echo "Usage: commit_rc.sh -j ticket -v version_number -m maven_repo_id"
    echo "  -j   The GEODE-nnnnn Jira identifier for this release"
    echo "  -v   The #.#.#.RC# version number"
    echo "  -m   The 4 digit id of the nexus maven repo"
    exit 1
}

JIRA=""
FULL_VERSION=""
MAVEN=""

while getopts ":j:v:m:" opt; do
  case ${opt} in
    j )
      JIRA=$OPTARG
      ;;
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

if [[ ${JIRA} == "" ]] || [[ ${FULL_VERSION} == "" ]] || [[ ${MAVEN} == "" ]]; then
    usage
fi

if [[ $FULL_VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+)$ ]]; then
    VERSION=${BASH_REMATCH[1]}
else
    echo "Malformed version number ${FULL_VERSION}. Example valid version: 1.9.0.RC1"
    exit 1
fi

VERSION_MM=${VERSION%.*}

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
  echo "Comment out any steps that already succeeded (approximately lines 87-$(( errln - 1 ))) and try again"
}
trap 'failMsg $LINENO' ERR


echo ""
echo "============================================================"
echo "Publishing artifacts to apache release location..."
echo "============================================================"
set -x
cd ${SVN_DIR}
svn commit -m "$JIRA: Release Apache Geode ${FULL_VERSION}

Publish the source, binary, and checksum artifacts to ASF svn server,
from which they will be picked up and published within 15 minutes to
the URLs on https://geode.apache.org/releases/"
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
git diff --staged --color | cat
git commit -m "$JIRA: Set temporary staging repo

This serves two purposes: it gives the RC pipeline a way to get the
nexus staging repo id needed for various tests, and it gives the
Jenkins server a valid configuration during the voting period."
git push
set +x


echo ""
echo "============================================================"
echo "Keeping -build.0 suffix"
echo "============================================================"
cd ${GEODE}/../..
set -x
${0%/*}/set_versions.sh -j $JIRA -v ${VERSION} -s -n -w "${WORKSPACE}"
set +x


echo ""
echo "============================================================"
echo "Pushing copyrights, versions, and tags..."
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git pull -r
    git push -u origin
    git push origin rel/v${FULL_VERSION}
    set +x
done


echo ""
echo "============================================================"
echo "Done publishing the release candidate!  Next steps:"
echo "============================================================"
cd ${GEODE}/../..
echo "1. In a separate terminal window, ${0%/*}/deploy_rc_pipeline.sh -v ${VERSION_MM}"
echo "2. Monitor https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-support-${VERSION_MM//./-}-rc until all green"
echo "3. If you haven't already, add a ${VERSION} section to https://cwiki.apache.org/confluence/display/GEODE/Release+Notes"
jiraverid=$(curl -s 'https://issues.apache.org/jira/secure/ConfigureReleaseNote.jspa?projectId=12318420' | tr -d ' \n' | tr '<' '\n'| awk '/optionvalue.*'$VERSION'$/{sub(/optionvalue="/,"");sub(/">.*/,"");print}')
echo "   The 'full list' link will be https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12318420&version=$jiraverid"
echo "4. Send the following email to announce the RC:"
echo "To: dev@geode.apache.org"
echo "Subject: [VOTE] Apache Geode ${FULL_VERSION}"
${0%/*}/print_rc_email.sh -v ${FULL_VERSION} -m ${MAVEN}
echo ""
which pbcopy >/dev/null && ${0%/*}/print_rc_email.sh -v ${FULL_VERSION} -m ${MAVEN} | pbcopy && echo "(copied to clipboard)"
