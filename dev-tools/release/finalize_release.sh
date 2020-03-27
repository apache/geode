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
    echo "Usage: finalize_release.sh -v version_number -g your_github_username"
    echo "  -v   The #.#.# version number to finalize"
    echo "  -g   Your github username"
    exit 1
}

VERSION=""

while getopts ":v:g:" opt; do
  case ${opt} in
    v )
      VERSION=$OPTARG
      ;;
    g )
      GITHUB_USER=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${VERSION} == "" ]] || [[ ${GITHUB_USER} == "" ]] ; then
    usage
fi

if [[ $VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${VERSION}. Example valid version: 1.9.0"
    exit 1
fi

set -x
WORKSPACE=$PWD/release-${VERSION}-workspace
GEODE=$WORKSPACE/geode
GEODE_DEVELOP=$WORKSPACE/geode-develop
GEODE_EXAMPLES=$WORKSPACE/geode-examples
GEODE_NATIVE=$WORKSPACE/geode-native
GEODE_BENCHMARKS=$WORKSPACE/geode-benchmarks
SVN_RELEASE_DIR=$WORKSPACE/dist/release/geode
set +x

if [ -d "$GEODE" ] && [ -d "$GEODE_DEVELOP" ] && [ -d "$GEODE_EXAMPLES" ] && [ -d "$GEODE_NATIVE" ] && [ -d "$GEODE_BENCHMARKS" ] && [ -d "$SVN_RELEASE_DIR" ] ; then
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
echo "Destroying pipelines"
echo "============================================================"
set -x
cd ${0%/*}/../../ci/pipelines/meta
DEVELOP_META=$(pwd)
cd ${GEODE}
fly -t concourse.apachegeode-ci.info-main login --team-name main --concourse-url https://concourse.apachegeode-ci.info/
${DEVELOP_META}/destroy_pipelines.sh
set +x


echo ""
echo "============================================================"
echo "Removing temporary commit from geode-examples..."
echo "============================================================"
set -x
cd ${GEODE_EXAMPLES}
git pull
set +x
sed -e 's#^geodeRepositoryUrl *=.*#geodeRepositoryUrl =#' \
    -e 's#^geodeReleaseUrl *=.*#geodeReleaseUrl =#' -i.bak gradle.properties
rm gradle.properties.bak
set -x
git add gradle.properties
git diff --staged
git commit -m 'Revert "temporarily point to staging repo for CI purposes"'
git push
set +x


echo ""
echo "============================================================"
echo "Merging to master"
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git fetch origin
    git checkout release/${VERSION}
    #this creates a merge commit that will then be ff-merged to master, so word it from that perspective
    git merge -s ours origin/master -m "Replacing master with contents of release/${VERSION}"
    git checkout master
    git merge release/${VERSION}
    git push origin master
    set +x
done


echo ""
echo "============================================================"
echo "Destroying release branches"
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git push origin --delete release/${VERSION}
    git branch -D release/${VERSION}
    set +x
done


echo ""
echo "============================================================"
echo "Updating 'old' versions"
echo "============================================================"
set -x
cd ${GEODE_DEVELOP}
git pull
git remote add myfork git@github.com:${GITHUB_USER}/geode.git || true
git checkout -b add-${VERSION}-to-old-versions
set +x
#before:
# '1.9.0'].each {
#after:
# '1.9.0',
# '1.10.0'].each {
sed -e "s/].each/,\\
 '${VERSION}'].each/" \
    -i.bak settings.gradle
rm settings.gradle.bak
set -x
git add settings.gradle
git diff --staged
git commit -m "add ${VERSION} to old versions"
git push -u myfork
set -x


echo ""
echo "============================================================"
echo "Removing old versions from mirrors"
echo "============================================================"
set -x
cd $SVN_RELEASE_DIR
svn update --set-depth immediates
#identify the latest patch release for "N-2" (the latest 3 major.minor releases), remove anything else from mirrors (all releases remain available on non-mirrored archive site)
RELEASES_TO_KEEP=3
set +x
ls | awk -F. '/KEYS/{next}{print 1000000*$1+1000*$2+$3,$1"."$2"."$3}'| sort -n | awk '{mm=$2;sub(/\.[^.]*$/,"",mm);V[mm]=$2}END{for(v in V){print V[v]}}'|tail -$RELEASES_TO_KEEP > ../keep
echo Keeping releases: $(cat ../keep)
(ls | grep -v KEYS; cat ../keep ../keep)|sort|uniq -u|while read oldVersion; do
    set -x
    svn rm $oldVersion
    svn commit -m "remove $oldVersion from mirrors (it is still available at http://archive.apache.org/dist/geode)"
    set +x
    [ -z "$DID_REMOVE" ] || DID_REMOVE="${DID_REMOVE} and "
    DID_REMOVE="${DID_REMOVE}${oldVersion}"
done
rm ../keep


echo ""
echo "============================================================"
echo "Done finalizing the release!"
echo "============================================================"
cd ${GEODE}/../..
echo "Don't forget to:"
echo "- Go to https://github.com/${GITHUB_USER}/geode/pull/new/add-${VERSION}-to-old-versions and create the pull request"
[ -z "$DID_REMOVE" ] || echo "- Update to non-mirror links for old release $DID_REMOVE that was removed from mirrors"
echo "- Publish documentation to docs site"
[ "${VERSION##*.}" -ne 0 ] || echo "- Ask for a volunteer to begin the chore of updating 3rd-party dependency versions on develop"
echo "- Send announce email"
