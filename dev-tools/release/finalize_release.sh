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
    echo "Usage: finalize_release.sh -v version_number -k your_full_gpg_public_key -g your_github_username"
    echo "  -v   The #.#.# version number to finalize"
    echo "  -k   Your 40 digit GPG fingerprint"
    echo "  -g   Your github username"
    exit 1
}

VERSION=""
SIGNING_KEY=""
GITHUB_USER=""

while getopts ":v:k:g:" opt; do
  case ${opt} in
    v )
      VERSION=$OPTARG
      ;;
    k )
      SIGNING_KEY=$OPTARG
      ;;
    g )
      GITHUB_USER=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${VERSION} == "" ]] || [[ ${SIGNING_KEY} == "" ]] || [[ ${GITHUB_USER} == "" ]] ; then
    usage
fi

SIGNING_KEY=$(echo $SIGNING_KEY|sed 's/[^0-9A-Fa-f]//g')
if [[ $SIGNING_KEY =~ ^[0-9A-Fa-f]{40}$ ]]; then
    true
else
    echo "Malformed signing key ${SIGNING_KEY}. Example valid key: '0000 0000 1111 1111 2222  2222 3333 3333 ABCD 1234'"
    exit 1
fi

if [[ $VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${VERSION}. Example valid version: 1.9.0"
    exit 1
fi

WORKSPACE=$PWD/release-${VERSION}-workspace
GEODE=$WORKSPACE/geode
GEODE_DEVELOP=$WORKSPACE/geode-develop
GEODE_EXAMPLES=$WORKSPACE/geode-examples
GEODE_NATIVE=$WORKSPACE/geode-native
BREW_DIR=$WORKSPACE/homebrew-core
SVN_DIR=$WORKSPACE/dist/dev/geode

if [ -d "$GEODE" ] && [ -d "$GEODE_DEVELOP" ] && [ -d "$GEODE_EXAMPLES" ] && [ -d "$GEODE_NATIVE" ] && [ -d "$BREW_DIR" ] && [ -d "$SVN_DIR" ] ; then
    true
else
    echo "Please run this script from the same working directory as you initially ran prepare_rc.sh"
    exit 1
fi


echo "============================================================"
echo "Updating brew"
echo "============================================================"
cd ${BREW_DIR}/Formula
git pull
git remote add myfork git@github.com:${GITHUB_USER}/homebrew-core.git
if ! git fetch myfork ; then
    echo "Please fork https://github.com/Homebrew/homebrew-core"
    exit 1
fi
git checkout -b apache-geode-${VERSION}
GEODE_SHA=$(awk '{print $1}' < $WORKSPACE/dist/release/geode/${VERSION}/apache-geode-${VERSION}.tgz.sha256)
sed -e 's# *url ".*#  url "https://www.apache.org/dyn/closer.cgi?path=geode/'"${VERSION}"'/apache-geode-'"${VERSION}"'.tgz"#' \
    -e 's/ *sha256 ".*/  sha256 "'"${GEODE_SHA}"'"/' \
    -i.bak apache-geode.rb
rm apache-geode.rb.bak
git add apache-geode.rb
git diff --staged
git commit -m "apache-geode ${VERSION}"
git push -u myfork


echo "============================================================"
echo "Updating Dockerfile"
echo "============================================================"
cd ${GEODE}/docker
sed -e "s/^ENV GEODE_GPG.*/ENV GEODE_GPG ${SIGNING_KEY}/" \
    -e "s/^ENV GEODE_VERSION.*/ENV GEODE_VERSION ${VERSION}/" \
    -e "s/^ENV GEODE_SHA256.*/ENV GEODE_SHA256 ${GEODE_SHA}/" \
    -i.bak Dockerfile
rm Dockerfile.bak
git add Dockerfile
git diff --staged
git commit -m "apache-geode ${VERSION}"
git push


echo "============================================================"
echo "Building docker image"
echo "============================================================"
set -x
cd ${GEODE}/docker
docker build .
docker build -t apachegeode/geode:${VERSION} .
docker build -t apachegeode/geode:latest .
docker login
docker push apachegeode/geode:${VERSION}
docker push apachegeode/geode:latest
set +x


echo "============================================================"
echo "Destroying pipeline"
echo "============================================================"
set -x
cd ${GEODE}
fly -t concourse.apachegeode-ci.info login --concourse-url https://concourse.apachegeode-ci.info/
cd ci/pipelines/meta
./destroy_pipelines.sh
set +x


echo "============================================================"
echo "Removing temporary commit from geode-examples..."
echo "============================================================"
cd ${GEODE_EXAMPLES}
git pull
sed -e 's#^geodeRepositoryUrl *=.*#geodeRepositoryUrl =#' \
    -e 's#^geodeReleaseUrl *=.*#geodeReleaseUrl =#' -i.bak gradle.properties
rm gradle.properties.bak
git add gradle.properties
git diff --staged
git commit -m 'Revert "temporarily point to staging repo for CI purposes"'
git push


echo "============================================================"
echo "Merging to master"
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ; do
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


echo "============================================================"
echo "Destroying release branches"
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ; do
    set -x
    cd ${DIR}
    git push origin --delete release/${VERSION}
    git branch -D release/${VERSION}
    set +x
done


echo "============================================================"
echo "Updating 'old' versions"
echo "============================================================"
cd ${GEODE_DEVELOP}
git pull
#before:
# '1.9.0'].each {
#after:
# '1.9.0',
# '1.10.0'].each {
sed -e "s/].each/,\\
 '${VERSION}'].each/" \
    -i.bak settings.gradle
rm settings.gradle.bak
git add settings.gradle
git diff --staged
git commit -m "add ${VERSION} to old versions"
git push


echo "============================================================"
echo "Removing old versions from mirrors"
echo "============================================================"
cd $WORKSPACE/dist/release/geode
svn update --set-depth immediates
#identify the latest patch release for the latest 2 major.minor releases, remove anything else from mirrors (all releases remain available on non-mirrored archive site)
RELEASES_TO_KEEP=2
ls | awk -F. '/KEYS/{next}{print 1000000*$1+1000*$2+$3,$1"."$2"."$3}'| sort -n | awk '{mm=$2;sub(/\.[^.]*$/,"",mm);V[mm]=$2}END{for(v in V){print V[v]}}'|tail -$RELEASES_TO_KEEP > ../keep
echo Keeping releases: $(cat ../keep)
(ls | grep -v KEYS; cat ../keep ../keep)|sort|uniq -u|while read oldVersion; do
    set -x
    svn rm $oldVersion
    svn commit -m "remove $oldVersion from mirrors (it is still available at http://archive.apache.org/dist/geode)"
    set +x
done
rm ../keep


echo "============================================================"
echo "Done finalizing the release!"
echo "============================================================"
cd ${GEODE}/../..
echo "Don't forget to:"
echo "- Go to https://github.com/${GITHUB_USER}/homebrew-core/pull/new/apache-geode-${VERSION} and submit the pull request"
echo "- Validate docker image: docker run -it -p 10334:10334 -p 7575:7575 -p 1099:1099  apachegeode/geode"
echo "- Update mirror links for old releases that were removed from mirrors"
echo "- Publish documentation to docs site"
echo "- Ask for a volunteer to Update Dependencies"
echo "- Send announce email"
