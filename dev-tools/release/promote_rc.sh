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
    echo "Usage: promote_rc.sh -v version_number -k your_full_gpg_public_key -g your_github_username"
    echo "  -v   The #.#.#.RC# version number to ship"
    echo "  -k   Your 8 digit GPG key id (the last 8 digits of your gpg fingerprint)"
    echo "  -g   Your github username"
    exit 1
}

FULL_VERSION=""
SIGNING_KEY=""
GITHUB_USER=""

while getopts ":v:k:g:" opt; do
  case ${opt} in
    v )
      FULL_VERSION=$OPTARG
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

if [[ ${FULL_VERSION} == "" ]] || [[ ${SIGNING_KEY} == "" ]] || [[ ${GITHUB_USER} == "" ]]; then
    usage
fi

SIGNING_KEY=$(gpg --fingerprint "${SIGNING_KEY}"  | tr -d ' ' | grep "${SIGNING_KEY}" | sed 's/Keyfingerprint=//' | tail -1)

if [[ $SIGNING_KEY =~ ^[0-9A-Fa-f]{40}$ ]]; then
    true
else
    echo "Malformed signing key ${SIGNING_KEY}. Example valid key: ABCD1234"
    exit 1
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
GEODE_DEVELOP=$WORKSPACE/geode-develop
GEODE_EXAMPLES=$WORKSPACE/geode-examples
GEODE_NATIVE=$WORKSPACE/geode-native
GEODE_NATIVE_DEVELOP=$WORKSPACE/geode-native-develop
GEODE_BENCHMARKS=$WORKSPACE/geode-benchmarks
BREW_DIR=$WORKSPACE/homebrew-core
SVN_DIR=$WORKSPACE/dist/dev/geode
set +x

if [ -d "$GEODE" ] && [ -d "$GEODE_DEVELOP" ] && [ -d "$GEODE_EXAMPLES" ] && [ -d "$GEODE_NATIVE" ] && [ -d "$GEODE_NATIVE_DEVELOP" ] && [ -d "$GEODE_BENCHMARKS" ] && [ -d "$BREW_DIR" ] && [ -d "$SVN_DIR" ] ; then
    true
else
    echo "Please run this script from the same working directory as you initially ran prepare_rc.sh"
    exit 1
fi


echo ""
echo "============================================================"
echo "Checking docker..."
echo "============================================================"
if ! docker images >/dev/null ; then
  echo "Make sure docker daemon is running and try again."
  exit 1
fi


function failMsg {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 126-$(( errln - 1 ))) and try again"
}
trap 'failMsg $LINENO' ERR


echo ""
echo "============================================================"
echo "Checking for later versions..."
echo "============================================================"
cd ${GEODE_DEVELOP}
latestnv=$(git tag| grep '^rel/v' | grep -v RC | cut -c6- | egrep '^[0-9]+\.[0-9]+\.[0-9]+$' | awk -F. '{print 1000000*$1+1000*$2+$3,$1"."$2"."$3}' | sort -n | tail -1)
latestn=$(echo $latestnv | awk '{print $1}')
latestv=$(echo $latestnv | awk '{print $2}')
thisre=$(echo $VERSION | awk -F. '{print 1000000*$1+1000*$2+$3}')
if [ $latestn -gt $thisre ] ; then
  LATER=$latestv
  echo "Later version $LATER found; $VERSION will not be merged to master or tagged as 'latest' in docker."
else
  echo "No later versions found; $VERSION will be tagged as 'latest' in docker and merged to master"
fi


echo ""
echo "============================================================"
echo "Releasing artifacts to mirror sites..."
echo "(note: must be logged in to svn as a PMC member or this will fail)"
echo "============================================================"
set -x
cd ${SVN_DIR}/../..
svn update
svn mv dev/geode/${FULL_VERSION} release/geode/${VERSION}
cp dev/geode/KEYS release/geode/KEYS
svn commit -m "Releasing Apache Geode ${VERSION} distribution"
set +x


echo ""
echo "============================================================"
echo "Tagging ${FULL_VERSION} as ${VERSION} and pushing tags..."
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git tag -s -u ${SIGNING_KEY} rel/v${VERSION} -m "Apache Geode v${VERSION} release" rel/v${FULL_VERSION}^{}
    git push origin rel/v${VERSION}
    set +x
done


echo ""
echo "============================================================"
echo "Waiting for artifacts to publish to downloads.apache.org..."
echo "============================================================"
for suffix in "" .asc .sha256 ; do
  file=apache-geode-${VERSION}.tgz
  url=https://downloads.apache.org/geode/${VERSION}/${file}${suffix}
  expectedsize=$(cd ${SVN_DIR}/../../release/geode/${VERSION}; ls -l ${file}${suffix} | awk '{print $5}')
  actualsize=0
  while [ $expectedsize -ne $actualsize ] ; do
    while ! curl -s --output /dev/null --head --fail "$url"; do
      echo -n .
      sleep 3
    done
    actualsize=$(curl -s --head "$url" | grep "Content-Length" | awk '{print $2}' | tr -d '\r')
  done
  echo "$url exists and is correct size"
done


echo ""
echo "============================================================"
GEODE_SHA=$(awk '{print $1}' < $WORKSPACE/dist/release/geode/${VERSION}/apache-geode-${VERSION}.tgz.sha256)
if [ -n "$LATER" ] ; then
  echo "NOT updating brew to avoid overwriting newer version $LATER"
  echo "============================================================"
else
  echo "Updating brew"
  echo "============================================================"
  set -x
  cd ${BREW_DIR}/Formula
  git pull
  git remote add myfork git@github.com:${GITHUB_USER}/homebrew-core.git || true
  if ! git fetch myfork ; then
      echo "Please fork https://github.com/Homebrew/homebrew-core"
      exit 1
  fi
  git checkout -b apache-geode-${VERSION}
  set +x
  sed -e 's# *url ".*#  url "https://www.apache.org/dyn/closer.lua?path=geode/'"${VERSION}"'/apache-geode-'"${VERSION}"'.tgz"#' \
      -e '/ *mirror ".*www.*/d' \
      -e '/ *mirror ".*downloads.*/d' \
      -e 's# *mirror ".*archive.*#  mirror "https://archive.apache.org/dist/geode/'"${VERSION}"'/apache-geode-'"${VERSION}"'.tgz"\
  mirror "https://downloads.apache.org/geode/'"${VERSION}"'/apache-geode-'"${VERSION}"'.tgz"#' \
      -e 's/ *sha256 ".*/  sha256 "'"${GEODE_SHA}"'"/' \
      -i.bak apache-geode.rb
  rm apache-geode.rb.bak
  set -x
  git add apache-geode.rb
  git diff --staged --color | cat
  git commit -m "apache-geode ${VERSION}"
  git push -u myfork
  set +x
fi


echo ""
echo "============================================================"
echo "Updating Geode Dockerfile"
echo "============================================================"
set -x
cd ${GEODE}/docker
git pull -r
set +x
sed -e "s/^ENV GEODE_GPG.*/ENV GEODE_GPG ${SIGNING_KEY}/" \
    -e "s/^ENV GEODE_VERSION.*/ENV GEODE_VERSION ${VERSION}/" \
    -e "s/^ENV GEODE_SHA256.*/ENV GEODE_SHA256 ${GEODE_SHA}/" \
    -e "s/ha.pool.sks-keyservers.net/keyserver.ubuntu.com/" \
    -i.bak Dockerfile
rm Dockerfile.bak
set -x
git add Dockerfile
git diff --staged --color | cat
git commit -m "update Dockerfile to apache-geode ${VERSION}"
git push
set +x


echo ""
echo "============================================================"
echo "Updating Native Dockerfile and other variables"
echo "============================================================"
set -x
cd ${GEODE_NATIVE}
git pull -r
set +x
if [ -r .travis.yml ] ; then
  sed -e "s/geode-native-build:[latest0-9.]*/geode-native-build:${VERSION}/" \
      -i.bak .travis.yml
fi
sed -e "s/GEODE_VERSION=.*/GEODE_VERSION=${VERSION}/" \
    -e "s/^ENV GEODE_VERSION.*/ENV GEODE_VERSION ${VERSION}/" \
    -i.bak $(git grep -l GEODE_VERSION= ; git grep -l 'ENV GEODE_VERSION')
rm $(find . -name '*.bak')
set -x
git add .
git diff --staged --color | cat
git commit -m "update Dockerfile and other variables to apache-geode ${VERSION}"
git push
set +x


echo ""
echo "============================================================"
echo "Building Geode docker image"
echo "============================================================"
set -x
cd ${GEODE}/docker
docker build .
docker build -t apachegeode/geode:${VERSION} .
[ -n "$LATER" ] || docker build -t apachegeode/geode:latest .
set +x


echo ""
echo "============================================================"
echo "Building Native docker image"
echo "============================================================"
set -x
cd ${GEODE_NATIVE}/docker
docker build .
docker build -t apachegeode/geode-native-build:${VERSION} .
[ -n "$LATER" ] || docker build -t apachegeode/geode-native-build:latest .
set +x


echo ""
echo "============================================================"
echo "Publishing Geode docker image"
echo "============================================================"
set -x
cd ${GEODE}/docker
docker login
docker push apachegeode/geode:${VERSION}
[ -n "$LATER" ] || docker push apachegeode/geode:latest
set +x


echo ""
echo "============================================================"
echo "Publishing Native docker image"
echo "============================================================"
set -x
cd ${GEODE_NATIVE}/docker
docker push apachegeode/geode-native-build:${VERSION}
[ -n "$LATER" ] || docker push apachegeode/geode-native-build:latest
set +x


if [ -z "$LATER" ] ; then
  echo ""
  echo "============================================================"
  echo "Setting Geode version for geode-native develop"
  echo "============================================================"
  set -x
  cd ${GEODE_NATIVE_DEVELOP}
  git pull
  git remote add myfork git@github.com:${GITHUB_USER}/geode-native.git || true
  git checkout -b update-to-geode-${VERSION}
  set +x

  sed -e "s/GEODE_VERSION=[0-9.]*/GEODE_VERSION=${VERSION}/" \
      -e "s/^ENV GEODE_VERSION.*/ENV GEODE_VERSION ${VERSION}/" \
      -i.bak $(git grep -l GEODE_VERSION= ; git grep -l 'ENV GEODE_VERSION')

  rm $(find . -name '*.bak')
  set -x
  git add .
  if [ $(git diff --staged | wc -l) -gt 0 ] ; then
    git diff --staged --color | cat
    git commit -m "Bumping Geode version to ${VERSION} for CI"
    git push -u myfork
  fi
  set +x
fi


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
git diff --staged --color | cat
git commit -m 'Revert "temporarily point to staging repo for CI purposes"'
git push
set +x


echo ""
echo "============================================================"
if [ -n "$LATER" ] ; then
  echo "NOT merging to master to avoid overwriting newer version $LATER"
  echo "============================================================"
else
  echo "Merging to master"
  echo "============================================================"
  for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
      set -x
      cd ${DIR}
      git remote set-branches --add origin master
      git fetch origin
      git checkout support/${VERSION_MM}
      git checkout -b release/${VERSION} rel/v${VERSION}
      #this creates a merge commit that will then be ff-merged to master, so word it from that perspective
      git merge -s ours origin/master -m "Replacing master with contents of rel/v${VERSION}"
      git checkout master
      git merge release/${VERSION}
      git push origin master
      set +x
  done
fi


echo ""
echo "============================================================"
echo "Updating 'old' versions and Benchmarks baseline on develop"
echo "============================================================"
set -x
cd ${GEODE_DEVELOP}
git pull
git remote add myfork git@github.com:${GITHUB_USER}/geode.git || true
git checkout -b add-${VERSION}-to-old-versions
set +x
PATCH=${VERSION##*.}
PREV=${VERSION%.*}.$(( PATCH - 1 ))
#add at the end if this is a new minor or a patch to the latest minor, otherwise add after it's predecessor
if [ $PATCH -eq 0 ] || grep -q "'${PREV}'].each" settings.gradle ; then
  #before:
  # '1.9.0'].each {
  #after:
  # '1.9.0',
  # '1.10.0'].each {
  sed -e "s/].each/,\\
 '${VERSION}'].each/" \
    -i.bak settings.gradle
else
  #before:
  # '1.9.0',
  #after:
  # '1.9.0',
  # '1.9.1',
  sed -e "s/'${PREV}'/'${PREV}',\\
 '${VERSION}'/" \
    -i.bak settings.gradle
fi
rm settings.gradle.bak
if [ -z "$LATER" ] ; then
  #also update benchmark baseline for develop to this new minor
  sed -e "s/^  baseline_version:.*/  baseline_version: '${VERSION}'/" \
    -i.bak ci/pipelines/shared/jinja.variables.yml
  rm ci/pipelines/shared/jinja.variables.yml.bak
  BENCHMSG=" and set as Benchmarks baseline"
  set -x
  git add ci/pipelines/shared/jinja.variables.yml
fi
set -x
git add settings.gradle
git diff --staged --color | cat
git commit -m "add ${VERSION} to old versions${BENCHMSG} on develop"
git push -u myfork
set +x


echo ""
echo "============================================================"
echo "Updating 'old' versions on support/$VERSION_MM"
echo "============================================================"
set -x
cd ${GEODE}
git checkout support/${VERSION_MM}
git pull
set +x
#add at the end as this release will always be the latest on this branch
sed -e "s/].each/,\\
 '${VERSION}'].each/" \
  -i.bak settings.gradle
rm settings.gradle.bak
set -x
git add settings.gradle
git diff --staged --color | cat
git commit -m "add ${VERSION} to old versions on support/$VERSION_MM"
git push
set +x


echo ""
echo "============================================================"
echo "Removing old versions from mirrors"
echo "============================================================"
set -x
cd ${SVN_DIR}/../../release/geode
svn update --set-depth immediates
#identify the latest patch release for "N-2" (the latest 3 major.minor releases), remove anything else from mirrors (all releases remain available on non-mirrored archive site)
RELEASES_TO_KEEP=3
set +x
ls | awk -F. '/KEYS/{next}{print 1000000*$1+1000*$2+$3,$1"."$2"."$3}'| sort -n | awk '{mm=$2;sub(/\.[^.]*$/,"",mm);V[mm]=$2}END{for(v in V){print V[v]}}'|tail -$RELEASES_TO_KEEP > ../keep
echo Keeping releases: $(cat ../keep)
rm -f ../did.remove
(ls | grep -v KEYS; cat ../keep ../keep)|sort|uniq -u|while read oldVersion; do
    set -x
    svn rm $oldVersion
    svn commit -m "remove $oldVersion from mirrors (it is still available at http://archive.apache.org/dist/geode)"
    set +x
    [ ! -r ../did.remove ] || echo -n " and " >> ../did.remove
    echo -n $oldVersion >> ../did.remove
done
touch ../did.remove
DID_REMOVE=$(cat ../did.remove)
rm ../keep ../did.remove


echo ""
echo "============================================================"
echo 'Done promoting release artifacts!'
echo "============================================================"
cd ${GEODE}/../..
echo "Next steps:"
echo "1. Click 'Release' in http://repository.apache.org/ (if you haven't already)"
[ -n "$LATER" ] || echo "2. Go to https://github.com/${GITHUB_USER}/homebrew-core/pull/new/apache-geode-${VERSION} and submit the pull request"
echo "3. Go to https://github.com/${GITHUB_USER}/geode/pull/new/add-${VERSION}-to-old-versions and create the pull request"
[ -n "$LATER" ] || echo "3b.Go to https://github.com/${GITHUB_USER}/geode-native/pull/new/update-to-geode-${VERSION} and create the pull request"
[ -n "$LATER" ] && tag=":${VERSION}" || tag=""
echo "4. Validate docker image: docker run -it apachegeode/geode${tag}"
echo "5. Bulk-transition JIRA issues fixed in this release to Closed"
echo "5b.Publish to GitHub (see https://cwiki.apache.org/confluence/display/GEODE/Releasing+Apache+Geode#ReleasingApacheGeode-PublishtoGitHub)"
echo "6. Wait overnight for apache mirror sites and mavencentral to sync"
[ -n "$LATER" ] || echo "7. Confirm that your homebrew PR passed its PR checks and was merged to master"
echo "8. Check that ${VERSION} documentation has been published to https://geode.apache.org/docs/"
[ -z "$DID_REMOVE" ] || DID_REMOVE=" and ${DID_REMOVE} info has been removed"
echo "9. Check that ${VERSION} download info has been published to https://geode.apache.org/releases/${DID_REMOVE}"
MAJOR="${VERSION_MM%.*}"
MINOR="${VERSION_MM#*.}"
PATCH="${VERSION##*.}"
[ "${PATCH}" -ne 0 ] || echo "10. Ask on the dev list for a volunteer to begin the chore of updating 3rd-party dependency versions on develop (see dev-tools/dependencies/README.md)"
M=$(date --date '+18 months' '+%a, %B %d %Y' 2>/dev/null || date -v +9m "+%a, %B %d %Y" 2>/dev/null || echo "18 months from now")
[ "${PATCH}" -ne 0 ] || echo "11. Mark your calendar for $M (assuming we release Geode ${MAJOR}.$((MINOR + 3)) on that day) to run ${0%/*}/end_of_support.sh -v ${VERSION_MM}"
[ "${PATCH}" -ne 0 ] || [ -n "$LATER" ] || echo "12. Log in to https://hub.docker.com/repository/docker/apachegeode/geode and update the latest Dockerfile linktext and url to ${VERSION_MM}"
echo "If there are any support branches between ${VERSION_MM} and develop, manually cherry-pick '${VERSION}' bumps to those branches of geode and geode-native."
echo "Bump support pipeline to ${VERSION_MM}.$(( PATCH + 1 )) by plussing BumpPatch in https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-support-${VERSION_MM//./-}-main?group=Semver%20Management"
echo "Run ${0%/*}/set_versions.sh -v ${VERSION_MM}.$(( PATCH + 1 )) -s"
echo 'Finally, send announce email!'
