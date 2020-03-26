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
    echo "  -k   Your 40-digit GPG fingerprint"
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

SIGNING_KEY=$(echo $SIGNING_KEY|sed 's/[^0-9A-Fa-f]//g')
if [[ $SIGNING_KEY =~ ^[0-9A-Fa-f]{40}$ ]]; then
    true
else
    echo "Malformed signing key ${SIGNING_KEY}. Example valid key: '0000 0000 1111 1111 2222  2222 3333 3333 ABCD 1234'"
    exit 1
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
BREW_DIR=$WORKSPACE/homebrew-core
SVN_DIR=$WORKSPACE/dist/dev/geode
set +x

if [ -d "$GEODE" ] && [ -d "$GEODE_EXAMPLES" ] && [ -d "$GEODE_NATIVE" ] && [ -d "$GEODE_BENCHMARKS" ] && [ -d "$BREW_DIR" ] && [ -d "$SVN_DIR" ] ; then
    true
else
    echo "Please run this script from the same working directory as you initially ran prepare_rc.sh"
    exit 1
fi


function failMsg {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 94-$(( errln - 1 ))) and try again"
}
trap 'failMsg $LINENO' ERR


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
    git tag -s -u ${SIGNING_KEY} rel/v${VERSION} -m "Apache Geode v${VERSION} release" rel/v${FULL_VERSION}
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
GEODE_SHA=$(awk '{print $1}' < $WORKSPACE/dist/release/geode/${VERSION}/apache-geode-${VERSION}.tgz.sha256)
set +x
sed -e 's# *url ".*#  url "https://www.apache.org/dyn/closer.cgi?path=geode/'"${VERSION}"'/apache-geode-'"${VERSION}"'.tgz"#' \
    -e '/ *mirror ".*www.*/d' \
    -e '/ *mirror ".*downloads.*/d' \
    -e 's# *mirror ".*archive.*#  mirror "https://archive.apache.org/dist/geode/'"${VERSION}"'/apache-geode-'"${VERSION}"'.tgz"\
  mirror "https://downloads.apache.org/geode/'"${VERSION}"'/apache-geode-'"${VERSION}"'.tgz"#' \
    -e 's/ *sha256 ".*/  sha256 "'"${GEODE_SHA}"'"/' \
    -i.bak apache-geode.rb
rm apache-geode.rb.bak
set -x
git add apache-geode.rb
git diff --staged
git commit -m "apache-geode ${VERSION}"
git push -u myfork
set +x


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
    -i.bak Dockerfile
rm Dockerfile.bak
set -x
git add Dockerfile
git diff --staged
git commit -m "apache-geode ${VERSION}"
git push
set +x


echo ""
echo "============================================================"
echo "Updating Native Dockerfile"
echo "============================================================"
set -x
cd ${GEODE_NATIVE}/docker
git pull -r
set +x
sed -e "/wget.*closer.*apache-geode-/s#http.*filename=geode#https://downloads.apache.org/geode#" \
    -e "/wget.*closer.*apache-rat-/s#http.*filename=creadur#https://archive.apache.org/dist/creadur#" \
    -e "s/^ENV GEODE_VERSION.*/ENV GEODE_VERSION ${VERSION}/" \
    -i.bak Dockerfile
rm Dockerfile.bak
set -x
git add Dockerfile
git diff --staged
git commit -m "apache-geode ${VERSION}"
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
docker build -t apachegeode/geode:latest .
set +x


echo ""
echo "============================================================"
echo "Building Native docker image"
echo "============================================================"
set -x
cd ${GEODE_NATIVE}/docker
docker build .
docker build -t apachegeode/geode-native-build:${VERSION} .
docker build -t apachegeode/geode-native-build:latest .
set +x


echo ""
echo "============================================================"
echo "Publishing Geode docker image"
echo "============================================================"
set -x
cd ${GEODE}/docker
docker login
docker push apachegeode/geode:${VERSION}
docker push apachegeode/geode:latest
set +x


echo ""
echo "============================================================"
echo "Publishing Native docker image"
echo "============================================================"
set -x
cd ${GEODE_NATIVE}/docker
docker push apachegeode/geode-native-build:${VERSION}
docker push apachegeode/geode-native-build:latest
set +x


echo ""
echo "============================================================"
echo "Done promoting release artifacts!"
echo "============================================================"
cd ${GEODE}/../..
echo "Next steps:"
echo "1. Click 'Release' in http://repository.apache.org/ (if you haven't already)"
echo "2. Go to https://github.com/${GITHUB_USER}/homebrew-core/pull/new/apache-geode-${VERSION} and submit the pull request"
echo "3. Validate docker image: docker run -it -p 10334:10334 -p 7575:7575 -p 1099:1099  apachegeode/geode"
echo "4. Bulk-transition JIRA issues fixed in this release to Closed"
echo "5. Wait overnight for apache mirror sites to sync"
echo "6. Confirm that your homebrew PR passed its PR checks and was merged to master"
echo "7. Run ${0%/*}/finalize_release.sh -v ${VERSION}"
