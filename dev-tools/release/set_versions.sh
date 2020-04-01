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
    echo "Usage: set_versions.sh -v version_number"
    echo "  -v   The #.#.# version number for the next release"
    exit 1
}

FULL_VERSION=""

while getopts ":v:" opt; do
  case ${opt} in
    v )
      VERSION=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${VERSION} == "" ]] ; then
    usage
fi

if ! [[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Malformed version number ${VERSION}. Example valid version: 1.9.0"
    exit 1
fi

VERSION_MM=${VERSION%.*}

set -x
WORKSPACE=$PWD/release-${VERSION}-workspace
GEODE=$WORKSPACE/geode
GEODE_EXAMPLES=$WORKSPACE/geode-examples
set +x


function failMsg1 {
  echo "ERROR: script did NOT complete successfully.  Please try again."
}
trap failMsg1 ERR


echo ""
echo "============================================================"
echo "Cleaning workspace directory..."
echo "============================================================"
set -x
rm -rf $WORKSPACE
mkdir -p $WORKSPACE
cd $WORKSPACE
set +x


echo ""
echo "============================================================"
echo "Cloning repositories..."
echo "============================================================"
set -x
git clone --single-branch --branch support/${VERSION_MM} git@github.com:apache/geode.git
git clone --single-branch --branch support/${VERSION_MM} git@github.com:apache/geode-examples.git
set +x


function failMsg2 {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 63-$(( errln - 1 ))) and try again"
}
trap 'failMsg2 $LINENO' ERR


echo ""
echo "============================================================"
echo "Setting Geode versions and updating expected pom"
echo "============================================================"
set -x
cd ${GEODE}
git pull
set +x

#version = 1.13.0-SNAPSHOT
sed -e "s/^version =.*/version = ${VERSION}/" -i.bak gradle.properties

#SEMVER_PRERELEASE_TOKEN=SNAPSHOT
sed -e 's/^SEMVER_PRERELEASE_TOKEN=.*/SEMVER_PRERELEASE_TOKEN=""/' -i.bak ci/pipelines/meta/meta.properties

#  initial_version: 1.12.0
sed -e "s/^  initial_version:.*/  initial_version: ${VERSION}/" -i.bak ./ci/pipelines/shared/jinja.variables.yml

rm gradle.properties.bak ci/pipelines/meta/meta.properties.bak ci/pipelines/shared/jinja.variables.yml.bak
set -x
git add .
git diff --staged

./gradlew clean
./gradlew build -Dskip.tests=true
./gradlew updateExpectedPom

git commit -a -m "Bumping version to ${VERSION}"
git push -u origin
set +x


echo ""
echo "============================================================"
echo "Setting geode-examples version"
echo "============================================================"
set -x
cd ${GEODE_EXAMPLES}
git pull
set +x

#version = 1.12.0-SNAPSHOT
#geodeVersion = 1.12.0-SNAPSHOT
sed -e "s/^version = .*/version = ${VERSION}/" \
    -e "s/^geodeVersion = .*/geodeVersion = ${VERSION}/" \
    -i.bak gradle.properties

rm gradle.properties.bak
set -x
git add .
git diff --staged
git commit -m "Bumping version to ${VERSION}"
git push -u origin
set +x


echo ""
echo "============================================================"
echo "Done setting support versions!"
echo "============================================================"
cd ${GEODE}/../..
PATCH=${VERSION##*.}
[ $PATCH -eq 0 ] || echo "Bump support pipeline to ${VERSION} by plussing BumpPatch in https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-support-${VERSION_MM//./-}-main?group=Semver%20Management"
echo "That's it for now.  Once all needed fixes have been proposed and cherry-picked to support/${VERSION_MM}, come back and run ${0%/*}/prepare_rc.sh -v ${VERSION}.RC1"
