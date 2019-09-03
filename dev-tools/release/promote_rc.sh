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
    echo "Usage: promote_rc.sh -v version_number"
    echo "  -v   The #.#.#.RC# version number to ship"
    echo "  -k   Your 8 digit GPG key id (the last 8 digits of your gpg fingerprint)"
    exit 1
}

FULL_VERSION=""
SIGNING_KEY=""

while getopts ":v:k:" opt; do
  case ${opt} in
    v )
      FULL_VERSION=$OPTARG
      ;;
    k )
      SIGNING_KEY=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${FULL_VERSION} == "" ]] || [[ ${SIGNING_KEY} == "" ]]; then
    usage
fi

if [[ $FULL_VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+)$ ]]; then
    VERSION=${BASH_REMATCH[1]}
else
    echo "Malformed version number ${FULL_VERSION}. Example valid version: 1.9.0.RC1"
    exit 1
fi

WORKSPACE=$PWD/release-${VERSION}-workspace
GEODE=$WORKSPACE/geode
GEODE_EXAMPLES=$WORKSPACE/geode-examples
GEODE_NATIVE=$WORKSPACE/geode-native
SVN_DIR=$WORKSPACE/dist/dev/geode

if [ -d "$GEODE" ] && [ -d "$GEODE_EXAMPLES" ] && [ -d "$GEODE_NATIVE" ] && [ -d "$SVN_DIR" ] ; then
    true
else
    echo "Please run this script from the same working directory as you initially ran prepare_rc.sh"
    exit 1
fi


echo "============================================================"
echo "Releasing artifacts to mirror sites..."
echo "(note: must be logged in to svn as a PMC member or this will fail)"
echo "============================================================"
cd ${SVN_DIR}/../..
svn mv dev/geode/${FULL_VERSION} release/geode/${VERSION}
cp dev/geode/KEYS release/geode/KEYS
svn commit -m "Releasing Apache Geode ${VERSION} distribution"


echo "============================================================"
echo "Tagging ${FULL_VERSION} as ${VERSION} and pushing tags..."
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ; do
    cd ${DIR}
    git tag -s -u ${SIGNING_KEY} rel/v${VERSION} -m "Apache Geode v${VERSION} release" rel/v${FULL_VERSION}
    git push origin rel/v${VERSION}
done


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
echo "Done promoting release artifacts!"
echo "============================================================"
cd ${GEODE}/../..
echo "Next steps:"
echo "1. Click 'Release' in http://repository.apache.org/"
echo "2. Transition JIRA issues fixed in this release to Closed"
echo "3. Wait 8-24 hours for apache mirror sites to sync"
echo "4. Run ${0%/*}/finalize-release.sh -v ${VERSION}"
