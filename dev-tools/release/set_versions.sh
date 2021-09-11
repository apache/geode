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
    echo "Usage: set_versions.sh -v version_number [-s]"
    echo "  -v   The #.#.# version number for the next release"
    echo "  -s   configure examples to use latest snapshot instead of release"
    exit 1
}

FULL_VERSION=""


while getopts ":v:snw:" opt; do
  case ${opt} in
    v )
      VERSION=$OPTARG
      ;;
    s )
      EXAMPLES_USE_SNAPSHOTS=true
      ;;
    n )
      NOPUSH=true
      ;;
    w )
      WORKSPACE="$OPTARG"
      CLEAN=false
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

BUILDSUFFIX="-build.0"
VERSION_MM=${VERSION%.*}

if [ "${EXAMPLES_USE_SNAPSHOTS}" = "true" ] ; then
  GEODEFOREXAMPLES="${VERSION_MM}.+"
else
  GEODEFOREXAMPLES="${VERSION}"
fi

set -x
[ -n "${WORKSPACE}" ] || WORKSPACE=$PWD/release-${VERSION}-workspace
GEODE=$WORKSPACE/geode
GEODE_EXAMPLES=$WORKSPACE/geode-examples
set +x


function failMsg1 {
  echo "ERROR: set_versions script did NOT complete successfully.  Please try again."
}
trap failMsg1 ERR


if [ "${CLEAN}" != "false" ] ; then
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
fi


function failMsg2 {
  errln=$1
  echo "ERROR: set_versions script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 76-$(( errln - 1 ))) and try again"
}
trap 'failMsg2 $LINENO' ERR


echo ""
echo "============================================================"
echo "Setting Geode versions"
echo "============================================================"
set -x
cd ${GEODE}
set +x

#version = 1.13.0-build.0
sed -e "s/^version =.*/version = ${VERSION}${BUILDSUFFIX}/" -i.bak gradle.properties

#  product_version: '1.13'
sed -E \
    -e "s#product_version: '[0-9.]+'#product_version: '${VERSION_MM}'#" \
    -i.bak geode-book/config.yml

#git clone -b branch --depth 1 https://github.com/apache/geode.git geode
sed -e "s#clone -b [ds][evlopurt/0-9.]*#clone -b support/${VERSION_MM}#" \
    -i.bak \
    ci/docker/cache_dependencies.sh \
    ci/images/google-geode-builder/scripts/cache_dependencies.sh

rm -f gradle.properties.bak geode-book/config.yml.bak ci/docker/cache_dependencies.sh.bak ci/images/google-geode-builder/scripts/cache_dependencies.sh.bak
set -x
git add gradle.properties geode-book/config.yml ci/docker/cache_dependencies.sh ci/images/google-geode-builder/scripts/cache_dependencies.sh
if [ $(git diff --staged | wc -l) -gt 0 ] ; then
  git diff --staged --color | cat
  git commit -m "Bumping version to ${VERSION}"
  [ "$NOPUSH" = "true" ] || git push -u origin
fi
set +x


echo ""
echo "============================================================"
echo "Setting geode-examples version"
echo "============================================================"
set -x
cd ${GEODE_EXAMPLES}
git pull
set +x

#version = 1.12.0-build.0
#geodeVersion = 1.12.+
sed -e "s/^version = .*/version = ${VERSION}${BUILDSUFFIX}/" \
    -e "s/^geodeVersion = .*/geodeVersion = ${GEODEFOREXAMPLES}/" \
    -i.bak gradle.properties

rm gradle.properties.bak
set -x
git add .
if [ $(git diff --staged | wc -l) -gt 0 ] ; then
  git diff --staged --color | cat
  git commit -m "Bumping version to ${VERSION}"
  [ "$NOPUSH" = "true" ] || git push -u origin
fi
set +x


echo ""
echo "============================================================"
echo 'Done setting support versions!'
echo "============================================================"
cd ${GEODE}/../..
