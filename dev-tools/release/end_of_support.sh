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
    echo "Usage: end_of_support.sh -v version_number"
    echo "  -v   The #.# version number of the support branch that is no longer supported"
    exit 1
}

VERSION_MM=""

while getopts ":v:" opt; do
  case ${opt} in
    v )
      VERSION_MM=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${VERSION_MM} == "" ]] ; then
    usage
fi

if [[ $VERSION_MM =~ ^([0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${VERSION_MM}. Example valid version: 1.9"
    exit 1
fi

set -x
WORKSPACE=$PWD/support-${VERSION_MM}-workspace
GEODE=$WORKSPACE/geode
GEODE_EXAMPLES=$WORKSPACE/geode-examples
GEODE_NATIVE=$WORKSPACE/geode-native
GEODE_BENCHMARKS=$WORKSPACE/geode-benchmarks
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
git clone --branch support/${VERSION_MM} git@github.com:apache/geode.git
git clone --branch support/${VERSION_MM} git@github.com:apache/geode-examples.git
git clone --branch support/${VERSION_MM} git@github.com:apache/geode-native.git
git clone --branch support/${VERSION_MM} git@github.com:apache/geode-benchmarks.git
set +x


function failMsg2 {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 65-$(( errln - 1 ))) and try again"
}
trap 'failMsg2 $LINENO' ERR


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
echo "Destroying support branches"
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git checkout develop
    git push origin --delete support/${VERSION_MM}
    git branch -D support/${VERSION_MM}
    set +x
done


echo ""
echo "============================================================"
echo "Done shutting down the support branch!"
echo "============================================================"
echo "Don't forget to remove the JIRA Release placeholder for any future ${VERSION_MM}.x"
echo "Probably also a good idea to announce on the dev list that support/${VERSION_MM} has expired"