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
SIGNING_KEY=""

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

if [[ $FULL_VERSION =~ ([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+) ]]; then
    VERSION=${BASH_REMATCH[1]}
    RC=${BASH_REMATCH[2]}
else
    echo "Malformed version number ${FULL_VERSION}. Example valid number - 1.9.0.RC1"
    exit 1
fi

GEODE=$PWD/build/geode
GEODE_EXAMPLES=$PWD/build/geode-examples
GEODE_NATIVE=$PWD/build/geode-native
SVN_DIR=$PWD/build/dist/dev/geode

echo "============================================================"
echo "Publishing artifacts to apache release location..."
echo "============================================================"
cd ${SVN_DIR}
svn commit -m "Releasing Apache Geode ${FULL_VERSION} distribution"


echo "============================================================"
echo "Pushing tags..."
echo "============================================================"

cd ${GEODE}
git push origin rel/v${FULL_VERSION}
cd ${GEODE_EXAMPLES}
git push origin rel/v${FULL_VERSION}
cd ${GEODE_NATIVE}
git push origin rel/v${FULL_VERSION}


echo "============================================================"
echo "Adding temporary commit for geode-examples to build against staged ${FULL_VERSION}..."
echo "============================================================"
cd ${GEODE_EXAMPLES}
sed -e 's#^geodeRepositoryUrl *=.*#geodeRepositoryUrl = https://repository.apache.org/content/repositories/orgapachegeode-'"${MAVEN}#" \
    -e 's#^geodeReleaseUrl *=.*#geodeReleaseUrl = https://dist.apache.org/repos/dist/dev/geode/'"${FULL_VERSION}#" -i.bak gradle.properties
rm gradle.properties.bak
git add gradle.properties
git diff --staged
git commit -m "temporarily point to staging repo for CI purposes"
git push


echo "============================================================"
echo "Done publishing the release!  Send the email below to announce it:"
echo "============================================================"
echo "To: dev@geode.apache.org"
echo "Subject: [VOTE] Apache Geode ${FULL_VERSION}"
cd ${GEODE}/../..
${0%/*}/print_rc_email.sh -v ${FULL_VERSION} -m ${MAVEN}
echo ""
which pbcopy >/dev/null && ${0%/*}/print_rc_email.sh -v ${FULL_VERSION} -m ${MAVEN} | pbcopy && echo "(copied to clipboard)"
