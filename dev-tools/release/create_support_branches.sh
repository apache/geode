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
    echo "Usage: create_support_branches.sh -v version_number -g your_github_username"
    echo "  -v   The #.# version number of the support branch to create"
    echo "  -g   Your github username"
    exit 1
}

VERSION_MM=""
GITHUB_USER=""

while getopts ":v:g:" opt; do
  case ${opt} in
    v )
      VERSION_MM=$OPTARG
      ;;
    g )
      GITHUB_USER=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${VERSION_MM} == "" ]] || [[ ${GITHUB_USER} == "" ]] ; then
    usage
fi

if [[ $VERSION_MM =~ ^([0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${VERSION_MM}. Example valid version: 1.9"
    exit 1
fi

MAJOR=${VERSION_MM%.*}
MINOR=${VERSION_MM#*.}

#tip: hardcode NEWMAJOR and NEWMINOR as needed if jumping to a new major
NEWMAJOR=${MAJOR}
NEWMINOR=$((MINOR + 1))

NEWVERSION=${NEWMAJOR}.${NEWMINOR}
NEWVERSION_NODOT=${NEWVERSION//./}

set -x
WORKSPACE=$PWD/support-${VERSION_MM}-workspace
GEODE=$WORKSPACE/geode
GEODE_DEVELOP=$WORKSPACE/geode-develop
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
git clone --single-branch --branch develop git@github.com:apache/geode.git
git clone --single-branch --branch develop git@github.com:apache/geode.git geode-develop
git clone --single-branch --branch develop git@github.com:apache/geode-examples.git
git clone --single-branch --branch develop git@github.com:apache/geode-native.git
git clone --single-branch --branch develop git@github.com:apache/geode-benchmarks.git
set +x


function failMsg2 {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 70-$(( errln - 1 ))) and try again"
}
trap 'failMsg2 $LINENO' ERR


echo ""
echo "============================================================"
echo "Creating support/${VERSION_MM} branches"
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git checkout -b support/${VERSION_MM}
    git push -u origin HEAD
    set +x
done


echo ""
echo "============================================================"
echo "Bumping version on develop to ${NEWVERSION}"
echo "============================================================"
set -x
cd ${GEODE_DEVELOP}
git pull
git remote add myfork git@github.com:${GITHUB_USER}/geode.git || true
git checkout -b roll-develop-to-${NEWVERSION}
set +x

#version = 1.13.0-SNAPSHOT
sed -e "s/^version =.*/version = ${NEWVERSION}-SNAPSHOT/" -i.bak gradle.properties

#  initial_version: 1.12.0
sed -e "s/^  initial_version:.*/  initial_version: ${NEWVERSION}/" -i.bak ./ci/pipelines/shared/jinja.variables.yml

VER=geode-serialization/src/main/java/org/apache/geode/internal/serialization/Version.java
#add the new ordinal and Version constants and set them as current&highest
CURORD=$(cat $VER | awk '/private static final short GEODE_.*_ORDINAL/{print $NF}' | tr -d ';' | sort -n | tail -1)
NEWORD=$(( CURORD + 5 ))
sed -e "s#/. NOTE: when adding a new version#private static final short GEODE_${NEWMAJOR}_${NEWMINOR}_0_ORDINAL = ${NEWORD};\\
\\
  @Immutable\\
  public static final Version GEODE_${NEWMAJOR}_${NEWMINOR}_0 =\\
      new Version("'"'"GEODE"'"'", "'"'"${NEWMAJOR}.${NEWMINOR}.0"'"'", (byte) ${NEWMAJOR}, (byte) ${NEWMINOR}, (byte) 0, (byte) 0, GEODE_${NEWMAJOR}_${NEWMINOR}_0_ORDINAL);\\
\\
  /* NOTE: when adding a new version#" \
  -e "/public static final Version CURRENT/s#GEODE[0-9_]*#GEODE_${NEWMAJOR}_${NEWMINOR}_0#" \
  -e "/public static final int HIGHEST_VERSION/s# = [0-9]*# = ${NEWORD}#" \
  -i.bak $VER

COM=geode-core/src/main/java/org/apache/geode/internal/cache/tier/sockets/CommandInitializer.java
#add to list of all commands
sed -e "s#return Collections.unmodifiableMap(allCommands#allCommands.put(Version.GEODE_${NEWMAJOR}_${NEWMINOR}_0, geode18Commands);\\
    return Collections.unmodifiableMap(allCommands#" \
  -i.bak $COM

#  directory: docs/guide/113
#  product_version: '1.13'
#  product_version_nodot: '113'
#  product_version_geode: '1.13'
sed -E \
    -e "s#docs/guide/[0-9]+#docs/guide/${NEWVERSION_NODOT}#" \
    -e "s#product_version: '[0-9.]+'#product_version: '${NEWVERSION}'#" \
    -e "s#version_nodot: '[0-9]+'#version_nodot: '${NEWVERSION_NODOT}'#" \
    -e "s#product_version_geode: '[0-9.]+'#product_version_geode: '${NEWVERSION}'#" \
    -i.bak geode-book/config.yml

#rewrite '/', '/docs/guide/113/about_geode.html'
#rewrite '/index.html', '/docs/guide/113/about_geode.html'
sed -E -e "s#docs/guide/[0-9]+#docs/guide/${NEWVERSION_NODOT}#" -i.bak geode-book/redirects.rb

rm gradle.properties.bak ci/pipelines/shared/jinja.variables.yml.bak geode-book/config.yml.bak geode-book/redirects.rb.bak $VER.bak* $COM.bak*
set -x
git add .
git diff --staged

./gradlew clean
./gradlew build -Dskip.tests=true
./gradlew updateExpectedPom

git commit -a -m "roll develop to ${NEWVERSION} now that support/${VERSION_MM} has been created"
git push -u myfork
set +x


echo ""
echo "============================================================"
echo "Setting version on support/${VERSION_MM}"
echo "============================================================"
set -x
${0%/*}/set_versions.sh -v ${VERSION_MM}.0
set +x


echo ""
echo "============================================================"
echo "Logging you in to concourse"
echo "============================================================"
set -x
fly -t concourse.apachegeode-ci.info-main login --team-name main --concourse-url https://concourse.apachegeode-ci.info/
set +x


echo ""
echo "============================================================"
echo "Done creating support branches"
echo "============================================================"
cd ${GEODE}/../..
echo "Next steps:"
echo "1. Go to https://github.com/${GITHUB_USER}/geode/pull/new/roll-develop-to-${NEWVERSION} and create the pull request"
echo "2. Plus the BumpMinor job at https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-develop-main?group=Semver%20Management"
echo "3. Add the new version to Jira at https://issues.apache.org/jira/projects/GEODE?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page"
echo "4. (cd ${GEODE}/ci/pipelines/meta && ./deploy_meta.sh) #takes about 2 hours. keep re-running until successful."
