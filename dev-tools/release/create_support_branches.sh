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

NEWVERSION_MM=${NEWMAJOR}.${NEWMINOR}
NEWVERSION_MM_NODOT=${NEWVERSION_MM//./}
NEWVERSION=${NEWVERSION_MM}.0

set -x
WORKSPACE=$PWD/support-${VERSION_MM}-workspace
GEODE=$WORKSPACE/geode
GEODE_DEVELOP=$WORKSPACE/geode-develop
GEODE_EXAMPLES=$WORKSPACE/geode-examples
GEODE_EXAMPLES_DEVELOP=$WORKSPACE/geode-examples-develop
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
#(cd geode; git reset --hard $desired_sha) #uncomment if latest commit is not the desired branchpoint
git clone --single-branch --branch develop git@github.com:apache/geode.git geode-develop
git clone --single-branch --branch develop git@github.com:apache/geode-examples.git
git clone --single-branch --branch develop git@github.com:apache/geode-examples.git geode-examples-develop
git clone --single-branch --branch develop git@github.com:apache/geode-native.git
git clone --single-branch --branch develop git@github.com:apache/geode-benchmarks.git
set +x


function failMsg2 {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 83-$(( errln - 1 ))) and try again"
}
trap 'failMsg2 $LINENO' ERR


cd ${GEODE}/../..
set -x
${0%/*}/set_copyright.sh ${GEODE} ${GEODE_DEVELOP} ${GEODE_EXAMPLES} ${GEODE_EXAMPLES_DEVELOP} ${GEODE_NATIVE} ${GEODE_BENCHMARKS}
set +x


echo ""
echo "============================================================"
echo "Pushing copyright updates (if any) to develop before branching"
echo "============================================================"
#get these 2 done before the branch so we don't have to do develop and support separately.
#the other 2 will be pushed to develop and support versions when version bumps are pushed.
for DIR in ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    if ! git push --dry-run 2>&1 | grep -q 'Everything up-to-date' ; then
      git push -u origin
    fi
    set +x
done


echo ""
echo "============================================================"
echo "Creating support/${VERSION_MM} branches"
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git checkout -b support/${VERSION_MM}
    git push -u origin support/${VERSION_MM}
    set +x
done


echo ""
echo "============================================================"
echo "Bumping version on develop to ${NEWVERSION}"
echo "============================================================"
set -x
cd ${GEODE_DEVELOP}
git pull -r
git remote add myfork git@github.com:${GITHUB_USER}/geode.git || true
git checkout -b roll-develop-to-${NEWVERSION}
set +x

#version = 1.13.0-build.0
sed -e "s/^version =.*/version = ${NEWVERSION}-build.0/" -i.bak gradle.properties

#  initial_version: 1.13.0-((stuff)).0
sed -e "s/^  initial_version:[^-]*\(-[^.0-9]*\)[.0-9]*/  initial_version: ${NEWVERSION}\1.0/" -i.bak ./ci/pipelines/shared/jinja.variables.yml

VER=geode-serialization/src/main/java/org/apache/geode/internal/serialization/KnownVersion.java
[ -r $VER ] || VER=geode-serialization/src/main/java/org/apache/geode/internal/serialization/Version.java
#add the new ordinal and KnownVersion constants and set them as current&highest
CURORD=$(cat $VER | awk '/private static final short GEODE_.*_ORDINAL/{print $NF}' | tr -d ';' | sort -n | tail -1)
NEWORD=$(( CURORD + 10 ))
sed -e "s#/. NOTE: when adding a new version#private static final short GEODE_${NEWMAJOR}_${NEWMINOR}_0_ORDINAL = ${NEWORD};\\
\\
  @Immutable\\
  public static final KnownVersion GEODE_${NEWMAJOR}_${NEWMINOR}_0 =\\
      new KnownVersion("'"'"GEODE"'"'", "'"'"${NEWMAJOR}.${NEWMINOR}.0"'"'", (byte) ${NEWMAJOR}, (byte) ${NEWMINOR}, (byte) 0, (byte) 0,\\
          GEODE_${NEWMAJOR}_${NEWMINOR}_0_ORDINAL);\\
\\
  /* NOTE: when adding a new version#" \
  -e "/public static final KnownVersion CURRENT/s#GEODE[0-9_]*#GEODE_${NEWMAJOR}_${NEWMINOR}_0#" \
  -e "/public static final int HIGHEST_VERSION/s# = [0-9]*# = ${NEWORD}#" \
  -i.bak $VER

#  directory: docs/guide/113
#  product_version: '1.13.2'
#  product_version_nodot: '113'
#  product_version_geode: '1.13'
#  product_version_old_minor: '1.12'
sed -E \
    -e "s#docs/guide/[0-9]+#docs/guide/${NEWVERSION_MM_NODOT}#" \
    -e "s#product_version: '[0-9.]+'#product_version: '${NEWVERSION%.0}'#" \
    -e "s#version_nodot: '[0-9]+'#version_nodot: '${NEWVERSION_MM_NODOT}'#" \
    -e "s#product_version_geode: '[0-9.]+'#product_version_geode: '${NEWVERSION_MM}'#" \
    -e "s#product_version_old_minor: '[0-9.]+'#product_version_old_minor: '${VERSION_MM}'#" \
    -i.bak geode-book/config.yml

#rewrite '/', '/docs/guide/113/about_geode.html'
#rewrite '/index.html', '/docs/guide/113/about_geode.html'
sed -E -e "s#docs/guide/[0-9]+#docs/guide/${NEWVERSION_MM_NODOT}#" -i.bak geode-book/redirects.rb

rm gradle.properties.bak ci/pipelines/shared/jinja.variables.yml.bak geode-book/config.yml.bak geode-book/redirects.rb.bak $VER.bak*
set -x
git add .
git diff --staged --color | cat

./gradlew updateExpectedPom

git commit -a -m "roll develop to ${NEWVERSION} now that support/${VERSION_MM} has been created"
git push -u myfork
set +x


echo ""
echo "============================================================"
echo "Bumping examples version on develop to ${NEWVERSION}"
echo "============================================================"
set -x
cd ${GEODE_EXAMPLES_DEVELOP}
git pull -r
set +x

#version = 1.13.0-build.0
#geodeVersion = 1.13.0-build+
sed \
  -e "s/^version =.*/version = ${NEWVERSION}-build.0/" \
  -e "s/^geodeVersion =.*/geodeVersion = ${NEWVERSION_MM}.+/" \
  -i.bak gradle.properties
rm gradle.properties.bak
set -x
git add gradle.properties
git diff --staged --color | cat
git commit -m "pair develop examples with ${NEWVERSION} now that support/${VERSION_MM} has been created"
git push -u origin
set +x


echo ""
echo "============================================================"
echo "Removing CODEOWNERS and duplicate scripts from support/${VERSION_MM}"
echo "============================================================"
set -x
cd ${GEODE}/dev-tools/release
git pull -r
git rm *.sh
cat << EOF > README.md
See [Releasing Apache Geode](https://cwiki.apache.org/confluence/display/GEODE/Releasing+Apache+Geode)
EOF
git add README.md
cd ${GEODE}
[ ! -r CODEOWNERS ] || git rm CODEOWNERS
[ ! -r CODEWATCHERS ] || git rm CODEWATCHERS
git commit -m "remove outdated copies of release scripts to ensure they are not run by accident + remove CODEOWNERS to avoid confusion"
git push -u origin
set +x


echo ""
echo "============================================================"
echo "Setting version on support/${VERSION_MM}"
echo "============================================================"
cd ${GEODE}/../..
set -x
${0%/*}/set_versions.sh -v ${VERSION_MM}.0 -s -w "${WORKSPACE}"
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
echo "3. Add ${NEWVERSION} to Jira at https://issues.apache.org/jira/projects/GEODE?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page"
echo "4. (cd ${GEODE}/ci/pipelines/meta && ./deploy_meta.sh) #takes about 2 hours. keep re-running until successful."
echo "5. That's it for now.  Once all needed fixes have been proposed and cherry-picked to support/${VERSION_MM} and https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-support-${VERSION_MM/./-}-main is green, come back and run ${0%/*}/prepare_rc.sh -v ${VERSION}.RC1"
