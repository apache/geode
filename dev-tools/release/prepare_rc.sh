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
    echo "Usage: prepare_rc -v version_number -k signing_key -a apache_ldap_username"
    echo "  -v   The #.#.#.RC# version number"
    echo "  -k   Your 8 digit GPG key id (the last 8 digits of your gpg fingerprint)"
    echo "  -a   Your apache LDAP username (that you use to log in to https://id.apache.org)"
    exit 1
}

checkCommand() {
    COMMAND=$1
    if ! [[ -x "$(command -v $COMMAND)" ]]; then
        echo "$COMMAND must be installed"
        exit 1
    fi
}

FULL_VERSION=""
SIGNING_KEY=""
APACHE_USERNAME=""

while getopts ":v:k:a:" opt; do
  case ${opt} in
    v )
      FULL_VERSION=$OPTARG
      ;;
    k )
      SIGNING_KEY=$OPTARG
      ;;
    a )
      APACHE_USERNAME=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${FULL_VERSION} == "" ]] || [[ ${SIGNING_KEY} == "" ]] || [[ ${APACHE_USERNAME} == "" ]]; then
    usage
fi

if [[ $SIGNING_KEY =~ ^[0-9A-Fa-f]{8}$ ]]; then
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

checkCommand gpg
checkCommand cmake
checkCommand svn
checkCommand doxygen


echo ""
echo "============================================================"
echo "Checking java..."
echo "============================================================"
[ -z "$JAVA_HOME" ] && JAVA=java || JAVA=$JAVA_HOME/bin/java
if ! $JAVA -XshowSettings:properties -version 2>&1 | grep 'java.specification.version = 1.8' ; then
  echo "Please set JAVA_HOME to use JDK 8 to compile Geode for release"
  exit 1
fi
if $JAVA -XshowSettings:properties -version 2>&1 | grep 'java.vm.vendor = Oracle' ; then
  echo "Please set JAVA_HOME to use an Open JDK 8 such as from https://adoptopenjdk.net/?variant=openjdk8&jvmVariant=hotspot to compile Geode for release"
  exit 1
else
  $JAVA -XshowSettings:properties -version 2>&1 | grep 'java.vm.vendor = '
fi


echo ""
echo "============================================================"
echo "Checking gpg... (you will be prompted to enter passphrase)"
echo "============================================================"
SECRING=~/.gnupg/secring.gpg
! [ -r $SECRING ] || SECRING=/dev/null
if gpg --export-secret-keys > ${SECRING} && echo "1234" | gpg -o /dev/null --local-user ${SIGNING_KEY} -as - ; then
  echo "You entered the correct passphrase; proceeding."
  echo "Please note, you will still need to enter it a few more times."
  echo "PLEASE NOTE, the very last prompt will be for your apache password (not gpg).  Pay attention as the prompts look very similar."
else
  echo "Hmm, gpg seems unhappy.  Check that you entered correct passphrase or refer to release wiki for troubleshooting."
  exit 1
fi
if ! gpg --list-keys ${SIGNING_KEY} | grep -q "${APACHE_USERNAME}@apache.org" ; then
  echo "Please specify a gpg key that is associated with your apache email address."
  echo "Expected: ${APACHE_USERNAME}@apache.org"
  echo "Found:    $(gpg --list-keys ${SIGNING_KEY} | grep ^uid | sed -e 's/.*<//' -e 's/>.*//')"
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
if which shasum >/dev/null; then
  SHASUM="shasum -a 256"
else
  SHASUM=sha256sum
fi
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
#if you attempt to reset to a prior SHA here, skip ${GEODE} in set_copyright.sh or it may backfire
#(cd geode; git reset --hard $desired_sha) #uncomment if latest commit is not the desired sha
git clone git@github.com:apache/geode.git geode-develop
git clone --single-branch --branch support/${VERSION_MM} git@github.com:apache/geode-examples.git
git clone --single-branch --branch support/${VERSION_MM} git@github.com:apache/geode-native.git
git clone --single-branch --branch develop git@github.com:apache/geode-native.git geode-native-develop
git clone --single-branch --branch support/${VERSION_MM} git@github.com:apache/geode-benchmarks.git
git clone --single-branch --branch develop git@github.com:apache/geode-benchmarks.git geode-benchmarks-develop
git clone --single-branch --branch master git@github.com:Homebrew/homebrew-core.git

svn checkout https://dist.apache.org/repos/dist --depth empty
svn update --set-depth immediates --parents dist/release/geode
svn update --set-depth infinity --parents dist/dev/geode
set +x

for REPO in ${GEODE} ${WORKSPACE}/geode-develop ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ${BREW_DIR} ; do
  cd ${REPO}
  git config user.email "${APACHE_USERNAME}@apache.org"
done

cd ${GEODE}/../..
set -x
${0%/*}/set_copyright.sh ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS}
set +x


echo ""
echo "============================================================"
echo "Keeping -build.0 suffix"
echo "============================================================"
cd ${GEODE}/../..
set -x
${0%/*}/set_versions.sh -v ${VERSION} -n -w ${WORKSPACE}
set +x


echo ""
echo "============================================================"
echo "Building geode..."
echo "============================================================"
set -x
cd ${GEODE}
git clean -fdx && ./gradlew build -x test publishToMavenLocal -Pversion=${VERSION} -Paskpass -Psigning.keyId=${SIGNING_KEY} -Psigning.secretKeyRingFile=${HOME}/.gnupg/secring.gpg
set +x


if [ "${FULL_VERSION##*.RC}" -gt 1 ] ; then
    echo ""
    echo "============================================================"
    echo "Removing previous RC's temporary commit from geode-examples..."
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
    if [ $(git diff --staged | wc -l) -gt 0 ] ; then
        git diff --staged --color | cat
        git commit -m 'Revert "temporarily point to staging repo for CI purposes"'
    fi
    set +x
fi


echo ""
echo "============================================================"
echo "Building geode-examples..."
echo "============================================================"
set -x
cd ${GEODE_EXAMPLES}
git clean -dxf && ./gradlew -Pversion=${VERSION} -PsignArchives -PgeodeReleaseUrl="file://${GEODE}/geode-assembly/build/geode-assembly/build/distributions/apache-geode-${VERSION}" -PgeodeRepositoryUrl="file://${HOME}/.m2/repository" -Psigning.keyId=${SIGNING_KEY} -Psigning.secretKeyRingFile=${HOME}/.gnupg/secring.gpg build
set +x


echo ""
echo "============================================================"
echo "Building geode-native..."
echo "============================================================"
set -x
cd ${GEODE_NATIVE}
mkdir build
which brew >/dev/null && OPENSSL_ROOT_DIR=$(brew --prefix openssl) || OPENSSL_ROOT_DIR=$(which openssl)
cd ${GEODE_NATIVE}/build
cmake .. -DPRODUCT_VERSION=${VERSION} -DOPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR -DGEODE_ROOT=${GEODE}/geode-assembly/build/install/apache-geode
cpack -G TGZ --config CPackSourceConfig.cmake
NCOUT=apache-geode-native-${VERSION}-src.tar.gz
NCTGZ=apache-geode-native-${VERSION}-src.tgz
mkdir repkg-temp
cd repkg-temp
tar xzf ../${NCOUT}
rm ../${NCOUT}*
mv apache-geode-native apache-geode-native-${VERSION}-src
tar czf ../${NCTGZ} *
cd ..
rm -Rf repkg-temp
gpg --armor -u ${SIGNING_KEY} -b ${NCTGZ}
${SHASUM} ${NCTGZ} > ${NCTGZ}.sha256
set +x


echo ""
echo "============================================================"
echo "Building geode-benchmarks..."
echo "============================================================"
set -x
cd ${GEODE_BENCHMARKS}
BMDIR=apache-geode-benchmarks-${VERSION}-src
BMTAR=${BMDIR}.tgz
git clean -dxf
mkdir ../${BMDIR}
cp -r .travis.yml * ../${BMDIR}
tar czf ${BMTAR} -C .. ${BMDIR}
rm -Rf ../${BMDIR}
gpg --armor -u ${SIGNING_KEY} -b ${BMTAR}
${SHASUM} ${BMTAR} > ${BMTAR}.sha256
set +x


function failMsg2 {
  errln=$1
  echo "ERROR: script did NOT complete successfully"
  echo "Comment out any steps that already succeeded (approximately lines 144-$(( errln - 1 ))) and try again"
  echo "For this script only (prepare_rc.sh), it's also safe to just try again from the top"
}
trap 'failMsg2 $LINENO' ERR


echo ""
echo "============================================================"
echo "Tagging the release candidate in each repository. The tags will not be pushed yet..."
echo "============================================================"
for DIR in ${GEODE} ${GEODE_EXAMPLES} ${GEODE_NATIVE} ${GEODE_BENCHMARKS} ; do
    set -x
    cd ${DIR}
    git tag -s -u ${SIGNING_KEY} rel/v${FULL_VERSION} -m "Release candidate ${FULL_VERSION}"
    set +x
done


echo ""
echo "============================================================"
echo "Copying artifacts to svn directory for publication. The artifacts will not be committed..."
echo "============================================================"
set -x
cd ${SVN_DIR}
svn rm ${VERSION}.RC* &>/dev/null || true
cp ${GEODE}/KEYS .
mkdir ${FULL_VERSION}
cp ${GEODE}/geode-assembly/build/distributions/* ${FULL_VERSION}
cp ${GEODE_EXAMPLES}/build/distributions/* ${FULL_VERSION}
cp ${GEODE_NATIVE}/build/apache-geode-native-${VERSION}* ${FULL_VERSION}
cp ${GEODE_BENCHMARKS}/apache-geode-benchmarks-${VERSION}* ${FULL_VERSION}
set +x

# verify all files are signed.  sometimes gradle "forgets" to make the .asc file
for f in ${FULL_VERSION}/*.tgz ; do
  if ! [ -r $f.sha256 ] ; then
    echo missing $f.sha256
    exit 1
  fi
  if ! [ -r $f.asc ] ; then
    set -x
    gpg --armor -u ${SIGNING_KEY} -b $f
    set +x
    if ! [ -r $f.asc ] ; then
      echo missing $f.asc
      exit 1
    fi
  fi
  size=$(ls -l $f | awk '{print $5}')
  if [ $size -lt 10000 ] ; then
    echo $f file size is only $size bytes, that seems suspicious.
    exit 1
  fi
done

set -x
svn add ${FULL_VERSION}
set +x


echo ""
echo "============================================================"
echo "Publishing artifacts to nexus staging manager..."
echo "PLEASE NOTE, the 2nd prompt will be for your apache (not gpg) password.  Pay attention as the prompts look very similar."
echo "============================================================"
publishcmd="./gradlew publish --no-parallel -Pversion=${VERSION} -Paskpass -Psigning.keyId=${SIGNING_KEY} -Psigning.secretKeyRingFile=${HOME}/.gnupg/secring.gpg -PmavenUsername=${APACHE_USERNAME}"
set -x
cd ${GEODE}
sh -c "$publishcmd"
set +x


echo ""
echo "============================================================"
echo "Done preparing the release and staging to nexus! Next steps:"
echo "============================================================"
cd ${GEODE}/../..
echo "1. Go to https://repository.apache.org, login as ${APACHE_USERNAME}, and click on Staging Repositories"
echo "2. If there is a prior ${VERSION} RC, select it and click Drop."
echo "2b.If publication got split between two staging repos, drop one of them then run: pushd ${GEODE}; $publishcmd; popd"
echo '3. Make a note of the 4-digit ID of the current ("implicitly created") staging repo.'
echo '4. Select the current staging repo and click Close.'
echo '5. Wait ~10 seconds and then refresh the page to confirm that status has become "Closed"'
echo "6. Run ${0%/*}/commit_rc.sh -v ${FULL_VERSION} -m <4-DIGIT-ID-NOTED-ABOVE>"
