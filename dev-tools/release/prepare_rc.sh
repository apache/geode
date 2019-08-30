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
      echo "  -k   Your 8 digit PGP key id. Must be 8 digits. Also the last 8 digits of your gpg fingerprint"
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

if [[ $FULL_VERSION =~ ([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+) ]]; then
    VERSION=${BASH_REMATCH[1]}
    RC=${BASH_REMATCH[2]}
else
    echo "Malformed version number ${FULL_VERSION}. Example valid number - 1.9.0.RC1"
    exit 1
fi

checkCommand gpg
checkCommand cmake
checkCommand svn
checkCommand doxygen

echo "============================================================"
echo "Checking gpg... (you will be prompted to enter passphase)"
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


GEODE=$PWD/build/geode
GEODE_EXAMPLES=$PWD/build/geode-examples
GEODE_NATIVE=$PWD/build/geode-native
SVN_DIR=$PWD/build/dist/dev/geode

echo "============================================================"
echo "Cleaning build directory..."
echo "============================================================"
rm -rf build
mkdir -p build
cd build



echo "============================================================"
echo "Cloning repositories..."
echo "============================================================"
set -x
git clone --branch release/${VERSION} git@github.com:apache/geode.git
git clone --branch release/${VERSION} git@github.com:apache/geode-examples.git
git clone --branch release/${VERSION} git@github.com:apache/geode-native.git

svn checkout https://dist.apache.org/repos/dist --depth empty
svn update --set-depth infinity --parents dist/dev/geode


set +x
echo "============================================================"
echo "Building geode..."
echo "============================================================"

cd ${GEODE}
svn rm ${VERSION}.RC* &>/dev/null || true
set -x
git clean -fdx && ./gradlew build -x test publishToMavenLocal -Paskpass -Psigning.keyId=${SIGNING_KEY} -Psigning.secretKeyRingFile=${HOME}/.gnupg/secring.gpg
set +x


echo "============================================================"
echo "Building geode-examples..."
echo "============================================================"

cd ${GEODE_EXAMPLES}
set -x
git clean -dxf && ./gradlew -PsignArchives -PgeodeReleaseUrl="file://${GEODE}/geode-assembly/build/geode-assembly/build/distributions/apache-geode-${VERSION}" -PgeodeRepositoryUrl="file://${HOME}/.m2/repository" -Psigning.keyId=${SIGNING_KEY} -Psigning.secretKeyRingFile=${HOME}/.gnupg/secring.gpg build
set +x

echo "============================================================"
echo "Building geode-native..."
echo "============================================================"

cd ${GEODE_NATIVE}
mkdir build
cd build
which brew >/dev/null && OPENSSL_ROOT_DIR=$(brew --prefix openssl) || OPENSSL_ROOT_DIR=$(which openssl)
set -x
cmake .. -DPRODUCT_VERSION=${VERSION} -DOPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR -DGEODE_ROOT=${GEODE}/geode-assembly/build/install/apache-geode
cpack -G TGZ --config CPackSourceConfig.cmake
gpg --armor -u ${SIGNING_KEY} -b apache-geode-native-${VERSION}-src.tar.gz
set +x


echo "============================================================"
echo "Tagging the release candidate in each repository. The tags will not be pushed yet..."
echo "============================================================"

cd ${GEODE}
git tag -s -u ${SIGNING_KEY} rel/v${FULL_VERSION} -m "Release candidate ${FULL_VERSION}"
cd ${GEODE_EXAMPLES}
git tag -s -u ${SIGNING_KEY} rel/v${FULL_VERSION} -m "Release candidate ${FULL_VERSION}"
cd ${GEODE_NATIVE}
git tag -s -u ${SIGNING_KEY} rel/v${FULL_VERSION} -m "Release candidate ${FULL_VERSION}"

echo "============================================================"
echo "Copying artifacts to svn directory for publication. The artifacts will not be committed..."
echo "============================================================"

cd ${SVN_DIR}
cp ${GEODE}/KEYS .
mkdir ${FULL_VERSION}
cp ${GEODE}/geode-assembly/build/distributions/* ${FULL_VERSION}

cp ${GEODE_EXAMPLES}/build/distributions/* ${FULL_VERSION}

cp ${GEODE_NATIVE}/build/apache-geode-native-${VERSION}* ${FULL_VERSION}
svn add ${FULL_VERSION}

echo "============================================================"
echo "Publishing artifacts to nexus staging manager..."
echo "PLEASE NOTE, the 2nd prompt will be for your apache password (not gpg).  Pay attention as the prompts look very similar."
echo "============================================================"
cd ${GEODE}
set -x
./gradlew publish -Paskpass -Psigning.keyId=${SIGNING_KEY} -Psigning.secretKeyRingFile=${HOME}/.gnupg/secring.gpg -PmavenUsername=${APACHE_USERNAME}
set +x

echo "============================================================"
echo "Done preparing the release and staging to nexus! Next steps:"
echo "============================================================"
echo "1. Go to https://repository.apache.org, login as ${APACHE_USERNAME}, and click on Staging Repositories"
echo "2. If there is a prior ${VERSION} RC, select it and click Drop."
echo '3. Make a note of the 4-digit ID of the current ("implicitly created") staging repo.'
echo '4. Select the current staging repo and click Close.'
echo '5. Wait ~15 minutes for status to become "Closed"'
echo "6. Run ${0%/*}/commit_rc.sh -v ${FULL_VERSION} -m <4-DIGIT-ID-NOTED-ABOVE>"

cd ${GEODE}/../..
