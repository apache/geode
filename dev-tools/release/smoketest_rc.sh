#!/bin/bash



set -e

usage() {
    echo "Usage: smoketest-rc.sh -v version_number -m maven_coordinates"
    echo "  -v   The #.#.#.RC# version number"
    echo "  -m   The maven url to download artifacts from"
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
MAVEN_URL=""

while getopts ":v:m:" opt; do
  case ${opt} in
    v )
      FULL_VERSION=$OPTARG
      ;;
    m )
      MAVEN_URL=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${FULL_VERSION} == "" ]] || [[ ${MAVEN_URL} == "" ]]; then
    usage
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
#checkCommand svn
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

set -x
WORKSPACE=$PWD/smoketest-release-${VERSION}-workspace
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
echo "Cloning repositories at the RC tag..."
echo "============================================================"
set -x
mkdir src_clones
cd src_clones
git clone --depth=1 --branch=rel/v${FULL_VERSION} git@github.com:apache/geode.git apache-geode-${VERSION}-src
git clone --depth=1 --branch=rel/v${FULL_VERSION} git@github.com:apache/geode-examples.git apache-geode-examples-${VERSION}-src
git clone --depth=1 --branch=rel/v${FULL_VERSION} git@github.com:apache/geode-native.git apache-geode-native-${VERSION}-src
git clone --depth=1 --branch=rel/v${FULL_VERSION} git@github.com:apache/geode-benchmarks.git apache-geode-benchmarks-${VERSION}-src
cd $WORKSPACE
set +x


echo ""
echo "============================================================"
echo "Downloading artifacts"
echo "============================================================"

DOWNLOAD_URL="https://dist.apache.org/repos/dist/dev/geode/$FULL_VERSION"
set -x
wget $DOWNLOAD_URL -m -np -nv
set +x

DOWNLOAD_DIR=$WORKSPACE/dist.apache.org/repos/dist/dev/geode/$FULL_VERSION
echo ""
echo "============================================================"
echo "Untarring source releases"
echo "============================================================"
set -x
mkdir -p src_releases
cd src_releases
ls $DOWNLOAD_DIR/*src.tgz | xargs -n1 -I {} bash -c "tar xzf {}"
cd -
set +x

echo ""
echo "============================================================"
echo "Verifying Signatures"
echo "============================================================"

set -x
cd $DOWNLOAD_DIR
ls *.zip *.tar.gz *.tgz | xargs -n1 -I {} bash -c "shasum -c {}.sha*"
ls *.zip *.tar.gz *.tgz | xargs -n1 -I {} bash -c "gpg --verify {}.asc"
cd $WORKSPACE
set +x

echo ""
echo "============================================================"
echo "Comparing Source releases with the git tags"
echo "============================================================"
set -x
diff -r -q \
    -x build \
    -x gradlew \
    -x gradle-wrapper.jar \
    -x gradlew.bat \
    -x .git \
    -x .gitignore \
    -x .buildinfo \
    -x .gradle \
    -x .travis.yml \
    -x jpf.properties \
    -x .gitattributes \
    -x geode-old-versions \
    -x .asf.yaml \
    -x KEYS \
    $WORKSPACE/src_clones $WORKSPACE/src_releases
    
set +x

echo ""
echo "============================================================"
echo "Building from source releases"
echo "============================================================"
set -x

cd $WORKSPACE/src_releases/apache-geode-${VERSION}-src
./gradlew build -Pversion=${VERSION}

cd $WORKSPACE/src_releases/apache-geode-benchmarks-${VERSION}-src
./gradlew build -x test

cd $WORKSPACE/src_releases/apache-geode-examples-${VERSION}-src
./gradlew build -x test -PgeodeRepositoryUrl=$MAVEN_URL  -PgeodeReleaseUrl=$DOWNLOAD_URL

#Build the native source
#GEODE_INSTALL=$WORKSPACE/src_releases/apache-geode-${VERSION}-src/geode-assembly/build/install/apache-geode
#which brew >/dev/null && OPENSSL_ROOT_DIR=$(brew --prefix openssl) || OPENSSL_ROOT_DIR=$(which openssl)
#mkdir -p $WORKSPACE/src_releases/apache-geode-native-${VERSION}-src/build
#cd $WORKSPACE/src_releases/apache-geode-native-${VERSION}-src/build
#cmake .. -DPRODUCT_VERSION=${VERSION} -DOPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR -DGEODE_ROOT=${GEODE_INSTALL}
#cmake --build . --target install -j8
    
set +x

echo ""
echo "Automated release validation PASSED"
echo ""
echo "From the apache release policy - https://www.apache.org/legal/release-policy.html"
echo " . Before casting +1 binding votes, individuals are REQUIRED to "
echo " . download all signed source code packages onto their own hardware, "
echo " . verify that they meet all requirements of ASF policy on releases as described below, "
echo " . validate all cryptographic signatures, compile as provided, "
echo "   and test the result on their own platform."
echo ""
echo "This script has alerady validated signatures, compiled, and run the Geode unit tests"
echo "Please perform additional manual validation such as "
echo " - Build the native client from source "
echo " - Ensure that all artifacts meet with apache licensing and release quality standards "
echo " - Verify the release candidate passed all tests in the appropriate concourse pipeline on https://concourse.apachegeode-ci.info/"
echo ""
