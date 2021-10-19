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
    echo "Usage: deploy_rc_pipeline -v version_number"
    echo "  -v   The #.# version number"
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

if [[ ${VERSION_MM} == "" ]]; then
    usage
fi

if [[ $VERSION_MM =~ ^([0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${VERSION_MM}. Example valid version: 1.9"
    exit 1
fi

PIPEYML=$PWD/rc-pipeline.yml
cat << "EOF" | sed -e "s/<VERSION_MM>/${VERSION_MM}/" > $PIPEYML
---

resources:
- name: geode
  type: git
  source:
    branch: support/<VERSION_MM>
    tag_filter: rel/v<VERSION_MM>.*.RC*
    uri: https://github.com/apache/geode.git
- name: geode-develop
  type: git
  source:
    branch: develop
    uri: https://github.com/apache/geode.git
- name: geode-examples
  type: git
  source:
    branch: support/<VERSION_MM>
    uri: https://github.com/apache/geode-examples.git
- name: geode-native
  type: git
  source:
    branch: support/<VERSION_MM>
    tag_filter: rel/v<VERSION_MM>.*.RC*
    uri: https://github.com/apache/geode-native.git
- name: geode-benchmarks
  type: git
  source:
    branch: support/<VERSION_MM>
    tag_filter: rel/v<VERSION_MM>.*.RC*
    uri: https://github.com/apache/geode-benchmarks.git
- name: upthewaterspout-tests
  type: git
  source:
    branch: master
    uri: https://github.com/upthewaterspout/geode-release-check.git

jobs:
  - name: build-geode-from-tag
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends unzip git
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              SHA=$(cd geode && git rev-parse HEAD)
              java -version
              cd geode
              ./gradlew test installDist
              gfsh=geode-assembly/build/install/apache-geode/bin/gfsh
              $gfsh version --full | grep "^Source-Revision: ${SHA}$"
              $gfsh version --full | grep "^Product-Version: ${VERSION}-build"
              ! $gfsh version --full | grep Oracle
  - name: build-geode-from-src-tgz
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends unzip git
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              SHA=$(cd geode && git rev-parse HEAD)
              curl -L -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-${VERSION}-src.tgz > src.tgz
              tar xzf src.tgz
              cd apache-geode-${VERSION}-src
              java -version
              ./gradlew test installDist
              gfsh=geode-assembly/build/install/apache-geode/bin/gfsh
              $gfsh version --full | grep "^Source-Revision: ${SHA}$"
              $gfsh version --full | grep "^Product-Version: ${VERSION}-build"
              ! $gfsh version --full | grep Oracle
  - name: run-gfsh-from-tgz
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends git
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              SHA=$(cd geode && git rev-parse HEAD)
              curl -L -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-${VERSION}.tgz > bin.tgz
              tar xzf bin.tgz
              cd apache-geode-${VERSION}
              java -version
              gfsh=bin/gfsh
              $gfsh version --full | grep "^Source-Revision: ${SHA}$"
              $gfsh version --full | grep "^Product-Version: ${VERSION}$"
              ! $gfsh version --full | grep Oracle
  - name: run-geode-examples-jdk11
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode-examples
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 11
          inputs:
            - name: geode-examples
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends unzip
              cd geode-examples
              java -version
              ./gradlew runAll
  - name: run-geode-examples-from-src-tgz-jdk8
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode-examples
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode-examples
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends unzip git
              FULL_VERSION=$(cd geode-examples && git describe --tags | sed -e 's#^rel/v##' -e 's#-.*##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              if [ "${FULL_VERSION}" = "${VERSION}" ] ; then
                GRADLE_ARGS=""
                curl -L -s https://downloads.apache.org/geode/${VERSION}/apache-geode-examples-${VERSION}-src.tgz > src.tgz
              else
                STAGING_MAVEN=$(cat geode-examples/gradle.properties | grep geodeRepositoryUrl | awk '{print $3}')
                GRADLE_ARGS="-PgeodeReleaseUrl=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION} -PgeodeRepositoryUrl=${STAGING_MAVEN}"
                curl -L -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-examples-${VERSION}-src.tgz > src.tgz
              fi
              tar xzf src.tgz
              cd apache-geode-examples-${VERSION}-src
              java -version
              ./gradlew ${GRADLE_ARGS} build runAll
  - name: build-geode-native-from-tag
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode-native
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: adoptopenjdk/openjdk8
              tag: slim
          inputs:
            - name: geode-native
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends git
              FULL_VERSION=$(cd geode-native && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              #use geode from binary dist
              curl -L -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-${VERSION}.tgz > geode-bin.tgz
              tar xzf geode-bin.tgz
              apt-get update || true
              DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y cmake openssl doxygen build-essential libssl-dev zlib1g-dev
              #cmake wrongly assumes javah wasn't removed until JDK10, but adoptopenjdk removed it in JDK8
              echo '/opt/java/openjdk/bin/javac -h "$@"' > /opt/java/openjdk/bin/javah
              chmod +x /opt/java/openjdk/bin/javah
              cd geode-native
              mkdir build
              cd build
              cmake .. -DGEODE_ROOT=$PWD/../../apache-geode-${VERSION}
              cmake --build . -- -j 4
              cmake --build . --target docs -- -j 4
              cmake --build . --target install -- -j 4
  - name: build-geode-native-from-src-tgz
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode-native
          trigger: true
        - get: geode
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: adoptopenjdk/openjdk8
              tag: slim
          inputs:
            - name: geode-native
            - name: geode
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends unzip git
              FULL_VERSION=$(cd geode-native && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              # build geode from source
              cd geode
              ./gradlew build -x test -x javadoc -x rat -x pmdMain
              cd ..
              apt-get update || true
              DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y cmake openssl doxygen build-essential libssl-dev zlib1g-dev
              #cmake wrongly assumes javah wasn't removed until JDK10, but adoptopenjdk removed it in JDK8
              echo '/opt/java/openjdk/bin/javac -h "$@"' > /opt/java/openjdk/bin/javah
              chmod +x /opt/java/openjdk/bin/javah
              curl -L -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-native-${VERSION}-src.tgz > src.tgz
              tar xzf src.tgz
              cd apache-geode-native-${VERSION}-src
              mkdir build
              cd build
              cmake .. -DGEODE_ROOT=$PWD/../../geode/geode-assembly/build/install/apache-geode
              cmake --build . -- -j 4
              cmake --build . --target docs -- -j 4
              cmake --build . --target install -- -j 4
  - name: upthewaterspout
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
        - get: upthewaterspout-tests
        - get: geode-examples
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode
            - name: upthewaterspout-tests
            - name: geode-examples
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends unzip git gpg wget
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              STAGING_MAVEN=$(cat geode-examples/gradle.properties | grep geodeRepositoryUrl | awk '{print $3}')
              cd upthewaterspout-tests
              curl -L -s https://dist.apache.org/repos/dist/dev/geode/KEYS > KEYS
              gpg --import KEYS
              java -version
              ./gradlew build -PmavenURL=${STAGING_MAVEN} -PdownloadURL=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/ -Pversion=${FULL_VERSION}
  - name: benchmarks-test
    serial: true
    public: true
    plan:
      - get: geode-benchmarks
        trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode-benchmarks
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends unzip git keychain
              FULL_VERSION=$(cd geode-benchmarks && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              curl -L -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-benchmarks-${VERSION}-src.tgz > src.tgz
              tar xzf src.tgz
              cd apache-geode-benchmarks-${VERSION}-src
              java -version
              mkdir -p ~/.ssh
              ssh-keygen -m PEM -b 2048 -t rsa -f ~/.ssh/id_rsa -q -N ""
              cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
              apt-get update || true
              apt-get install openssh-server --no-install-recommends -y
              echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config
              service ssh start
              echo 'tasks.withType(JavaCompile) { options.encoding = "UTF-8" }' >> geode-benchmarks/build.gradle
              ./gradlew build test
  - name: verify-expected-files-and-keys
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode
          platform: linux
          run:
            path: /bin/bash
            args:
            - -ec
            - |
              set -ex
              apt update -q
              apt install -qq -y --no-install-recommends git gpg
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              curl -L -s https://dist.apache.org/repos/dist/dev/geode/KEYS > KEYS
              gpg --import KEYS
              url=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}
              function verifyArtifactSizeSignatureLicenseNoticeAndCopyright {
                tld=$1
                file=${tld}.tgz
                minfilesize=$2
                maxfilesize=$3
                echo Verifying $file...
                asc=${file}.asc
                sha=${file}.sha256
                sum=sha256sum
                curl -L -s $url/$file > $file
                actualfilesize=$(wc -c < $file)
                if [ $actualfilesize -lt $minfilesize ] ; then
                  echo "File size of $file is only $actualfilesize bytes, expected at least $minfilesize"
                  return 1
                fi
                if [ $actualfilesize -gt $maxfilesize ] ; then
                  echo "File size of $file is $actualfilesize, expected no more than $maxfilesize bytes"
                  return 1
                fi
                curl -L -s $url/$asc > $asc
                curl -L -s $url/$sha > $sha
                gpg --verify $asc
                $sum -c $sha
                echo $file >> exp
                echo $asc >> exp
                echo $sha >> exp
                #check that each archive contains all content below a top-level-directory with the same name as the file (sans .tgz)
                ! tar tvzf $file | grep -v " ${tld}/"
                #check that each archive contains LICENSE and NOTICE
                tar tvzf $file | grep " ${tld}/LICENSE"
                tar tvzf $file | grep " ${tld}/NOTICE"
                #check that NOTICE contains current copyright year and correctly assigns copyright to ASF
                tar xzf $file "${tld}/NOTICE"
                year=$(date +%Y)
                grep "Copyright" "${tld}/NOTICE"
                grep -q "Copyright.*${year}.*Apache Software Foundation" "${tld}/NOTICE"
                #check that the declared license is of the correct type
                tar xzf $file "${tld}/LICENSE"
                head -1 "${tld}/LICENSE" | grep -q "Apache License"
              }
              verifyArtifactSizeSignatureLicenseNoticeAndCopyright apache-geode-${VERSION}-src 10000000 30000000
              verifyArtifactSizeSignatureLicenseNoticeAndCopyright apache-geode-${VERSION} 100000000 150000000
              verifyArtifactSizeSignatureLicenseNoticeAndCopyright apache-geode-examples-${VERSION}-src 50000 2000000
              verifyArtifactSizeSignatureLicenseNoticeAndCopyright apache-geode-native-${VERSION}-src 2000000 4000000
              verifyArtifactSizeSignatureLicenseNoticeAndCopyright apache-geode-benchmarks-${VERSION}-src 50000 500000
              curl -L -s ${url}/ | awk '/>..</{next}/<li>/{gsub(/ *<[^>]*>/,"");print}' | sort > actual-file-list
              sort < exp > expected-file-list
              set +x
              echo ""
              if diff -q expected-file-list actual-file-list ; then
                echo "The file list at $url matches what is expected and all signatures were verified :)"
              else
                echo "Expected:"
                cat expected-file-list
                echo ""
                echo "Actual:"
                cat actual-file-list
                echo ""
                echo "Diff:"
                diff expected-file-list actual-file-list
                exit 1
              fi
  - name: verify-no-binaries
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode
          platform: linux
          run:
            path: /bin/bash
            args:
            - -ec
            - |
              set -e
              apt update -q
              apt install -qq -y --no-install-recommends git
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              url=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}
              BINARY_EXTENSIONS="jar|war|class|exe|dll|o|so|obj|bin|out|pyc"
              echo "Source artifacts should not contain any files ending in$(echo "|${BINARY_EXTENSIONS}"|sed 's/[^a-z]/ ./g')"
              echo ""
              function verifyNoBinaries {
                file=$1
                echo ""
                echo Checking $file...
                curl -L -s $url/$file | tar tvzf - | egrep '\.('"${BINARY_EXTENSIONS}"')$' | tee -a bins
              }
              verifyNoBinaries apache-geode-${VERSION}-src.tgz
              verifyNoBinaries apache-geode-examples-${VERSION}-src.tgz
              verifyNoBinaries apache-geode-native-${VERSION}-src.tgz
              verifyNoBinaries apache-geode-benchmarks-${VERSION}-src.tgz
              echo ""
              echo ""
              if grep -q . bins ; then
                echo Binary files were found!
                exit 1
              else
                echo All good
              fi
  - name: verify-license
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
        - get: geode-develop
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: bellsoft/liberica-openjdk-debian
              tag: 8
          inputs:
            - name: geode
            - name: geode-develop
          platform: linux
          run:
            path: /bin/bash
            args:
            - -ec
            - |
              set -e
              apt update -q
              apt install -qq -y --no-install-recommends unzip git
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              ./geode-develop/dev-tools/release/license_review.sh -v $FULL_VERSION
  - name: all-passed
    serial: true
    public: true
    plan:
      - in_parallel:
        - get: geode
          trigger: true
          passed:
            - verify-license
            - upthewaterspout
            - run-gfsh-from-tgz
            - verify-no-binaries
            - build-geode-from-tag
            - build-geode-from-src-tgz
            - verify-expected-files-and-keys
        - get: geode-examples
          passed:
            - run-geode-examples-jdk11
            - run-geode-examples-from-src-tgz-jdk8
        - get: geode-native
          passed:
            - build-geode-native-from-tag
            - build-geode-native-from-src-tgz
        - get: geode-benchmarks
          passed:
            - benchmarks-test
EOF
fly -t concourse.apachegeode-ci.info-main login --team-name main --concourse-url https://concourse.apachegeode-ci.info/
fly -t concourse.apachegeode-ci.info-main set-pipeline -p apache-support-${VERSION_MM//./-}-rc -c $PIPEYML
fly -t concourse.apachegeode-ci.info-main unpause-pipeline -p apache-support-${VERSION_MM//./-}-rc
fly -t concourse.apachegeode-ci.info-main expose-pipeline -p apache-support-${VERSION_MM//./-}-rc
rm $PIPEYML
