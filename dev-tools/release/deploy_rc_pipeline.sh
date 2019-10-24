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
    echo "  -v   The #.#.# version number"
    exit 1
}

VERSION=""

while getopts ":v:" opt; do
  case ${opt} in
    v )
      VERSION=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${VERSION} == "" ]]; then
    usage
fi

if [[ $VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${VERSION}. Example valid version: 1.9.0"
    exit 1
fi

PIPEYML=$PWD/rc-pipeline.yml
cat << "EOF" | sed -e "s/<VERSION>/${VERSION}/" > $PIPEYML
---

resources:
- name: geode
  type: git
  source:
    branch: release/<VERSION>
    tag_filter: rel/v<VERSION>.RC*
    uri: https://github.com/apache/geode.git
- name: geode-examples
  type: git
  source:
    branch: release/<VERSION>
    uri: https://github.com/apache/geode-examples.git
- name: geode-native
  type: git
  source:
    branch: release/<VERSION>
    tag_filter: rel/v<VERSION>.RC*
    uri: https://github.com/apache/geode-native.git
- name: upthewaterspout-tests
  type: git
  source:
    branch: master
    uri: https://github.com/apache/geode.git

jobs:
  - name: build-geode-from-tag
    serial: true
    plan:
      - aggregate:
          - get: geode
            trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
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
              java -version
              cd geode
              ./gradlew test
  - name: build-geode-from-src-tgz
    serial: true
    plan:
      - aggregate:
          - get: geode
            trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
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
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              curl -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-${VERSION}-src.tgz > src.tgz
              tar xzf src.tgz
              cd apache-geode-${VERSION}-src
              java -version
              ./gradlew test
  - name: run-geode-examples
    serial: true
    plan:
      - aggregate:
          - get: geode-examples
            trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
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
              cd geode-examples
              java -version
              ./gradlew runAll
  - name: run-geode-examples-from-src-tar-gz
    serial: true
    plan:
      - aggregate:
          - get: geode-examples
            trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
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
              FULL_VERSION=$(cat geode-examples/gradle.properties | grep geodeReleaseUrl | sed -e 's#.*/geode/##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              STAGING_MAVEN=$(cat geode-examples/gradle.properties | grep geodeRepositoryUrl | awk '{print $3}')
              curl -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-examples-${VERSION}.tar.gz > src.tgz
              tar xzf src.tgz
              cd apache-geode-examples-${VERSION}
              java -version
              ./gradlew -PgeodeReleaseUrl=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION} -PgeodeRepositoryUrl=${STAGING_MAVEN} build runAll
  - name: run-geode-examples-from-src-zip-11
    serial: true
    plan:
      - aggregate:
          - get: geode-examples
            trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
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
              FULL_VERSION=$(cat geode-examples/gradle.properties | grep geodeReleaseUrl | sed -e 's#.*/geode/##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              STAGING_MAVEN=$(cat geode-examples/gradle.properties | grep geodeRepositoryUrl | awk '{print $3}')
              curl -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-examples-${VERSION}.zip > src.zip
              unzip src.zip
              cd apache-geode-examples-${VERSION}
              java -version
              ./gradlew -PgeodeReleaseUrl=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION} -PgeodeRepositoryUrl=${STAGING_MAVEN} build runAll
  - name: build-geode-native-from-tag
    serial: true
    plan:
      - aggregate:
          - get: geode-native
            trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
              tag: 8
          inputs:
            - name: geode-native
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              FULL_VERSION=$(cd geode-native && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              #use geode from binary dist
              curl -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-${VERSION}.tgz > geode-bin.tgz
              tar xzf geode-bin.tgz
              # needed to get cmake >= 3.12
              echo 'APT::Default-Release "stable";' >> /etc/apt/apt.conf.d/99defaultrelease
              echo 'deb     http://ftp.de.debian.org/debian/    stable main contrib non-free' >> /etc/apt/sources.list.d/stable.list
              echo 'deb-src http://ftp.de.debian.org/debian/    stable main contrib non-free' >> /etc/apt/sources.list.d/stable.list
              echo 'deb     http://security.debian.org/         stable/updates  main contrib non-free' >> /etc/apt/sources.list.d/stable.list
              apt-get update
              DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y cmake openssl doxygen build-essential libssl-dev zlib1g-dev
              cd geode-native
              mkdir build
              cd build
              cmake .. -DGEODE_ROOT=$PWD/../../apache-geode-${VERSION}
              cmake --build . -- -j 4
              cmake --build . --target docs -- -j 4
              cmake --build . --target install -- -j 4
  - name: build-geode-native-from-src-tar-gz
    serial: true
    plan:
      - aggregate:
          - get: geode-native
            trigger: true
          - get: geode
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
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
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              # build geode from source
              cd geode
              ./gradlew build -x test -x javadoc -x rat -x pmdMain
              cd ..
              # needed to get cmake >= 3.12
              echo 'APT::Default-Release "stable";' >> /etc/apt/apt.conf.d/99defaultrelease
              echo 'deb     http://ftp.de.debian.org/debian/    stable main contrib non-free' >> /etc/apt/sources.list.d/stable.list
              echo 'deb-src http://ftp.de.debian.org/debian/    stable main contrib non-free' >> /etc/apt/sources.list.d/stable.list
              echo 'deb     http://security.debian.org/         stable/updates  main contrib non-free' >> /etc/apt/sources.list.d/stable.list
              apt-get update
              DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y cmake openssl doxygen build-essential libssl-dev zlib1g-dev
              curl -s https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/apache-geode-native-${VERSION}-src.tar.gz > src.tgz
              tar xzf src.tgz
              cd apache-geode-native*
              mkdir build
              cd build
              cmake .. -DGEODE_ROOT=$PWD/../../geode/geode-assembly/build/install/apache-geode
              cmake --build . -- -j 4
              cmake --build . --target docs -- -j 4
              cmake --build . --target install -- -j 4
  - name: upthewaterspout
    serial: true
    plan:
      - aggregate:
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
              repository: openjdk
              tag: 8
          inputs:
            - name: upthewaterspout-tests
            - name: geode-examples
          platform: linux
          run:
            path: /bin/sh
            args:
            - -ec
            - |
              set -ex
              FULL_VERSION=$(cd geode-examples && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              STAGING_MAVEN=$(cat geode-examples/gradle.properties | grep geodeRepositoryUrl | awk '{print $3}')
              cd upthewaterspout-tests
              curl -s https://dist.apache.org/repos/dist/dev/geode/KEYS > KEYS
              gpg --import KEYS
              java -version
              ./gradlew build -PmavenURL=${STAGING_MAVEN} -PdownloadURL=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/ -Pversion=${FULL_VERSION}
  - name: verify-keys
    serial: true
    plan:
      - aggregate:
          - get: geode
            trigger: true
      - task: validate
        timeout: 1h
        config:
          image_resource:
            type: docker-image
            source:
              repository: openjdk
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
              FULL_VERSION=$(cd geode && git describe --tags | sed -e 's#^rel/v##')
              VERSION=$(echo $FULL_VERSION|sed -e 's/\.RC.*//')
              curl -s https://dist.apache.org/repos/dist/dev/geode/KEYS > KEYS
              gpg --import KEYS
              url=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}
              function verifyArtifactSignature {
                file=$1
                echo Verifying $file...
                asc=${file}.asc
                sha=${file}.sha$2
                sum=sha${2}sum
                curl -s $url/$file > $file
                curl -s $url/$asc > $asc
                curl -s $url/$sha > $sha
                gpg --verify $asc
                $sum -c $sha
              }
              verifyArtifactSignature apache-geode-${VERSION}-src.tgz 256 
              verifyArtifactSignature apache-geode-${VERSION}.tgz 256
              verifyArtifactSignature apache-geode-examples-${VERSION}.tar.gz 256
              verifyArtifactSignature apache-geode-examples-${VERSION}.zip 256
              verifyArtifactSignature apache-geode-native-${VERSION}-src.tar.gz 512
EOF
fly -t concourse.apachegeode-ci.info login --concourse-url https://concourse.apachegeode-ci.info/
fly -t concourse.apachegeode-ci.info set-pipeline -p apache-release-${VERSION//./-}-rc -c $PIPEYML
rm $PIPEYML
