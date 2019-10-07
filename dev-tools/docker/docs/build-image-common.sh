#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -u

export DOCKER_ENV_VERSION="0.1"

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#Stupid OSX has a different mktemp command
TMP_DIR=`mktemp -d 2>/dev/null || mktemp -d -t 'geodedocs'`

function cleanup() {
  rm -rf $TMP_DIR
}

trap cleanup EXIT

IMAGE_NAME="geode/docsbuild:${DOCKER_ENV_VERSION}"

pushd ${TMP_DIR} 1> /dev/null
cp $SCRIPT_DIR/Dockerfile .
cp $SCRIPT_DIR/../../../geode-book/Gemfile* .

echo "Building ${IMAGE_NAME} image..."
docker build -q -t ${IMAGE_NAME} .

popd 1> /dev/null

if [ "$(uname -s)" == "Linux" ]; then
  USER_NAME=${SUDO_USER:=$USER}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
else # boot2docker uid and gid
  USER_NAME=$USER
  USER_ID=1000
  GROUP_ID=50
fi

echo "Building ${IMAGE_NAME}-${USER_NAME} image..."
docker build -q -t "${IMAGE_NAME}-${USER_NAME}" - <<UserSpecificDocker
FROM ${IMAGE_NAME} 
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME}
ENV HOME /home/${USER_NAME}
UserSpecificDocker

# Go to root
pushd ${SCRIPT_DIR}/../../../ 1> /dev/null
