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

set -e -u

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

. $SCRIPT_DIR/build-image-common.sh

REPO_PATH=${SCRIPT_DIR}/../../../

echo "Starting up web server..."
docker run -i -t \
  --rm=true \
  -w "${REPO_PATH}/geode-book/final_app/" \
  -v "$PWD:${REPO_PATH}" \
  -p 127.0.0.1:9292:9292 \
  ${IMAGE_NAME}-${USER_NAME} \
  /bin/bash -c "bundle install; rackup --host 0.0.0.0"

popd 1> /dev/null

