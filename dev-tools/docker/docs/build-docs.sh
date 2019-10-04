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

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

. $SCRIPT_DIR/build-image-common.sh

REPO_PATH=${SCRIPT_DIR}/../../../

echo "Running Bookbinder inside Docker container to generate documentation..."
echo "  Complete log can be found in ${SCRIPT_DIR}/build-docs-output.txt"

docker run -i -t \
  --rm=true \
  -w "${REPO_PATH}/geode-book" \
  -v "$PWD:${REPO_PATH}" \
  ${IMAGE_NAME}-${USER_NAME} \
  /bin/bash -c "bundle exec bookbinder bind local &> ${SCRIPT_DIR}/build-docs-output.txt"

SUCCESS=$(grep "Bookbinder bound your book into" ${SCRIPT_DIR}/build-docs-output.txt)

if [[ "${SUCCESS}" == "" ]];then
  echo "Something went wrong while generating documentation, check log."
else
  echo ${SUCCESS}
fi

docker run -i -t \
  --rm=true \
  -w "${REPO_PATH}/geode-book" \
  -v "$PWD:${REPO_PATH}" \
  ${IMAGE_NAME}-${USER_NAME} \
  /bin/bash -c "chown -R ${USER_ID}:${GROUP_ID} ${REPO_PATH}/geode-book/output ${REPO_PATH}/geode-book/final_app"

popd 1> /dev/null

