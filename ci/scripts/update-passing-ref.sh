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
#


export BUILDROOT=$(pwd)
REPOSITORY_DIR=$(pwd)/geode
LOCAL_FILE=${BUILDROOT}/results/passing.txt
DESTINATION_URL=gs://${PUBLIC_BUCKET}/${MAINTENANCE_VERSION}/passing.txt
pushd ${REPOSITORY_DIR}
git rev-parse HEAD > ${LOCAL_FILE}
popd
echo "Updating passing reference file for ${MAINTENANCE_VERSION} to the following SHA:"
cat ${LOCAL_FILE}
gsutil -q -m cp ${LOCAL_FILE} ${DESTINATION_URL}
gsutil setmeta -h "Cache-Control:no-cache" ${DESTINATION_URL}
