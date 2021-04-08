#!/usr/bin/env bash

#Licensed to the Apache Software Foundation (ASF) under one or more contributor license
#agreements. See the NOTICE file distributed with this work for additional information regarding
#copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance with the License. You may obtain a
#copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software distributed under the License
#is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#or implied. See the License for the specific language governing permissions and limitations under
#the License.

while getopts ":b:c:" opt; do
  case ${opt} in
  b)
    BASELINE_COMMIT=${OPTARG}
    ;;
  c)
    COMPARISON_COMMIT=${OPTARG}
    ;;
  \?)
    echo "Usage: ${0} -b BASELINE_COMMIT -c COMPARISON_COMMIT"
    exit 0
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

if [ -z ${BASELINE_COMMIT} ] || [ -z ${COMPARISON_COMMIT} ]; then
  echo "Must specify both BASELINE_COMMIT and COMPARISON_COMMIT. Shame on you."
  exit 1
fi

echo "BASELINE_COMMIT: ${BASELINE_COMMIT}"
echo "COMPARISON_COMMIT: ${COMPARISON_COMMIT}"

ORIGINAL_BRANCH=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
echo " ORIGINAL_BRANCH: ${ORIGINAL_BRANCH}"

git checkout ${BASELINE_COMMIT}
RETURN_CODE=$?
 if [[ ${RETURN_CODE} -ne 0 ]] ; then
  echo "Please stash any uncommitted changes before using this script."
  exit 1
fi

FILE_PREFIX=$(git rev-parse --short HEAD)
bash environment-setup.sh -g -f ${FILE_PREFIX}

git checkout ${COMPARISON_COMMIT}

FILE_PREFIX=$(git rev-parse --short HEAD)
bash environment-setup.sh -g -f ${FILE_PREFIX}

git checkout ${ORIGINAL_BRANCH}
