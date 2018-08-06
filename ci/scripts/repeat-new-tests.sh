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

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

CHANGED_FILES=$(cd geode && git diff --name-only HEAD $(git merge-base HEAD develop) | grep -e 'src/test/java\|src/integrationTest/java\|src/distributedTest/java')

TESTS_FLAG=""

if [ ${#CHANGED_FILES[@]} -gt 50 ] 
  then
    echo "Too many changed tests to stress test. Allowing this job to pass without stress testing."
    exit 0
fi

for FILENAME in $CHANGED_FILES ; do
  SHORT_NAME=$(basename $FILENAME)
  SHORT_NAME="${SHORT_NAME%.java}"
  TESTS_FLAG="$TESTS_FLAG --tests *$SHORT_NAME"
done

export GRADLE_TASK=repeatTest
export GRADLE_TASK_OPTIONS="-Prepeat=200 -PfailOnNoMatchingTests=false $TESTS_FLAG"

echo "GRADLE_TASK_OPTIONS=${GRADLE_TASK_OPTIONS}"

${SCRIPTDIR}/execute_tests.sh

