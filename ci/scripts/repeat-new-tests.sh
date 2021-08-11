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
while [[ -h "$SOURCE" ]]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

. ${SCRIPTDIR}/shared_utilities.sh

is_source_from_pr_testable "geode" "$(get_geode_pr_exclusion_dirs)" || exit 0

function changes_for_path() {
  pushd geode >> /dev/null
    local path="$1" # only expand once in the line below
    # .git/resource/metadata.json is provided by the github-pr-resource used in Concourse
    local mergeBase=$(cat .git/resource/metadata.json |
                      jq -r -c '.[]| select(.name == "base_sha") | .value') || exit $?

    if [[ "${mergeBase}" == "" ]]; then
      echo "Could not determine merge base. Exiting..."
      exit 1
    fi
    git diff --name-only --diff-filter=ACMR ${mergeBase} -- $path
  popd >> /dev/null
}

function save_classpath() {
  echo "Building and saving classpath"
  pushd geode >> /dev/null
    BUILD_LOG=/tmp/classpath-build.log
    # Do this twice since devBuild still dumps a warning string to stdout.
    ./gradlew --console=plain -q compileTestJava compileIntegrationTestJava compileDistributedTestJava devBuild >${BUILD_LOG} 2>&1 || (cat ${BUILD_LOG}; false)
    ./gradlew --console=plain -q printTestClasspath 2>/dev/null >/tmp/classpath.txt
  popd >> /dev/null
}

function create_gradle_test_targets() {
  echo $(${JAVA_HOME}/bin/java -cp $(cat /tmp/classpath.txt) org.apache.geode.test.util.StressNewTestHelper $@)
}

UNIT_TEST_CHANGES=$(changes_for_path ':(glob)**/src/test/java/**') || exit $?
INTEGRATION_TEST_CHANGES=$(changes_for_path ':(glob)**/src/integrationTest/java/**') || exit $?
DISTRIBUTED_TEST_CHANGES=$(changes_for_path ':(glob)**/src/distributedTest/java/**') || exit $?
ACCEPTANCE_TEST_CHANGES=$(changes_for_path ':(glob)**/src/acceptanceTest/java/**') || exit $?
UPGRADE_TEST_CHANGES=$(changes_for_path ':(glob)**/src/upgradeTest/java/**') || exit $?

CHANGED_FILES_ARRAY=( $UNIT_TEST_CHANGES $INTEGRATION_TEST_CHANGES $DISTRIBUTED_TEST_CHANGES $ACCEPTANCE_TEST_CHANGES $UPGRADE_TEST_CHANGES )
NUM_CHANGED_FILES=${#CHANGED_FILES_ARRAY[@]}

echo "${NUM_CHANGED_FILES} changed test files"
for T in ${CHANGED_FILES_ARRAY[@]}; do
  echo "  ${T}"
done

if [[  "${NUM_CHANGED_FILES}" -eq 0 ]]
then
  echo "No changed test files, nothing to test."
  exit 0
fi

save_classpath

TEST_TARGETS=$(create_gradle_test_targets ${CHANGED_FILES_ARRAY[@]})
TEST_COUNT=$(echo ${TEST_TARGETS} | sed -e 's/.*testCount=\([0-9]*\).*/\1/g')

if [[ "${NUM_CHANGED_FILES}" -ne "${TEST_COUNT}" ]]
then
  echo ""
  echo "${TEST_COUNT} test files considered for stress test after pre-processing the initial set"
fi

if [[ "${TEST_COUNT}" -gt 35 ]]
then
  echo "${TEST_COUNT} is too many changed tests to stress test. Allowing this job to pass without stress testing."
  exit 0
fi

export GRADLE_TASK="compileTestJava compileIntegrationTestJava compileDistributedTestJava $TEST_TARGETS"
export GRADLE_TASK_OPTIONS="-Prepeat=50 -PfailOnNoMatchingTests=false"

echo "GRADLE_TASK_OPTIONS=${GRADLE_TASK_OPTIONS}"
echo "GRADLE_TASK=${GRADLE_TASK}"

${SCRIPTDIR}/execute_tests.sh

