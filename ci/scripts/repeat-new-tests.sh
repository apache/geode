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
    git diff --name-only ${mergeBase} -- $path
  popd >> /dev/null
}

function save_classpath() {
  echo "Building and saving classpath"
  pushd geode >> /dev/null
    # Do this twice since devBuild still dumps a warning string to stdout.
    ./gradlew --console=plain -q devBuild 2>/dev/null
    ./gradlew --console=plain -q printTestClasspath 2>/dev/null >/tmp/classpath.txt
  popd >> /dev/null
}

function expand_abstract_classes() {
  echo $(${JAVA_HOME}/bin/java -cp $(cat /tmp/classpath.txt) org.apache.geode.test.util.ClassScanner $@)
}

UNIT_TEST_CHANGES_TMP=$(changes_for_path '*/src/test/java') || exit $?
INTEGRATION_TEST_CHANGES_TMP=$(changes_for_path '*/src/integrationTest/java') || exit $?
DISTRIBUTED_TEST_CHANGES_TMP=$(changes_for_path '*/src/distributedTest/java') || exit $?
ACCEPTANCE_TEST_CHANGES_TMP=$(changes_for_path '*/src/acceptanceTest/java') || exit $?
UPGRADE_TEST_CHANGES_TMP=$(changes_for_path '*/src/upgradeTest/java') || exit $?

CHANGED_FILES_ARRAY=( $UNIT_TEST_CHANGES_TMP $INTEGRATION_TEST_CHANGES_TMP $DISTRIBUTED_TEST_CHANGES_TMP $ACCEPTANCE_TEST_CHANGES_TMP $UPGRADE_TEST_CHANGES_TMP )
NUM_CHANGED_FILES=${#CHANGED_FILES_ARRAY[@]}

echo "${NUM_CHANGED_FILES} changed test files"

if [[  "${NUM_CHANGED_FILES}" -eq 0 ]]
then
  echo "No changed test files, nothing to test."
  exit 0
fi

save_classpath

UNIT_TEST_CHANGES=$(expand_abstract_classes $UNIT_TEST_CHANGES_TMP) || exit $?
INTEGRATION_TEST_CHANGES=$(expand_abstract_classes $INTEGRATION_TEST_CHANGES_TMP) || exit $?
DISTRIBUTED_TEST_CHANGES=$(expand_abstract_classes $DISTRIBUTED_TEST_CHANGES_TMP) || exit $?
ACCEPTANCE_TEST_CHANGES=$(expand_abstract_classes $ACCEPTANCE_TEST_CHANGES_TMP) || exit $?
UPGRADE_TEST_CHANGES=$(expand_abstract_classes $UPGRADE_TEST_CHANGES_TMP) || exit $?

CHANGED_FILES_ARRAY=( $UNIT_TEST_CHANGES $INTEGRATION_TEST_CHANGES $DISTRIBUTED_TEST_CHANGES $ACCEPTANCE_TEST_CHANGES $UPGRADE_TEST_CHANGES )
NUM_EXPANDED_FILES=${#CHANGED_FILES_ARRAY[@]}

X=$((NUM_EXPANDED_FILES - NUM_CHANGED_FILES))
if [[ "${X}" -gt 0 ]]
  echo "Including ${X} inferred test classes"
fi

if [[ "${NUM_EXPANDED_FILES}" -gt 25 ]]
then
  echo "${NUM_EXPANDED_FILES} is too many changed tests to stress test. Allowing this job to pass without stress testing."
  exit 0
fi

TEST_TARGETS=""

function append_to_test_targets() {
  local target="$1"
  local files="$2"
  if [[ -n "$files" ]]
  then
    TEST_TARGETS="$TEST_TARGETS $target"
    for FILENAME in $files
    do
      SHORT_NAME=$(basename $FILENAME)
      SHORT_NAME="${SHORT_NAME%.java}"
      TEST_TARGETS="$TEST_TARGETS --tests $SHORT_NAME"
    done
  fi
}

append_to_test_targets "repeatUnitTest" "$UNIT_TEST_CHANGES"
append_to_test_targets "repeatIntegrationTest" "$INTEGRATION_TEST_CHANGES"
append_to_test_targets "repeatDistributedTest" "$DISTRIBUTED_TEST_CHANGES"
append_to_test_targets "repeatUpgradeTest" "$UPGRADE_TEST_CHANGES"

# Acceptance tests cannot currently run in parallel, so do not stress these tests
#append_to_test_targets "repeatAcceptanceTest" "$ACCEPTANCE_TEST_CHANGES"

export GRADLE_TASK="compileTestJava compileIntegrationTestJava compileDistributedTestJava $TEST_TARGETS"
export GRADLE_TASK_OPTIONS="-Prepeat=50 -PfailOnNoMatchingTests=false"

echo "GRADLE_TASK_OPTIONS=${GRADLE_TASK_OPTIONS}"
echo "GRADLE_TASK=${GRADLE_TASK}"

${SCRIPTDIR}/execute_tests.sh

