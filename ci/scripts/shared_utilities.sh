#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find-here-test-reports() {
  output_directories_file=${1}
  set +e
  find . -type d -name "reports" > ${output_directories_file}
  find .  -type d -name "test-results" >> ${output_directories_file}
  (find . -type d -name "*Test" | grep "build/[^/]*Test$") >> ${output_directories_file}
  find . -name "*-progress*txt" >> ${output_directories_file}
  find . -name "*.hprof" >> ${output_directories_file}
  find . -type d -name "callstacks" >> ${output_directories_file}
  echo "Collecting the following artifacts..."
  cat ${output_directories_file}
  echo ""
}



function sendFailureJobEmail {
  echo "Sending job failure email"

  cat <<EOF >${EMAIL_SUBJECT}
Build for version ${FULL_PRODUCT_VERSION} of Apache Geode failed.
EOF

  cat <<EOF >${EMAIL_BODY}
=================================================================================================

The build job for Apache Geode version ${FULL_PRODUCT_VERSION} has failed.


Build artifacts are available at:
http://${BUILD_ARTIFACTS_DESTINATION}

Test results are available at:
http://${TEST_RESULTS_DESTINATION}


Job: \${ATC_EXTERNAL_URL}/teams/\${BUILD_TEAM_NAME}/pipelines/\${BUILD_PIPELINE_NAME}/jobs/\${BUILD_JOB_NAME}/builds/\${BUILD_NAME}

=================================================================================================
EOF

}