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

## Parsing functions for the Concourse Semver resource.
## These functions expect one input in the form of the resource file, e.g., "1.14.0-build.325"
get-geode-version() {
  local CONCOURSE_VERSION=$1
  # Prune all after '-', yielding e.g., "1.14.0"
  local GEODE_PRODUCT_VERSION=${CONCOURSE_VERSION%%-*}
  (>&2 echo "Geode product VERSION is ${GEODE_PRODUCT_VERSION}")
  echo ${GEODE_PRODUCT_VERSION}
}

get-geode-version-qualifier-slug() {
  local CONCOURSE_VERSION=$1
  # Prune all before '-', yielding e.g., "build.325"
  local CONCOURSE_BUILD_SLUG=${CONCOURSE_VERSION##*-}
  # Prune all before '.', yielding e.g., "build"
  local QUALIFIER_SLUG=${CONCOURSE_BUILD_SLUG%%.*}
  echo ${QUALIFIER_SLUG}
}

get-geode-build-id() {
  local CONCOURSE_VERSION=$1
  # Prune all before the last '.', yielding e.g., "325"
  local BUILD_ID=${CONCOURSE_VERSION##*.}
  echo ${BUILD_ID}
}

get-geode-build-id-padded() {
  local CONCOURSE_VERSION=$1
  local BUILD_ID=$(get-geode-build-id ${CONCOURSE_VERSION})
  # Prune all before the last '.', yielding e.g., "325", then zero-pad, e.g., "0325"
  local PADDED_BUILD_ID=$(printf "%04d" ${BUILD_ID})
  (>&2 echo "Build ID is ${PADDED_BUILD_ID}")
  echo ${PADDED_BUILD_ID}
}

get-full-version() {
  # Extract each component so that the BuildId can be zero-padded, then reassembled.
  local CONCOURSE_VERSION=$1
  local GEODE_PRODUCT_VERSION=$(get-geode-version ${CONCOURSE_VERSION})
  local QUALIFIER_SLUG=$(get-geode-version-qualifier-slug ${CONCOURSE_VERSION})
  local PADDED_BUILD_ID=$(get-geode-build-id-padded ${CONCOURSE_VERSION})
  local FULL_PRODUCT_VERSION="${GEODE_PRODUCT_VERSION}-${QUALIFIER_SLUG}.${PADDED_BUILD_ID}"
  (>&2 echo "Full product VERSION is ${FULL_PRODUCT_VERSION}")
  echo ${FULL_PRODUCT_VERSION}
}

get_geode_pr_exclusion_dirs() {
  local exclude_dirs="ci dev-tools etc geode-book geode-docs"
  echo "${exclude_dirs}"
}

is_source_from_pr_testable() {
  if [[ $# -ne 2 ]]; then
    >&2 echo "Invalid args. Try ${0} \"<repo_path>\" \"<list of exclusion dirs>\""
    exit 1
  fi
  local repo_dir="${1}"
  if [[ ! -d "${repo_dir}" ]]; then
    # If the repo_dir does not exist, assume call from non-PR
    return 0;
  fi

  pushd "${repo_dir}" 2>&1 >> /dev/null
    local base_dir=$(git rev-parse --show-toplevel)
    local github_pr_dir="${base_dir}/.git/resource"
    pushd ${base_dir} 2>&1 >> /dev/null
      local return_code=0
      if [ -d "${github_pr_dir}" ]; then
        # Modify this path list with directories to exclude
        local exclude_dirs="${2}"
        for d in $(echo ${exclude_dirs}); do
          local exclude_pathspec="${exclude_pathspec} :(exclude,glob)${d}/**"
        done
        pushd ${base_dir} &> /dev/null
          local files=$(git diff --name-only $(cat "${github_pr_dir}/base_sha") $(cat "${github_pr_dir}/head_sha") -- . $(echo ${exclude_pathspec}))
        popd &> /dev/null
        if [[ -z "${files}" ]]; then
          >&2 echo "Code changes are from CI only"
          return_code=1
        else
          >&2 echo "real code change here!"
        fi
      else
        >&2 echo "repo is not from a PR"
      fi
    popd 2>&1 >> /dev/null
  popd 2>&1 >> /dev/null
  return ${return_code}
}
