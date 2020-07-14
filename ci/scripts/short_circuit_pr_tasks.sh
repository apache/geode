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

is_source_from_pr_testable() {
  base_dir=$(git rev-parse --show-toplevel)
  github_pr_dir="${base_dir}/.git/resource"
  exclude_dirs="${@:-ci}"
  for d in $(echo ${exclude_dirs}); do
    local exclude_pathspec="${exclude_pathspec} :(exclude,glob)${d}/**"
  done
  local return_code=0
  if [ -d "${github_pr_dir}" ]; then
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
  return ${return_code}
}
