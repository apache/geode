#!/bin/bash

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

function cleanup {
  rm Gemfile Gemfile.lock
  rm -r geode-book geode-docs
}

trap cleanup EXIT

set -x -e

mkdir -p geode-book
mkdir -p geode-docs

cp ../../../geode-book/Gemfile* .
cp -r ../../../geode-book geode-book
cp -r ../../../geode-docs geode-docs

docker build -t geodedocs/temp:1.0 .

docker run -it -p 9292:9292 geodedocs/temp:1.0 /bin/bash -c "cd geode-book && bundle exec bookbinder bind local && cd final_app && bundle exec rackup --host=0.0.0.0"
