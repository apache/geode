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
}

trap cleanup EXIT

# Gemfile & Gemfile.lock are copied to avoid including the whole
# geode-book folder to the image context
cp ../../../geode-book/Gemfile* .

docker build -t geodedocs/temp:1.0 .

# "geode-book/final_app" and "geode-book/output" are created inside the container,
# so it is necessary to use the current user to avoid these folders owned by
# root user.
GEODE_BOOK="$(pwd)/../../../geode-book"
GEODE_DOCS="$(pwd)/../../../geode-docs"
export UID=$(id -u)
export GID=$(id -g)
docker run -it -p 9292:9292 --user $UID:$GID \
    --workdir="/home/$USER" \
    --volume="/etc/group:/etc/group:ro" \
    --volume="/etc/passwd:/etc/passwd:ro" \
    --volume="/etc/shadow:/etc/shadow:ro" \
    --volume="${GEODE_BOOK}:/geode-book:rw" \
    --volume="${GEODE_DOCS}:/geode-docs:rw" \
    geodedocs/temp:1.0 /bin/bash -c "cd /geode-book && bundle exec bookbinder bind local && cd final_app && bundle exec rackup --host=0.0.0.0"

# Bookbinder creates the following links
# <your geode repo>/geode-book/output/master_middleman/source/docs/guide/115 -> /geode-book/output/preprocessing/sections/docs/guide/115
# <your geode repo>/geode-book/output/preprocessing/sections/docs/guide/115 -> /geode-docs
#
# Following lines fix these wrong symbolic links:
#
ug_version=`ls ${GEODE_BOOK}/final_app/public/docs/guide/`
master_middleman_folder="${GEODE_BOOK}/output/master_middleman/source/docs/guide/${ug_version}"
preprocessing_folder="${GEODE_BOOK}/output/preprocessing/sections/docs/guide/${ug_version}"
rm ${master_middleman_folder}
rm ${preprocessing_folder}

ln -s ${GEODE_DOCS} ${preprocessing_folder}
ln -s ${preprocessing_folder} ${master_middleman_folder}

