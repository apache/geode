#!/usr/bin/env bash

#
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

cd ..

# We are currently using a patched version of this repo because our code does not implement all
# Redis commands.  Once all commands needed to run relevant test files are implemented, we hope to
# use Redis's repo without a patch.
git clone --config transfer.fsckObjects=false https://github.com/redis/redis.git
REDIS_PATCH=${PWD}/geode-redis/src/acceptanceTest/resources/0001-configure-redis-tests.patch
cd redis
git checkout origin/5.0
git apply ${REDIS_PATCH}

export JAVA_HOME=${JAVA_TEST_PATH}

../geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server1 \
  --compatible-with-redis-port=6380 \
  --compatible-with-redis-bind-address=127.0.0.1 \
  --compatible-with-redis-password=foobar

failCount=0

./runtest --host 127.0.0.1 --port 6380 --single unit/auth

((failCount += $?))


../geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server2 \
  --server-port=0 \
  --compatible-with-redis-port=6379 \
  --compatible-with-redis-bind-address=127.0.0.1

./runtest --host 127.0.0.1 --port 6379 --single unit/type/set --single unit/expire \
--single unit/type/hash --single unit/type/string \
--single unit/quit --single unit/pubsub

((failCount += $?))

exit ${failCount}
