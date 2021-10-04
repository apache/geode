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
REDIS_PATCH=${PWD}/geode-for-redis/src/acceptanceTest/resources/0001-configure-redis-tests.patch
cd redis
git checkout origin/5.0
git apply ${REDIS_PATCH}

export JAVA_HOME=${JAVA_TEST_PATH}

../geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-unsupported-commands=true \
  --name=server1 \
  --geode-for-redis-port=6380 \
  --geode-for-redis-bind-address=127.0.0.1 \
  --geode-for-redis-username=foobar \
  --server-port=0 \
  --J=-Dgemfire.security-manager=org.apache.geode.examples.SimpleSecurityManager \
  --J=-Dgemfire.jmx-manager=true \
  --J=-Dgemfire.jmx-manager-start=true \
  --J=-Dgemfire.jmx-manager-port=1099

# This will cause all buckets to be created
../geode-assembly/build/install/apache-geode/bin/gfsh -e "connect --jmx-manager=localhost[1099]" \
  -e "query --query='select count(*) from /REDIS_DATA'"

failCount=0

./runtest --host 127.0.0.1 --port 6380 --single unit/auth

((failCount += $?))

../geode-assembly/build/install/apache-geode/bin/gfsh stop server --dir=server1

../geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-unsupported-commands=true \
  --name=server2 \
  --server-port=0 \
  --geode-for-redis-port=6379 \
  --geode-for-redis-bind-address=127.0.0.1 \
  --J=-Dgemfire.jmx-manager=true \
  --J=-Dgemfire.jmx-manager-start=true \
  --J=-Dgemfire.jmx-manager-port=1099

# This will cause all buckets to be created
../geode-assembly/build/install/apache-geode/bin/gfsh -e "connect --jmx-manager=localhost[1099]" \
  -e "query --query='select count(*) from /REDIS_DATA'"

./runtest --host 127.0.0.1 --port 6379 \
--single unit/type/set \
--single unit/expire \
--single unit/type/hash \
--single unit/type/string \
--single unit/type/zset \
--single unit/quit \
--single unit/pubsub \
--single unit/dump

((failCount += $?))

exit ${failCount}
