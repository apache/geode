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
git clone https://github.com/prettyClouds/redis.git
pushd redis
git checkout tests-geode-redis
popd

JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64" ./geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server1 \
  --redis-port=6380 \
  --redis-bind-address=127.0.0.1 \
  --redis-password=hugz

JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64" ./geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server1 \
  --server-port=0 \
  --redis-port=6379 \
  --redis-bind-address=127.0.0.1

cd redis
./runtest --host 127.0.0.1 --port 6380 --single unit/auth && ./runtest --host 127.0.0.1 --port 6379 --single unit/type/set --single unit/expire
