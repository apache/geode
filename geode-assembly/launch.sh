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

/geode/bin/gfsh -e "start locator --name=locator1 --port=10334" -e "configure pdx --read-serialized=true" -e "start server --name=server --locators=localhost[10334] --redis-port=6379 --memcached-port=5678 --server-port=40404 --http-service-port=9090 --start-rest-api"

while :
do
	sleep 60
done
