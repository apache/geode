#!/bin/bash

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


if [ -z ${GEMFIRE:-} ]; then
  echo GEMFIRE is not set.
  exit 1
fi

echo Running GemFire Server

export CLASSPATH="${CLASSPATH}:../javaobject.jar"
export PATH="${PATH}:${GEMFIRE}/bin"

if [ ! -d gfecs ]
then
  mkdir gfecs
fi

if [ ! -d gfecs2 ]
then
  mkdir gfecs2
fi

   cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions.xml mcast-port=35673 -dir=gfecs

   cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions2.xml mcast-port=35673 -dir=gfecs2
