#!/bin/bash
#
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
#

## check if locator port has been set otherwise set to default
export GEODE_LOCATOR_PORT="${GEODE_LOCATOR_PORT:-10334}"

## check if GEODE_HOME has been set
: ${GEODE_HOME?"GEODE_HOME enviroment variable needs to be set"}

## check if gfsh script is accessible and print version
: ${GEODE_HOME/bin/gfsh?"gfsh doesn't seem to be available. Please check $GEODE_HOME"}
echo "Geode version: `$GEODE_HOME/bin/gfsh version`"

## prefer GEODE_HOME for finding gfsh
export PATH=$GEODE_HOME/bin:$PATH


: ${GEODE_LOCATOR_PORT?}
