#!/usr/bin/env bash

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

set -x -e -o pipefail

# Remove meta-packages that prevent installation of updated tools
pkg uninstall -v entire consolidation/userland/userland-incorporation consolidation/java-8/java-8-incorporation

# Install required tools
pkg install -v --accept \
    system/header \
    developer/assembler \
    developer/documentation-tool/doxygen \
    developer/java/jdk-8 \
    text/gnu-patch

# broken     developer/build/gnu-make \


# too old developer/build/cmake
/opt/csw/bin/pkgutil -i -y cmake gmake gtar

# dependent perl package conflict developer/versioning/git
/opt/csw/bin/pkgutil -i -y git
