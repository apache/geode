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
$ErrorActionPreference = "Stop"
Import-Module Packer

Install-Package http://downloads.activestate.com/ActivePerl/releases/5.22.3.2204/ActivePerl-5.22.3.2204-MSWin32-x64-401627.exe `
    -Hash 214083166032FBF61F20BB4C65B0610508B90101910F3293B67BD56619E6444D `
    -ArgumentList @("/qn", "/norestart", "PERL_PATH=Yes", "PERL_EXT=Yes")
