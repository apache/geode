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

# Chocolatey broken
#choco install cygwin cyg-get -confirm
#cyg-get git make
#exit 0

Import-Module Packer

Install-Package https://cygwin.com/setup-x86_64.exe `
    -Hash 08079A13888B74F6466DEF307A687E02CB26FC257EA2FA78D40F02E28330FD56 `
    -ArgumentList @(
        "--quiet-mode",
        "--verbose",
        "--site", "http://cygwin.osuosl.org", 
        "--packages", "git,patch,make"
        )

# Set Cygwin home to Windows profile
Add-Content C:\cygwin64\etc\nsswitch.conf "db_home: /%H"

# Remove Perl to avoid conflicts with AvtivePerl
Remove-Item -Path C:\cygwin64\bin\perl.exe
