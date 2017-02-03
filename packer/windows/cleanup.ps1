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
# Remove admin ssh keys
Remove-Item C:\Users\Administrator\.ssh -Recurse -Force -ErrorAction SilentlyContinue

# Cleanup temp
Get-ChildItem $env:tmp -Recurse | Remove-Item -Recurse -force -ErrorAction SilentlyContinue
Get-ChildItem ([environment]::GetEnvironmentVariable("temp","machine")) -Recurse| Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Cleanup Ec2ConfigService
Remove-Item $Env:ProgramFiles\Amazon\Ec2ConfigService\Scripts\UserScript.ps1 -Force -ErrorAction SilentlyContinue
Remove-Item $Env:ProgramFiles\Amazon\Ec2ConfigService\Logs -Recurse -Force -ErrorAction SilentlyContinue

# TODO debuggin
exit 0

# Cleanup Windows SxS
Dism.exe /online /Cleanup-Image /StartComponentCleanup

# Cleanup Event Log
Get-EventLog -List | foreach {Clear-EventLog -Log $_.Log}

# Defrag
defrag.exe C: /h /u /v /x
