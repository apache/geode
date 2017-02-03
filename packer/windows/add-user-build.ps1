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
$user = "build"
$pass = "p1votal!"

net.exe user $user $pass /add
net.exe localgroup Administrators $user /add
wmic.exe UserAccount where "Name='$user'" set PasswordExpires=False

$spw = ConvertTo-SecureString $pass -AsPlainText -Force
$cred = New-Object System.Management.Automation.PSCredential -ArgumentList $user,$spw
Start-Process cmd /c -WindowStyle Hidden -Credential $cred -ErrorAction SilentlyContinue


schtasks.exe /Create /TN init-user-build /RU SYSTEM /SC ONSTART /TR "powershell.exe -File 'C:\Users\build\init-user-build.ps1'" 

