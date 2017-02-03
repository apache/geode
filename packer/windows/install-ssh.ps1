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
# Install sshd

write-host "Installing OpenSSH"
choco install win32-openssh -version 2016.05.30 -params '"/SSHServerFeature /KeyBasedAuthenticationFeature"' -confirm 

# Chocolatey installer fails to setup permissions correctly via WinRM these scripts until fixed
Push-Location
cd "C:\Program Files\OpenSSH-Win64"
& "C:\Program Files\OpenSSH-Win64\install-sshd.ps1"
Set-Service sshd -StartupType Automatic
Set-Service ssh-agent -StartupType Automatic
& "C:\Program Files\OpenSSH-Win64\install-sshlsa.ps1"
Pop-Location

(Get-Content "C:\Program Files\OpenSSH-Win64\sshd_config") -replace 'C:/Program Files/OpenSSH', 'C:/Program Files/OpenSSH-Win64' | Set-Content "C:\Program Files\OpenSSH-Win64\sshd_config"

# Configure services to restart after failure
sc.exe failure sshd reset= 86400 actions= run/1000 command= "C:\Program Files\OpenSSH-Win64\sshd-recovery.bat"
sc.exe failure ssh-agent reset= 86400 actions= restart/1000

sc.exe sdset "sshd" "D:(A;;CCLCSWRPWPDTLOCRRC;;;SY)(A;;CCDCLCSWRPWPDTLOCRSDRCWDWO;;;BA)(A;;CCLCSWLOCRRC;;;IU)(A;;CCLCSWLOCRRC;;;SU)(A;;RPWPDTLO;;;S-1-5-80-3847866527-469524349-687026318-516638107-1125189541)S:(AU;FA;CCDCLCSWRPWPDTLOCRSDRCWDWO;;;WD)"
sc.exe sdset "ssh-agent" "D:(A;;CCLCSWRPWPDTLOCRRC;;;SY)(A;;CCDCLCSWRPWPDTLOCRSDRCWDWO;;;BA)(A;;CCLCSWLOCRRC;;;IU)(A;;CCLCSWLOCRRC;;;SU)(A;;RP;;;AU)(A;;RPWPDTLO;;;S-1-5-80-3847866527-469524349-687026318-516638107-1125189541)S:(AU;FA;CCDCLCSWRPWPDTLOCRSDRCWDWO;;;WD)"

schtasks.exe /Create /TN init-ssh /RU SYSTEM /SC ONSTART /TR "powershell.exe -File 'C:\Program Files\Amazon\Ec2ConfigService\Scripts\init-ssh.ps1'" 
#debug schtasks.exe /Query /TN init-ssh /XML
