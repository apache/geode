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
