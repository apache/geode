$user = "build"
$pass = "p1votal!"

net.exe user $user $pass /add
net.exe localgroup Administrators $user /add
wmic.exe UserAccount where "Name='$user'" set PasswordExpires=False

$spw = ConvertTo-SecureString $pass -AsPlainText -Force
$cred = New-Object System.Management.Automation.PSCredential -ArgumentList $user,$spw
Start-Process cmd /c -WindowStyle Hidden -Credential $cred -ErrorAction SilentlyContinue


schtasks.exe /Create /TN init-user-build /RU SYSTEM /SC ONSTART /TR "powershell.exe -File 'C:\Users\build\init-user-build.ps1'" 

