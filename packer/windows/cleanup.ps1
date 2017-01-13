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
