$ErrorActionPreference = "Stop"

write-host "Installing Chocolatey"

# Avoid bug in 7zip when running via WinRM
$Env:chocolateyUseWindowsCompression = $true

iwr https://chocolatey.org/install.ps1 | iex

write-host "Chocolatey Installed"
