# TODO AdminDeploy.xml
# vs_community.exe /AdminFile C:\Users\Administrator\AdminDeployment.xml /Log setup.log /Passive
Set-PSDebug -Trace 0

Import-Module Packer

$log = "vs_community.log"

Install-Package https://download.microsoft.com/download/0/B/C/0BC321A4-013F-479C-84E6-4A2F90B11269/vs_community.exe `
    -Hash ED8D88D0AB88832754302BFC2A374E803B3A21C1590B428092944272F9EA30FE `
    -ArgumentList @("/quiet", "/log", $log) `
    -Log $log -Verbose

