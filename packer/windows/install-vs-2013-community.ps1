# TODO AdminDeploy.xml
# vs_community.exe /AdminFile C:\Users\Administrator\AdminDeployment.xml /Log setup.log /Passive
Set-PSDebug -Trace 0

Import-Module Packer

$log = "vs_community.log"

Install-Package https://download.microsoft.com/download/A/A/D/AAD1AA11-FF9A-4B3C-8601-054E89260B78/vs_community.exe `
    -Hash EA57DF0294AE3CE4B893D19D497AEC02D1627F40B9576BC7013588DF79A0C528 `
    -ArgumentList @("/quiet", "/log", $log) `
    -Log $log -Verbose
