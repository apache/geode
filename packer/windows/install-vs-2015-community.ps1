# TODO AdminDeploy.xml
# vs_community.exe /AdminFile C:\Users\Administrator\AdminDeployment.xml /Log setup.log /Passive
Set-PSDebug -Trace 0

Import-Module Packer

$log = "vs_community.log"

choco install visualstudio2015community -confirm
