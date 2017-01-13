# Enable the system password to be retrieved from the AWS Console after this AMI is built and used to launch code
$ec2config = [xml] (get-content 'C:\Program Files\Amazon\Ec2ConfigService\Settings\config.xml')
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2SetPassword"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2DynamicBootVolumeSize"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2HandleUserData"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2SetComputerName"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2EventLog"}).state = "Enabled"
$ec2config.save("C:\Program Files\Amazon\Ec2ConfigService\Settings\config.xml")
