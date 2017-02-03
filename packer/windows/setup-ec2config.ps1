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
# Enable the system password to be retrieved from the AWS Console after this AMI is built and used to launch code
$ec2config = [xml] (get-content 'C:\Program Files\Amazon\Ec2ConfigService\Settings\config.xml')
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2SetPassword"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2DynamicBootVolumeSize"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2HandleUserData"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2SetComputerName"}).state = "Enabled"
($ec2config.ec2configurationsettings.plugins.plugin | where {$_.name -eq "Ec2EventLog"}).state = "Enabled"
$ec2config.save("C:\Program Files\Amazon\Ec2ConfigService\Settings\config.xml")
