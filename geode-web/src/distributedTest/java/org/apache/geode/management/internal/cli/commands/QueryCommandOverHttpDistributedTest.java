/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;

import java.util.Properties;

import org.apache.geode.test.junit.rules.GfshCommandRule;

public class QueryCommandOverHttpDistributedTest extends QueryCommandDistributedTestBase {

  @Override
  protected Properties locatorProperties(Properties configProperties) {
    int[] ports = getRandomAvailableTCPPorts(2);
    int httpPort = ports[0];
    int jmxPort = ports[1];

    configProperties.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));
    configProperties.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    return configProperties;
  }

  @Override
  protected void connectToLocator() throws Exception {
    gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
  }
}
