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

package org.apache.geode.management.internal.cli;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Server;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({DistributedTest.class, FlakyTest.class})
public class NetstatDUnitTest {
  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @ClassRule
  public static GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  private static int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);

  private static String netStatCommand = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties properties = new Properties();
    // common properties
    properties.setProperty("locators", "localhost[" + ports[0] + "]");
    properties.setProperty("http-service-port", "0");

    // start peer locator
    properties.setProperty("start-locator", "localhost[" + ports[0] + "],peer=true,server=true");
    properties.setProperty("jmx-manager-port", ports[1] + "");
    lsRule.startServerVM(0, properties);

    // start server with jmx Manager as well
    properties.remove("start-locator");
    properties.setProperty("jmx-manager-port", ports[2] + "");
    Server server = lsRule.startServerVM(1, properties);

    // start server with no jmx Manager
    properties.setProperty("jmx-manager", "false");
    properties.setProperty("jmx-manager-port", "0");
    properties.setProperty("jmx-manager-start", "false");
    lsRule.startServerVM(2, properties);

    // start another server
    lsRule.startServerVM(3, properties);

    netStatCommand = "netstat --with-lsof=true --member=" + server.getName();
  }

  @Test
  public void testConnectToLocator() throws Exception {
    gfshConnector.connect(ports[0], GfshShellConnectionRule.PortType.locator);
    gfshConnector.executeAndVerifyCommand(netStatCommand);
  }

  @Test
  public void testConnectToJmxManagerOne() throws Exception {
    gfshConnector.connect(ports[1], GfshShellConnectionRule.PortType.jmxManger);
    gfshConnector.executeAndVerifyCommand(netStatCommand);
  }

  @Test
  public void testConnectToJmxManagerTwo() throws Exception {
    gfshConnector.connect(ports[2], GfshShellConnectionRule.PortType.jmxManger);
    gfshConnector.executeAndVerifyCommand(netStatCommand);
  }

  @After
  public void after() throws Exception {
    gfshConnector.disconnect();
  }
}
