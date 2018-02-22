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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class NetstatDUnitTest {
  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfshConnector = new GfshCommandRule();

  private static String netStatCommand = null;
  private static String netStatLsofCommand = null;

  private static MemberVM server0, server1;

  @BeforeClass
  public static void beforeClass() throws Exception {
    server0 = lsRule.startServerVM(0, x -> x.withJMXManager().withEmbeddedLocator());
    int locatorPort = server0.getEmbeddedLocatorPort();

    // start server with jmx Manager as well
    server1 = lsRule.startServerVM(1, x -> x.withJMXManager().withConnectionToLocator(locatorPort));

    // start server with no jmx Manager
    lsRule.startServerVM(2, locatorPort);

    // start another server
    lsRule.startServerVM(3, locatorPort);

    netStatCommand = "netstat --with-lsof=false --member=" + server1.getName();
    netStatLsofCommand = "netstat --with-lsof=true --member=" + server1.getName();
  }

  @Test
  public void testConnectToLocator() throws Exception {
    gfshConnector.connect(server0.getEmbeddedLocatorPort(), GfshCommandRule.PortType.locator);
    gfshConnector.executeAndAssertThat(netStatCommand).statusIsSuccess();
  }

  @Test
  public void testConnectToJmxManagerOne() throws Exception {
    gfshConnector.connect(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfshConnector.executeAndAssertThat(netStatCommand).statusIsSuccess();
  }

  @Test
  public void testConnectToJmxManagerTwo() throws Exception {
    gfshConnector.connect(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfshConnector.executeAndAssertThat(netStatCommand).statusIsSuccess();
  }

  @Ignore("GEODE-2488")
  @Test
  public void testConnectToLocatorWithLargeCommandResponse() throws Exception {
    gfshConnector.connect(server0.getEmbeddedLocatorPort(), GfshCommandRule.PortType.locator);
    gfshConnector.executeAndAssertThat(netStatLsofCommand).statusIsSuccess();
  }

  @Ignore("GEODE-2488")
  @Test
  public void testConnectToJmxManagerOneWithLargeCommandResponse() throws Exception {
    gfshConnector.connect(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfshConnector.executeAndAssertThat(netStatLsofCommand).statusIsSuccess();
  }

  @Ignore("GEODE-2488")
  @Test
  public void testConnectToJmxManagerTwoWithLargeCommandResponse() throws Exception {
    gfshConnector.connect(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfshConnector.executeAndAssertThat(netStatLsofCommand).statusIsSuccess();
  }
}
