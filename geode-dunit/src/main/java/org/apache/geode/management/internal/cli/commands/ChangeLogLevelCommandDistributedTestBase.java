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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ChangeLogLevelCommandDistributedTestBase {

  private static final String MANAGER_NAME = "Manager";
  private static final String SERVER1_NAME = "Server1";
  private static final String SERVER2_NAME = "Server2";
  private static final String GROUP0 = "Group0";
  private static final String GROUP1 = "Group1";
  private static final String GROUP2 = "Group2";

  protected static MemberVM locator;

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setup() {
    Properties managerProps = new Properties();
    managerProps.setProperty(NAME, MANAGER_NAME);
    managerProps.setProperty(GROUPS, GROUP0);
    locator =
        clusterStartupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(managerProps));

    Properties server1Props = new Properties();
    server1Props.setProperty(NAME, SERVER1_NAME);
    server1Props.setProperty(GROUPS, GROUP1);
    clusterStartupRule.startServerVM(1, server1Props, locator.getPort());

    Properties server2Props = new Properties();
    server2Props.setProperty(NAME, SERVER2_NAME);
    server2Props.setProperty(GROUPS, GROUP2);
    clusterStartupRule.startServerVM(2, server2Props, locator.getPort());
  }

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locator.getJmxPort(), jmxManager);
  }

  @After
  public void after() throws Exception {
    gfsh.disconnect();
  }

  @Test
  public void testChangeLogLevelForGroups() {
    String commandString = "change loglevel --loglevel=finer --groups=" + GROUP1 + "," + GROUP2;

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .containsOutput(SERVER1_NAME, SERVER2_NAME).doesNotContainOutput(MANAGER_NAME);
  }

  @Test
  public void testChangeLogLevelForGroup() {
    String commandString = "change loglevel --loglevel=finer --groups=" + GROUP1;

    gfsh.executeAndAssertThat(commandString).statusIsSuccess().containsOutput(SERVER1_NAME)
        .doesNotContainOutput(SERVER2_NAME, MANAGER_NAME);
  }

  @Test
  public void testChangeLogLevelForMembers() {
    String commandString =
        "change loglevel --loglevel=finer --members=" + SERVER1_NAME + "," + SERVER2_NAME;

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .containsOutput(SERVER1_NAME, SERVER2_NAME).doesNotContainOutput(MANAGER_NAME);
  }

  @Test
  public void testChangeLogLevelForMember() {
    String commandString = "change loglevel --loglevel=finer --members=" + SERVER1_NAME;

    gfsh.executeAndAssertThat(commandString).statusIsSuccess().containsOutput(SERVER1_NAME)
        .doesNotContainOutput(SERVER2_NAME, MANAGER_NAME);
  }

  @Test
  public void testChangeLogLevelForInvalidMember() {
    String commandString = "change loglevel --loglevel=finer --members=NotAValidMember";

    gfsh.executeAndAssertThat(commandString).statusIsError()
        .containsOutput("No members were found matching the given member IDs or groups.");
  }
}
