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
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.management.internal.cli.commands.CliCommandTestBase.USE_HTTP_SYSTEM_PROPERTY;
import static org.apache.geode.test.junit.rules.GfshShellConnectionRule.PortType.http;
import static org.apache.geode.test.junit.rules.GfshShellConnectionRule.PortType.jmxManger;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;


@Category(DistributedTest.class)
public class GcCommandDUnitTest {
  private static final boolean CONNECT_OVER_HTTP = Boolean.getBoolean(USE_HTTP_SYSTEM_PROPERTY);
  private static final String MANAGER_NAME = "Manager";
  private static final String SERVER1_NAME = "Server1";
  private static final String SERVER2_NAME = "Server2";
  private static final String GROUP0 = "Group0";
  private static final String GROUP1 = "Group1";
  private static final String GROUP2 = "Group2";

  @Rule
  public LocatorServerStartupRule locatorServerStartupRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Before
  public void setup() throws Exception {
    Properties managerProps = new Properties();
    managerProps.setProperty(NAME, MANAGER_NAME);
    managerProps.setProperty(GROUPS, GROUP0);
    managerProps.setProperty(LOG_FILE, "someLog.log");

    MemberVM manager = locatorServerStartupRule.startLocatorVM(0, managerProps);

    Properties server1Props = new Properties();
    server1Props.setProperty(NAME, SERVER1_NAME);
    server1Props.setProperty(GROUPS, GROUP1);
    locatorServerStartupRule.startServerVM(1, server1Props, manager.getPort());

    Properties server2Props = new Properties();
    server2Props.setProperty(NAME, SERVER2_NAME);
    server2Props.setProperty(GROUPS, GROUP2);
    locatorServerStartupRule.startServerVM(2, server2Props, manager.getPort());

    if (CONNECT_OVER_HTTP) {
      gfsh.connectAndVerify(manager.getHttpPort(), http);
    } else {
      gfsh.connectAndVerify(manager.getJmxPort(), jmxManger);
    }
  }

  @Test
  public void testGCForGroup() {
    String gcCommand = "gc --group=" + GROUP0;
    gfsh.executeAndVerifyCommand(gcCommand);

    assertThat(gfsh.getGfshOutput()).contains(MANAGER_NAME);
  }

  @Test
  public void testGCForMemberID() {
    String gcCommand = "gc --member=" + MANAGER_NAME;

    gfsh.executeAndVerifyCommand(gcCommand);
    assertThat(gfsh.getGfshOutput()).contains(MANAGER_NAME);
  }

  @Test
  public void testGCForEntireCluster() {
    String command = "gc";
    gfsh.executeAndVerifyCommand(command);

    String output = gfsh.getGfshOutput();
    assertThat(output).contains(SERVER1_NAME);
    assertThat(output).contains(SERVER2_NAME);
    assertThat(output).doesNotContain(MANAGER_NAME);
  }

  @Test
  public void testGCForInvalidMember() throws Exception {
    String gcCommand = "gc --member=NotAValidMember";

    CommandResult result = gfsh.executeCommand(gcCommand);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not process command due to error. NotAValidMemberMember not found");
  }
}
