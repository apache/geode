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
import static org.apache.geode.test.junit.rules.GfshShellConnectionRule.PortType.jmxManager;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;

@Category(DistributedTest.class)
public class ShowLogCommandDUnitTest implements Serializable {
  private static final boolean CONNECT_OVER_HTTP = Boolean.getBoolean(USE_HTTP_SYSTEM_PROPERTY);

  private static final String MANAGER_NAME = "Manager";
  private static final String SERVER1_NAME = "Server1";
  private static final String SERVER2_NAME = "Server2";
  private static final String GROUP0 = "Group0";
  private static final String GROUP1 = "Group1";
  private static final String GROUP2 = "Group2";
  private static final String MESSAGE_ON_MANAGER = "someLogMessageOnManager";
  private static final String MESSAGE_ON_SERVER1 = "someLogMessageOnServer1";
  private static final String MESSAGE_ON_SERVER2 = "someLogMessageOnServer2";
  private MemberVM manager;
  private MemberVM server1;
  private MemberVM server2;
  @Rule
  public LocatorServerStartupRule locatorServerStartupRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();


  @Before
  public void setup() throws Exception {
    Properties managerProps = new Properties();
    managerProps.setProperty(NAME, MANAGER_NAME);
    managerProps.setProperty(GROUPS, GROUP0);
    managerProps.setProperty(LOG_FILE, MANAGER_NAME + ".log");

    manager = locatorServerStartupRule.startLocatorVM(0, managerProps);

    Properties server1Props = new Properties();
    server1Props.setProperty(NAME, SERVER1_NAME);
    server1Props.setProperty(GROUPS, GROUP1);
    server1Props.setProperty(LOG_FILE, SERVER1_NAME + ".log");

    server1 = locatorServerStartupRule.startServerVM(1, server1Props, manager.getPort());

    Properties server2Props = new Properties();
    server2Props.setProperty(NAME, SERVER2_NAME);
    server2Props.setProperty(GROUPS, GROUP2);
    server2Props.setProperty(LOG_FILE, SERVER2_NAME + ".log");

    server2 = locatorServerStartupRule.startServerVM(2, server2Props, manager.getPort());

    if (CONNECT_OVER_HTTP) {
      gfsh.connectAndVerify(manager.getHttpPort(), http);
    } else {
      gfsh.connectAndVerify(manager.getJmxPort(), jmxManager);
    }

    Awaitility.await().atMost(2, TimeUnit.MINUTES).until(this::allMembersAreConnected);
  }

  private void writeLogMessages() {
    manager.invoke(() -> LogService.getLogger().info(MESSAGE_ON_MANAGER));
    server1.invoke(() -> LogService.getLogger().info(MESSAGE_ON_SERVER1));
    server2.invoke(() -> LogService.getLogger().info(MESSAGE_ON_SERVER2));

  }

  @Test
  public void testShowLogDefault() {
    writeLogMessages();
    String command = "show log --member=" + MANAGER_NAME;

    gfsh.executeAndVerifyCommand(command);
    assertThat(gfsh.getGfshOutput()).contains(MESSAGE_ON_MANAGER);
    assertThat(gfsh.getGfshOutput()).doesNotContain(MESSAGE_ON_SERVER1);
    assertThat(gfsh.getGfshOutput()).doesNotContain(MESSAGE_ON_SERVER2);
  }

  @Test
  public void testShowLogNumLines() throws InterruptedException {
    writeLogMessages();

    String command = "show log --member=" + SERVER1_NAME + " --lines=50";

    gfsh.executeAndVerifyCommand(command);

    String output = gfsh.getGfshOutput();
    assertThat(output).contains(MESSAGE_ON_SERVER1);
    assertThat(gfsh.getGfshOutput()).doesNotContain(MESSAGE_ON_MANAGER);
    assertThat(gfsh.getGfshOutput()).doesNotContain(MESSAGE_ON_SERVER2);

    assertThat(output.split(System.getProperty("line.separator"))).hasSize(51);
  }

  @Test
  public void testShowLogInvalidMember() throws Exception {
    writeLogMessages();

    String command = "show log --member=NotAValidMember";

    CommandResult result = gfsh.executeCommand(command);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);

    String output = gfsh.getGfshOutput();
    assertThat(output).contains("Member not found");

  }

  private boolean allMembersAreConnected() {
    return manager.getVM().invoke(() -> {
      InternalCache cache = (InternalCache) CacheFactory.getAnyInstance();
      DistributedMember server1 = CliUtil.getDistributedMemberByNameOrId(SERVER1_NAME);
      DistributedMember server2 = CliUtil.getDistributedMemberByNameOrId(SERVER2_NAME);

      ShowLogCommand showLogCommand = new ShowLogCommand();

      boolean server1isConnected = showLogCommand.getMemberMxBean(cache, server1) != null;
      boolean server2isConnected = showLogCommand.getMemberMxBean(cache, server2) != null;
      return server1isConnected && server2isConnected;
    });
  }
}
