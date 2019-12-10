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

import static org.apache.geode.management.internal.i18n.CliStrings.DESCRIBE_MEMBER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({JMXTest.class})
public class DescribeMembersCommandDUnitTest {
  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();
  private static MemberVM locator;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setup() throws Exception {
    locator = lsRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    lsRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort).withServerCount(2));
  }

  @Test
  public void describeInvalidMember() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(DESCRIBE_MEMBER + " --name=foo").statusIsError()
        .containsOutput("Member foo could not be found");
  }

  @Test
  public void describeMembersWhenNotConnected() throws Exception {
    gfsh.executeAndAssertThat(DESCRIBE_MEMBER).statusIsError()
        .containsOutput("Command 'describe member' was found but is not currently available");
  }

  @Test
  public void describeLocator() throws Exception {
    gfsh.connectAndVerify(locator);
    CommandResult result = gfsh.executeAndAssertThat(DESCRIBE_MEMBER + " --name=locator-0")
        .statusIsSuccess()
        .getCommandResult();

    Map<String, String> memberInfo =
        result.getResultData().getDataSection("memberInfo").getContent();
    assertThat(memberInfo.get("Name")).isEqualTo("locator-0");
    assertThat(memberInfo.get("Id")).contains("locator-0");
    assertThat(memberInfo.get("Host")).as("Host").isNotBlank();
    assertThat(memberInfo.get("PID")).as("PID").isNotBlank();
    assertThat(memberInfo.get("Used Heap")).as("Used Heap").isNotBlank();
    assertThat(memberInfo.get("Max Heap")).as("Max Heap").isNotBlank();
    assertThat(memberInfo.get("Working Dir")).as("Working Dir").isNotBlank();
    assertThat(memberInfo.get("Log file")).as("Log File").isNotBlank();
    assertThat(memberInfo.get("Locators")).as("Locators").isNotBlank();
  }

  @Test
  public void describeServer() throws Exception {
    gfsh.connectAndVerify(locator);
    CommandResultAssert commandAssert =
        gfsh.executeAndAssertThat(DESCRIBE_MEMBER + " --name=server-1").statusIsSuccess();

    commandAssert.hasDataSection("memberInfo").hasContent()
        .containsEntry("Name", "server-1")
        .containsKeys("Id", "Host", "PID", "Used Heap", "Max Heap", "Working Dir", "Log file",
            "Locators");

    commandAssert.hasDataSection("connectionInfo").hasContent()
        .containsEntry("Client Connections", "0");

    commandAssert.hasDataSection("serverInfo0").hasContent()
        .containsKeys("Server Bind", "Server Port", "Running");

    commandAssert.hasDataSection("serverInfo1").hasContent()
        .containsKeys("Server Bind", "Server Port", "Running");
  }
}
