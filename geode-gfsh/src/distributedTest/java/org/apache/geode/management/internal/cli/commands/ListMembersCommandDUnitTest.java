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

import static org.apache.geode.management.internal.i18n.CliStrings.GROUPS;
import static org.apache.geode.management.internal.i18n.CliStrings.LIST_MEMBER;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class ListMembersCommandDUnitTest {
  private static MemberVM locator1, locator2;
  private final Pattern pattern = Pattern.compile("(.*)locator-0(.*)\\[Coordinator]");

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule(locator1::getJmxPort, jmxManager);

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @BeforeClass
  public static void setup() {
    Properties properties = new Properties();
    properties.setProperty(GROUPS, "locatorGroup");
    // First member, becomes the coordinator by default.
    locator1 = lsRule.startLocatorVM(0, properties);
    locator2 = lsRule.startLocatorVM(1, properties, locator1.getPort());

    lsRule.startServerVM(2, "serverGroup1", locator1.getPort(), locator2.getPort());
    lsRule.startServerVM(3, "serverGroup1", locator1.getPort(), locator2.getPort());
    lsRule.startServerVM(4, "serverGroup2", locator1.getPort(), locator2.getPort());
  }

  @Test
  public void listMembersWithoutConnection() throws Exception {
    gfsh.disconnect();
    gfsh.executeAndAssertThat(LIST_MEMBER).statusIsError()
        .containsOutput("Command 'list members' was found but is not currently available");
  }

  @Test
  public void listAllMembers() {
    gfsh.executeAndAssertThat(LIST_MEMBER).statusIsSuccess()
        .containsOutput("Member Count : 5")
        .hasTableSection(ListMembersCommand.MEMBERS_SECTION).hasColumn("Name")
        .containsExactlyInAnyOrder("locator-0", "locator-1", "server-2", "server-3", "server-4");

    assertCoordinatorTag(gfsh.getCommandResult());
  }

  @Test
  public void listMembersInLocatorGroup() {
    gfsh.executeAndAssertThat(LIST_MEMBER + " --group=locatorGroup").statusIsSuccess();
    String output = gfsh.getGfshOutput();

    assertThat(output).contains("Member Count : 2");
    assertThat(output).contains("locator-0");
    assertThat(output).doesNotContain("server-1");
    assertThat(output).doesNotContain("server-2");
    assertThat(output).doesNotContain("server-3");
    assertCoordinatorTag(gfsh.getCommandResult());
  }

  @Test
  public void listMembersInServerGroupOne() {
    gfsh.executeAndAssertThat(LIST_MEMBER + " --group=serverGroup1").statusIsSuccess();
    String output = gfsh.getGfshOutput();
    assertThat(output).contains("Member Count : 2");
    assertThat(output).contains("server-2");
    assertThat(output).contains("server-3");
    assertThat(output).doesNotContain("server-4");
  }

  @Test
  public void listMembersInServerGroupTwo() {
    gfsh.executeAndAssertThat(LIST_MEMBER + " --group=serverGroup2").statusIsSuccess();
    String output = gfsh.getGfshOutput();
    assertThat(output).contains("Member Count : 1");
    assertThat(output).doesNotContain("server-2");
    assertThat(output).doesNotContain("server-3");
    assertThat(output).contains("server-4");
  }

  @Test
  public void listMembersInNonExistentGroup() {
    gfsh.executeAndAssertThat(LIST_MEMBER + " --group=foo")
        .statusIsSuccess()
        .containsOutput("No Members Found")
        .doesNotContainOutput(("Member Count :"))
        .doesNotContainOutput("locator-0", "server-1", "server-2", "server-3");
  }

  @Test
  @Parameters(value = {"true", "false"})
  @TestCaseName("{method} - Connected to Coordinator: {params}")
  public void listMembersShouldAlwaysTagTheCoordinatorMember(boolean useCoordinator)
      throws Exception {
    int jmxPort = useCoordinator ? locator1.getJmxPort() : locator2.getJmxPort();
    gfsh.disconnect();
    gfsh.connectAndVerify(jmxPort, jmxManager);

    gfsh.executeAndAssertThat(LIST_MEMBER).statusIsSuccess()
        .hasTableSection(ListMembersCommand.MEMBERS_SECTION).hasColumn("Id")
        .hasSize(5);

    assertCoordinatorTag(gfsh.getCommandResult());
  }

  /**
   * Assert that the locator-0 has been marked with the "Coordinator" tag.
   *
   * @param commandResult GFSH command result to test.
   */
  private void assertCoordinatorTag(CommandResult commandResult) {
    List<String> ids = commandResult.getResultData()
        .getTableSection("members")
        .getValuesInColumn("Id");
    assertThat(ids.stream().filter(string -> pattern.matcher(string).matches())).hasSize(1);
  }
}
