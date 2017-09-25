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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUPS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.LIST_MEMBER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class ListMembersCommandDUnitTest {
  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();
  private static MemberVM locator;

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @BeforeClass
  public static void setup() throws Exception {
    locator = lsRule.startLocatorVM(0, propertiesForGroup("locatorGroup"));
    lsRule.startServerVM(1, propertiesForGroup("serverGroup1"), locator.getPort());
    lsRule.startServerVM(2, propertiesForGroup("serverGroup1"), locator.getPort());
    lsRule.startServerVM(3, propertiesForGroup("serverGroup2"), locator.getPort());
  }

  @Test
  public void listMembersWithoutConnection() throws Exception {
    String result = gfsh.execute(LIST_MEMBER);
    assertThat(result).contains("Command 'list members' was found but is not currently available");
  }

  @Test
  public void listAllMembers() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(LIST_MEMBER);
    String output = gfsh.getGfshOutput();

    assertThat(output).contains("locator-0");
    assertThat(output).contains("server-1");
    assertThat(output).contains("server-2");
    assertThat(output).contains("server-3");
  }

  @Test
  public void listMembersInLocatorGroup() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(LIST_MEMBER + " --group=locatorGroup");
    String output = gfsh.getGfshOutput();

    assertThat(output).contains("locator-0");
    assertThat(output).doesNotContain("server-1");
    assertThat(output).doesNotContain("server-2");
    assertThat(output).doesNotContain("server-3");
  }

  @Test
  public void listMembersInServerGroupOne() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(LIST_MEMBER + " --group=serverGroup1");
    String output = gfsh.getGfshOutput();

    assertThat(output).doesNotContain("locator-0");
    assertThat(output).contains("server-1");
    assertThat(output).contains("server-2");
    assertThat(output).doesNotContain("server-3");
  }

  @Test
  public void listMembersInServerGroupTwo() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(LIST_MEMBER + " --group=serverGroup2");
    String output = gfsh.getGfshOutput();

    assertThat(output).doesNotContain("locator-0");
    assertThat(output).doesNotContain("server-1");
    assertThat(output).doesNotContain("server-2");
    assertThat(output).contains("server-3");
  }

  @Test
  public void listMembersInNonExistentGroup() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(LIST_MEMBER + " --group=foo");
    String output = gfsh.getGfshOutput();

    assertThat(output).doesNotContain("locator-0");
    assertThat(output).doesNotContain("server-1");
    assertThat(output).doesNotContain("server-2");
    assertThat(output).doesNotContain("server-3");
    assertThat(output).contains("No Members Found");
  }

  private static Properties propertiesForGroup(String group) {
    Properties properties = new Properties();
    properties.setProperty(GROUPS, group);
    return properties;
  }
}
