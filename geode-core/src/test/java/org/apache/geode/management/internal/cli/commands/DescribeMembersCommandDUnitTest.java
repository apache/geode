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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.DESCRIBE_MEMBER;
import static org.assertj.core.api.Assertions.assertThat;

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
public class DescribeMembersCommandDUnitTest {
  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();
  private static MemberVM locator;

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @BeforeClass
  public static void setup() throws Exception {
    locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, locator.getPort());
  }

  @Test
  public void describeInvalidMember() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(DESCRIBE_MEMBER + " --name=foo");
    String output = gfsh.getGfshOutput();

    assertThat(output).contains("Member \"foo\" not found");
  }

  @Test
  public void describeMembersWhenNotConnected() throws Exception {
    String result = gfsh.execute(DESCRIBE_MEMBER);
    assertThat(result)
        .contains("Command 'describe member' was found but is not currently available");
  }

  @Test
  public void describeLocator() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(DESCRIBE_MEMBER + " --name=locator-0");
    String output = gfsh.getGfshOutput();

    assertThat(output).contains("locator-0");
    assertThat(output).doesNotContain("server-1");
  }

  @Test
  public void describeServer() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand(DESCRIBE_MEMBER + " --name=server-1");
    String output = gfsh.getGfshOutput();

    assertThat(output).doesNotContain("locator-0");
    assertThat(output).contains("server-1");
  }
}
