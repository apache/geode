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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({RegionsTest.class})
public class DescribeRegionIntegrationTest {
  private static final String MEMBER_NAME = "test-server";
  private static final String REGION_NAME = "test-region";
  private static final String GROUP_NAME = "test-group";

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule()
      .withRegion(RegionShortcut.REPLICATE, REGION_NAME).withName(MEMBER_NAME)
      .withProperty("groups", GROUP_NAME).withJMXManager().withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, PortType.jmxManager).withTimeout(2);

  @Test
  public void commandFailsWhenNotConnected() throws Exception {
    gfsh.disconnect();
    gfsh.executeAndAssertThat("describe region").statusIsError()
        .containsOutput("was found but is not currently available");
  }

  @Test
  public void commandFailsWithBadNameOption() throws Exception {
    String cmd = "describe region --name=invalid-region-name";
    gfsh.executeAndAssertThat(cmd).statusIsError().containsOutput("invalid-region-name not found");
  }

  @Test
  public void commandSucceedsWithGoodNameOption() throws Exception {
    String cmd = "describe region --name=" + REGION_NAME;
    gfsh.executeAndAssertThat(cmd).statusIsSuccess().containsOutput("Name", "Data Policy",
        "Hosting Members");
  }
}
