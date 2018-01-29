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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class AlterRegionCommandIntegrationTest {
  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withRegion(RegionShortcut.REPLICATE, "REPLICATED");

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void before() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  @Test
  public void validateGroup() throws Exception {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --group=unknown").statusIsError()
        .containsOutput("Group(s) \"[unknown]\" are invalid.");
  }

  @Test
  public void invalidCacheListener() throws Exception {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --cache-listener=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName[] for option 'cache-listener'");
  }

  @Test
  public void invalidCacheLoader() throws Exception {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --cache-loader=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName for option 'cache-loader'");
  }

  @Test
  public void invalidCacheWriter() throws Exception {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --cache-writer=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName for option 'cache-writer'");
  }

  @Test
  public void invalidEvictionMax() throws Exception {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --eviction-max=-1").statusIsError()
        .containsOutput("Specify 0 or a positive integer value for eviction-max");
  }
}
