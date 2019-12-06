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
import org.apache.geode.test.junit.categories.ConfigurationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({ConfigurationTest.class})
public class DescribeConfigCommandIntegrationTest {
  private static final String[] EXPECTED_BASE_CONFIGURATION_DATA = {"Configuration of member :",
      "JVM command line arguments", "GemFire properties defined using the API"};

  private static final String[] EXPECTED_EXPANDED_CONFIGURATION_DATA =
      {"Cache attributes", "GemFire properties using default values"};

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withName("server").withRegion(RegionShortcut.PARTITION, "region").withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule(server::getJmxPort, PortType.jmxManager);

  @Test
  public void describeConfig() throws Exception {
    gfsh.executeAndAssertThat("describe config --member=" + server.getName()).statusIsSuccess()
        .containsOutput(EXPECTED_BASE_CONFIGURATION_DATA);
  }

  @Test
  public void describeConfigAndShowDefaults() throws Exception {
    gfsh.executeAndAssertThat("describe config --hide-defaults=false --member=" + server.getName())
        .statusIsSuccess().containsOutput(EXPECTED_BASE_CONFIGURATION_DATA)
        .containsOutput(EXPECTED_EXPANDED_CONFIGURATION_DATA);
  }

  @Test
  public void describeConfigWhenNotConnected() throws Exception {
    gfsh.disconnect();
    gfsh.executeAndAssertThat("describe config --member=" + server.getName()).statusIsError()
        .containsOutput("was found but is not currently available");
  }

  @Test
  public void describeConfigOnInvalidMember() throws Exception {
    String invalidMemberName = "invalid-member-name";
    String expectedErrorString = String.format("Member %s could not be found", invalidMemberName);
    gfsh.executeAndAssertThat("describe config --member=" + invalidMemberName).statusIsError()
        .containsOutput(expectedErrorString);
  }
}
