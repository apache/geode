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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule.PortType;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DescribeConfigCommandJUnitTest {
  private static final String[] EXPECTED_BASE_CONFIGURATION_DATA = {"Configuration of member :",
      "JVM command line arguments", "GemFire properties defined using the API"};

  private static final String[] EXPECTED_EXPANDED_CONFIGURATION_DATA =
      {"Cache attributes", "GemFire properties using default values"};

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withJMXManager().withName("server")
      .withRegion(RegionShortcut.PARTITION, "region").withAutoStart();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Test
  public void describeConfig() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), PortType.jmxManger);
    String result = gfsh.execute("describe config --member=" + server.getName());
    for (String datum : EXPECTED_BASE_CONFIGURATION_DATA) {
      assertThat(result).contains(datum);
    }
  }

  @Test
  public void describeConfigAndShowDefaults() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), PortType.jmxManger);
    String result =
        gfsh.execute("describe config --hide-defaults=false --member=" + server.getName());
    for (String datum : EXPECTED_BASE_CONFIGURATION_DATA) {
      assertThat(result).contains(datum);
    }
    for (String datum : EXPECTED_EXPANDED_CONFIGURATION_DATA) {
      assertThat(result).contains(datum);
    }
  }

  @Test
  public void describeConfigWhenNotConnected() throws Exception {
    String result = gfsh.execute("describe config --member=" + server.getName());
    assertThat(result).contains("was found but is not currently available");
  }

  @Test
  public void describeConfigOnInvalidMember() throws Exception {
    String invalidMemberName = "invalid-member-name";
    String expectedErrorString = String.format("Member \"%s\" not found", invalidMemberName);

    gfsh.connectAndVerify(server.getJmxPort(), PortType.jmxManger);
    String result = gfsh.execute("describe config --member=" + invalidMemberName);
    assertThat(result).contains(expectedErrorString);
  }

  @Test
  public void describeConfigWithoutMemberName() throws Exception {
    String expectedErrorString = String.format("You should specify option ");

    gfsh.connectAndVerify(server.getJmxPort(), PortType.jmxManger);
    String result = gfsh.execute("describe config");
    assertThat(result).contains(expectedErrorString);
  }
}
