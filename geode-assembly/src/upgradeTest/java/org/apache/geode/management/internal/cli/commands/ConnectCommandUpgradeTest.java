/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.commands;

import static java.util.stream.Collectors.toList;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.version.TestVersions.greaterThan;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Collection;

import org.apache.commons.lang3.JavaVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshExecutor;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

@Category(GfshTest.class)
@RunWith(Parameterized.class)
public class ConnectCommandUpgradeTest {

  @Parameters(name = "Locator: {0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(greaterThan(TestVersion.valueOf("1.7.0"))))
        .collect(toList());
  }

  private final VmConfiguration sourceVmConfiguration;

  private GfshExecutor oldGfsh;
  private GfshExecutor currentGfsh;

  public ConnectCommandUpgradeTest(VmConfiguration vmConfiguration) {
    sourceVmConfiguration = vmConfiguration;
  }

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUp() {
    currentGfsh = gfshRule.executor()
        .build(folderRule.getFolder().toPath());
    oldGfsh = gfshRule.executor()
        .withVmConfiguration(sourceVmConfiguration)
        .build(folderRule.getFolder().toPath());
  }

  @Test
  public void useCurrentGfshToConnectToOlderLocator() {
    assumeThat(JavaVersion.JAVA_RECENT).isLessThanOrEqualTo(JavaVersion.JAVA_1_8);

    int[] ports = getRandomAvailableTCPPorts(2);
    int locatorPort = ports[0];
    int jmxPort = ports[1];

    GfshScript
        .of(startLocatorCommand("test", locatorPort, jmxPort))
        .execute(oldGfsh);

    // New version gfsh could not connect to locators with version below 1.10.0
    if (sourceVmConfiguration.geodeVersion().lessThan(TestVersion.valueOf("1.10.0"))) {
      GfshExecution connect = GfshScript
          .of("connect --locator=localhost[" + locatorPort + "]")
          .expectFailure()
          .execute(currentGfsh);

      assertThat(connect.getOutputText())
          .contains("Cannot use a")
          .contains("gfsh client to connect to")
          .contains("cluster.");
    }

    // From 1.10.0 new version gfsh are able to connect to old version locators
    else {
      GfshExecution connect = GfshScript
          .of("connect --locator=localhost[" + locatorPort + "]")
          .expectExitCode(0)
          .execute(currentGfsh);

      assertThat(connect.getOutputText())
          .contains("Successfully connected to:");
    }
  }

  @Test
  public void invalidHostname() {
    int[] ports = getRandomAvailableTCPPorts(2);
    int locatorPort = ports[0];
    int jmxPort = ports[1];
    GfshScript
        .of(startLocatorCommand("test", locatorPort, jmxPort))
        .execute(oldGfsh);

    GfshExecution connect = GfshScript
        .of("connect --locator=\"invalid host name[52326]\"")
        .expectFailure()
        .execute(currentGfsh);

    assertThat(connect.getOutputText())
        .doesNotContain("UnknownHostException")
        .doesNotContain("nodename nor servname")
        .contains("can't be reached. Hostname or IP address could not be found.");
  }

  private static String startLocatorCommand(String name, int port, int jmxPort) {
    String startLocatorCommand =
        "start locator --name=%s --port=%d --http-service-port=%d --J=-Dgemfire.jmx-manager-port=%d";
    return String.format(startLocatorCommand, name, port, 0, jmxPort);
  }
}
