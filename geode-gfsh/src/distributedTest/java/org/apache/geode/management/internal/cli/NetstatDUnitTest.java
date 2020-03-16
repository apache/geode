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

package org.apache.geode.management.internal.cli;

import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.lang3.JavaVersion;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class NetstatDUnitTest {
  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static MemberVM server0;
  private static MemberVM server1;

  private static final String GROUP_1 = "group-1";

  private static final String GROUP_2 = "group-2";

  private static String netStatLsofCommand;

  @BeforeClass
  public static void beforeClass() throws Exception {
    MemberVM locator0 = lsRule.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty("groups", GROUP_1);
    server0 = lsRule.startServerVM(1, props, locator0.getPort());
    props.setProperty("groups", GROUP_2);
    server1 = lsRule.startServerVM(2, props, locator0.getPort());

    gfsh.connectAndVerify(locator0);

    netStatLsofCommand = "netstat --with-lsof=true --member=" + server1.getName();

  }

  @Test
  public void testOutputToConsoleForAllMembers() {
    gfsh.executeAndAssertThat("netstat").statusIsSuccess()
        .doesNotContainOutput("Could not execute");

    String rawOutput = gfsh.getGfshOutput();
    String[] lines = rawOutput.split("\n");

    assertThat(lines.length).isGreaterThan(5);
    assertThat(lines[4].trim().split("[,\\s]+")).containsExactlyInAnyOrder("locator-0",
        "server-1",
        "server-2");
  }

  @Test
  public void testOutputToConsoleForOneMember() {
    gfsh.executeAndAssertThat("netstat --member=server-1")
        .statusIsSuccess()
        .doesNotContainOutput("Could not execute");

    String rawOutput = gfsh.getGfshOutput();
    String[] lines = rawOutput.split("\n");

    assertThat(lines.length).isGreaterThan(5);
    assertThat(lines[4].trim().split("[,\\s]+")).containsExactlyInAnyOrder("server-1");
  }

  @Ignore("GEODE-6228")
  @Test
  public void testOutputToConsoleWithLsofForOneMember() {
    gfsh.executeAndAssertThat("netstat --member=server-1 --with-lsof")
        .statusIsSuccess().doesNotContainOutput("Could not execute");

    String rawOutput = gfsh.getGfshOutput();
    String[] lines = rawOutput.split("\n");

    assertThat(lines.length).isGreaterThan(5);
    assertThat(lines[4].trim().split("[,\\s]+")).containsExactlyInAnyOrder("server-1");
    assertThat(lines).filteredOn(e -> e.contains("## lsof output ##")).hasSize(1);
  }

  @Test
  public void testOutputToFile() throws Exception {
    File outputFile = new File(temp.newFolder(), "command.log.txt");

    gfsh.executeAndAssertThat("netstat --file=" + outputFile.getAbsolutePath()).statusIsSuccess();

    List<String> lines = new ArrayList<>();
    Scanner scanner = new Scanner(outputFile);
    while (scanner.hasNextLine()) {
      lines.add(scanner.nextLine());
    }

    // verify that the OS commands executed
    assertThat(lines.toString()).doesNotContain("Could not execute");

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactlyInAnyOrder("locator-0",
        "server-1", "server-2");
  }

  @Test
  public void testOutputToFileForOneGroup() throws Exception {
    File outputFile = new File(temp.newFolder(), "command.log.txt");

    gfsh.executeAndAssertThat(
        String.format("netstat --file=%s --group=%s", outputFile.getAbsolutePath(), GROUP_1))
        .statusIsSuccess();

    List<String> lines = new ArrayList<>();
    Scanner scanner = new Scanner(outputFile);
    while (scanner.hasNextLine()) {
      lines.add(scanner.nextLine());
    }

    // verify that the OS commands executed
    assertThat(lines.toString()).doesNotContain("Could not execute");

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactly("server-1");
  }

  // This test runs OK on JDK11 but takes a very long time on JDK8
  @Test
  public void testOutputWithLsofToFile() throws Exception {
    // Skipping test on JDK8. Running lsof command takes an excessive amount of time on Java 8
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_11))
        .as("Skipping test due to excessive run time when running lsof on JDK 8").isTrue();

    File outputFile = new File(temp.newFolder(), "command.log.txt");

    @SuppressWarnings("deprecation")
    CommandResult result =
        gfsh.executeCommand("netstat --with-lsof=true --file=" + outputFile.getAbsolutePath());
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    List<String> lines = new ArrayList<>();
    Scanner scanner = new Scanner(outputFile);
    while (scanner.hasNextLine()) {
      lines.add(scanner.nextLine());
    }

    // verify that the OS commands executed
    assertThat(lines.toString()).doesNotContain("Could not execute");

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactlyInAnyOrder("locator-0",
        "server-1", "server-2");
    assertThat(lines).filteredOn(e -> e.contains("## lsof output ##")).hasSize(1);
  }

  @Ignore("GEODE-6228")
  @Test
  public void testConnectToLocatorWithLargeCommandResponse() throws Exception {
    gfsh.connect(server0.getEmbeddedLocatorPort(), GfshCommandRule.PortType.locator);
    gfsh.executeAndAssertThat(netStatLsofCommand).statusIsSuccess()
        .doesNotContainOutput("Could not execute");
  }

  @Ignore("GEODE-6228")
  @Test
  public void testConnectToJmxManagerOneWithLargeCommandResponse() throws Exception {
    gfsh.connect(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(netStatLsofCommand).statusIsSuccess()
        .doesNotContainOutput("Could not execute");

  }

  @Ignore("GEODE-6228")
  @Test
  public void testConnectToJmxManagerTwoWithLargeCommandResponse() throws Exception {
    gfsh.connect(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(netStatLsofCommand).statusIsSuccess()
        .doesNotContainOutput("Could not execute");
  }
}
