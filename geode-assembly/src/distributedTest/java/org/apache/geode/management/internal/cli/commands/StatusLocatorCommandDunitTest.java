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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.management.internal.i18n.CliStrings.STATUS_LOCATOR;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.LocatorLauncherStartupRule;

public class StatusLocatorCommandDunitTest {
  private static final Integer TIMEOUT = 300;
  private static final Integer INTERVAL = 10;
  private static final String locatorName = "locator";

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static LocatorLauncherStartupRule locator = new LocatorLauncherStartupRule()
      .withBuilder(b -> b.setMemberName(locatorName))
      .withAutoStart();

  private static int locatorPort;

  private static LocatorState state;

  @BeforeClass
  public static void beforeClass() {
    locatorPort = locator.getLauncher().getPort();
    state = locator.getLauncher().waitOnStatusResponse(TIMEOUT, INTERVAL, SECONDS);
  }

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locatorPort, PortType.locator);
  }

  @After
  public void after() throws Exception {
    gfsh.disconnect();
  }

  @Test
  public void testWithMemberAddress() throws Exception {
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --host=localhost --port=" + locatorPort)
        .statusIsSuccess();
    assertStatusCommandOutput(gfsh.getGfshOutput(), state);
  }

  @Test
  public void testWithMemberName() throws Exception {
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --name=" + locatorName).statusIsSuccess();
    assertStatusCommandOutput(gfsh.getGfshOutput(), state);
  }

  @Test
  public void testWithMemberID() throws Exception {
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --name=" + locator.getLauncher().getMemberId())
        .statusIsSuccess();
    assertStatusCommandOutput(gfsh.getGfshOutput(), state);
  }

  @Test
  public void testWithDirOnline() throws Exception {
    gfsh.executeAndAssertThat(
        STATUS_LOCATOR + " --dir=" + locator.getLauncher().getWorkingDirectory()).statusIsSuccess();
    assertStatusCommandOutput(gfsh.getGfshOutput(), state);
  }

  @Test
  public void testWithDirOffline() throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    gfsh.executeAndAssertThat(
        STATUS_LOCATOR + " --dir=" + locator.getLauncher().getWorkingDirectory()).statusIsSuccess();
    assertStatusCommandOutput(gfsh.getGfshOutput(), state);
  }

  public void assertStatusCommandOutput(String locatorStatusCommandMessage, LocatorState state) {
    assertSoftly(softly -> {
      softly.assertThat(locatorStatusCommandMessage)
          .contains("Process ID: " + state.getPid())
          .contains("Geode Version: " + state.getGemFireVersion())
          .contains("Java Version: " + state.getJavaVersion())
          .contains("Log File: " + state.getLogFile())
          .contains("JVM Arguments: " + parseJvmArgs(state.getJvmArguments()))
          .containsPattern("Uptime: \\d+");
    });
  }

  public String parseJvmArgs(List<String> jvmArgs) {
    String parsedArgs = jvmArgs.get(0);

    for (int i = 1; i < jvmArgs.size(); i++) {
      parsedArgs += " ";
      parsedArgs += jvmArgs.get(i);
    }

    return parsedArgs;
  }
}
