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
import static org.apache.geode.internal.i18n.LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE;
import static org.apache.geode.internal.i18n.LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.LOCATOR_TERM_NAME;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.STATUS_LOCATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;

public class StatusLocatorCommandDunitTest {
  private static final Integer TIMEOUT = 300;
  private static final Integer INTERVAL = 10;
  private static final String locatorName = "locator";

  private static LocatorLauncher locator;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static TemporaryFolder tempDir = new TemporaryFolder();

  @BeforeClass
  public static void before() throws IOException {
    File workingDir = tempDir.newFolder("workingDir");

    locator = new LocatorLauncher.Builder().setMemberName(locatorName).setPort(0)
        .setWorkingDirectory(workingDir.getAbsolutePath())
        .set("cluster-configuration-dir", workingDir.getAbsolutePath()).build();
    locator.start();
  }

  @Test
  public void testWithDisconnectedGfsh() throws Exception {
    final String expectedResult =
        CliStrings.format(CliStrings.STATUS_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Locator");

    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --name=" + locatorName)
        .statusIsError()
        .containsOutput(expectedResult);
  }

  @Test
  public void testWithMemberAddress() throws Exception {
    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    LocatorState state = locator.waitOnStatusResponse(TIMEOUT, INTERVAL, SECONDS);

    CommandResult result =
        gfsh.executeCommand(STATUS_LOCATOR + " --host=localhost --port=" + locator.getPort());

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertStatusCommandOutput(result.getMessageFromContent(), state);
  }

  @Test
  public void testWithInvalidMemberAddress() throws Exception {
    final String expectedError =
        Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toString(LOCATOR_TERM_NAME);

    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    CommandResult result =
        gfsh.executeCommand(STATUS_LOCATOR + " --host=invalidHostName --port=" + locator.getPort());

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMemberName() throws Exception {
    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    LocatorState state = locator.waitOnStatusResponse(TIMEOUT, INTERVAL, SECONDS);

    CommandResult result = gfsh.executeCommand(STATUS_LOCATOR + " --name=" + locatorName);

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertStatusCommandOutput(result.getMessageFromContent(), state);
  }

  @Test
  public void testWithInvalidMemberName() throws Exception {
    final String expectedError = CliStrings.format(
        CliStrings.STATUS_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE, "invalidLocatorName");

    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    CommandResult result = gfsh.executeCommand(STATUS_LOCATOR + " --name=invalidLocatorName");

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMemberID() throws Exception {
    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    LocatorState state = locator.waitOnStatusResponse(TIMEOUT, INTERVAL, SECONDS);

    CommandResult result = gfsh.executeCommand(STATUS_LOCATOR + " --name=" + locator.getMemberId());

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertStatusCommandOutput(result.getMessageFromContent(), state);
  }

  @Test
  public void testWithInvalidMemberID() throws Exception {
    final String expectedError =
        CliStrings.format(CliStrings.STATUS_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE,
            locator.getMemberId() + "1");

    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    CommandResult result =
        gfsh.executeCommand(STATUS_LOCATOR + " --name=" + locator.getMemberId() + "1");

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithDirOnline() throws Exception {
    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    LocatorState state = locator.waitOnStatusResponse(TIMEOUT, INTERVAL, SECONDS);

    CommandResult result =
        gfsh.executeCommand(STATUS_LOCATOR + " --dir=" + locator.getWorkingDirectory());

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertStatusCommandOutput(result.getMessageFromContent(), state);
  }

  @Test
  public void testWithDirOffline() throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    LocatorState state = locator.waitOnStatusResponse(TIMEOUT, INTERVAL, SECONDS);

    CommandResult result =
        gfsh.executeCommand(STATUS_LOCATOR + " --dir=" + locator.getWorkingDirectory());

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertStatusCommandOutput(result.getMessageFromContent(), state);
  }

  @Test
  public void testWithInvalidDirOnline() throws Exception {
    final String expectedError =
        Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE.toString(LOCATOR_TERM_NAME);

    gfsh.connectAndVerify(locator.getPort(), PortType.locator);

    CommandResult result = gfsh.executeCommand(STATUS_LOCATOR + " --dir=/invalid/working/dir");

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithInvalidDirOffline() throws Exception {
    final String expectedError =
        Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE.toString(LOCATOR_TERM_NAME);

    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    CommandResult result = gfsh.executeCommand(STATUS_LOCATOR + " --dir=/invalid/working/dir");

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @AfterClass
  public static void after() {
    locator.stop();
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
