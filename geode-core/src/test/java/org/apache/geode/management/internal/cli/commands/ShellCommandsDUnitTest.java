/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

@Category(DistributedTest.class)
public class ShellCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  @Override
  public final void postSetUpCliCommandTestBase() throws Exception {
    getDefaultShell();
  }

  protected CommandResult connectToLocator(final int locatorPort) {
    return executeCommand(new CommandStringBuilder(CliStrings.CONNECT).addOption(CliStrings.CONNECT__LOCATOR,
        "localhost[" + locatorPort + "]").toString());
  }

  @Category(FlakyTest.class) // GEODE-989: random ports, suspect string: DiskAccessException, disk pollution, HeadlessGfsh, time sensitive
  @Test
  public void testConnectToLocatorBecomesManager() {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "jmx-manager-port", String.valueOf(jmxManagerPort));
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "jmx-manager-http-port", "0");

    assertEquals(String.valueOf(jmxManagerPort), System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "jmx-manager-port"));
    assertEquals("0", System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "jmx-manager-http-port"));

    final String pathname = (getClass().getSimpleName() + "_" + getTestMethodName());
    final File workingDirectory = new File(pathname);

    workingDirectory.mkdir();

    assertTrue(workingDirectory.isDirectory());

    final LocatorLauncher locatorLauncher = new LocatorLauncher.Builder().setBindAddress(null).setForce(
        true).setMemberName(pathname).setPort(locatorPort).setWorkingDirectory(
        IOUtils.tryGetCanonicalPathElseGetAbsolutePath(workingDirectory)).build();

    assertNotNull(locatorLauncher);
    assertEquals(locatorPort, locatorLauncher.getPort().intValue());

    try {
      // fix for bug 46729
      locatorLauncher.start();

      final LocatorState locatorState = locatorLauncher.waitOnStatusResponse(60, 10, TimeUnit.SECONDS);

      assertNotNull(locatorState);
      assertEquals(Status.ONLINE, locatorState.getStatus());

      final Result result = connectToLocator(locatorPort);

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());
    } finally {
      assertEquals(Status.STOPPED, locatorLauncher.stop().getStatus());
      assertEquals(Status.NOT_RESPONDING, locatorLauncher.status().getStatus());
    }
  }

  @Test
  public void testEchoWithVariableAtEnd() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }
    getLogWriter().info("Gsh " + gfshInstance);

    gfshInstance.setEnvProperty("TESTSYS", "SYS_VALUE");
    printAllEnvs(gfshInstance);

    String command = "echo --string=\"Hello World! This is ${TESTSYS}\"";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      assertEquals("Hello World! This is SYS_VALUE", StringUtils.trim(stringResult));
    } else {
      fail("testEchoWithVariableAtEnd failed");
    }
  }

  @Test
  public void testEchoWithNoVariable() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }

    gfshInstance.setEnvProperty("TESTSYS", "SYS_VALUE");
    printAllEnvs(gfshInstance);

    String command = "echo --string=\"Hello World! This is Pivotal\"";

    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      assertTrue(stringResult.contains("Hello World! This is Pivotal"));
    } else {
      fail("testEchoWithNoVariable failed");
    }
  }

  @Test
  public void testEchoWithVariableAtStart() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }

    gfshInstance.setEnvProperty("TESTSYS", "SYS_VALUE");
    printAllEnvs(gfshInstance);

    String command = "echo --string=\"${TESTSYS} Hello World! This is Pivotal\"";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      assertTrue(stringResult.contains("SYS_VALUE Hello World! This is Pivotal"));
    } else {
      fail("testEchoWithVariableAtStart failed");
    }
  }

  @Test
  public void testEchoWithMultipleVariables() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }

    gfshInstance.setEnvProperty("TESTSYS", "SYS_VALUE");
    printAllEnvs(gfshInstance);

    String command = "echo --string=\"${TESTSYS} Hello World! This is Pivotal ${TESTSYS}\"";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      assertTrue(stringResult.contains("SYS_VALUE Hello World! This is Pivotal SYS_VALUE"));
    } else {
      fail("testEchoWithMultipleVariables failed");
    }
  }

  @Test
  public void testEchoAllPropertyVariables() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }

    String command = "echo --string=\"$*\"";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testEchoAllPropertyVariables failed");
    }
  }

  @Test
  public void testEchoForSingleVariable() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }

    gfshInstance.setEnvProperty("TESTSYS", "SYS_VALUE");
    printAllEnvs(gfshInstance);

    String command = "echo --string=${TESTSYS}";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);


    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      assertTrue(stringResult.contains("SYS_VALUE"));
    } else {
      fail("testEchoForSingleVariable failed");
    }
  }

  @Test
  public void testEchoForSingleVariable2() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }

    gfshInstance.setEnvProperty("TESTSYS", "SYS_VALUE");
    printAllEnvs(gfshInstance);

    String command = "echo --string=\"${TESTSYS} ${TESTSYS}\"";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      assertTrue(stringResult.contains("SYS_VALUE"));
    } else {
      fail("testEchoForSingleVariable2 failed");
    }
  }

  @Test
  public void testDebug() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testDebug command gfshInstance is null");
    }

    gfshInstance.setDebug(false);
    String command = "debug --state=ON";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testDebug failed");
    }
    assertEquals(gfshInstance.getDebug(), true);

  }

  @Test
  public void testHistoryWithEntry() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testHistory command gfshInstance is null");
    }

    gfshInstance.setDebug(false);

    // Generate a line of history
    executeCommand("help");
    executeCommand("connect");

    String command = "history";
    CommandResult cmdResult = executeCommand(command);
    String result = printCommandOutput(cmdResult);

    assertEquals("  1  0: help\n  2  1: connect\n\n\n", result);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testHistory failed");
    }
  }

  @Test
  public void testEmptyHistory() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testHistory command gfshInstance is null");
    }

    gfshInstance.setDebug(false);

    String command = "history";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);
    cmdResult.resetToFirstLine();
    assertEquals("", cmdResult.nextLine());

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testHistory failed");
    }
  }

  @Test
  public void testHistoryWithFileName() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testHistory command gfshInstance is null");
    }

    // Generate a line of history
    executeCommand("help");

    String historyFileName = gfshInstance.getGfshConfig().getHistoryFileName();
    File historyFile = new File(historyFileName);
    historyFile.deleteOnExit();
    String fileName = historyFile.getParent();
    fileName = fileName + File.separator + getClass().getSimpleName() + "_" + getName() + "-exported.history";

    String command = "history --file=" + fileName;
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testHistory failed");
    }
    assertTrue(historyFile.exists());
    assertTrue(0L != historyFile.length());
  }

  @Test
  public void testClearHistory() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testClearHistory command gfshInstance is null");
    }

    gfshInstance.setDebug(false);

    // Generate a line of history
    executeCommand("help");

    String command = "history --clear";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      getLogWriter().info("testClearHistory cmdResult=" + commandResultToString(cmdResult));
      String resultString = commandResultToString(cmdResult);
      getLogWriter().info("testClearHistory resultString=" + resultString);
      assertTrue(resultString.contains(CliStrings.HISTORY__MSG__CLEARED_HISTORY));
      assertTrue(gfshInstance.getGfshHistory().size()<= 1);
    } else {
      fail("testClearHistory failed");
    }
  }

  private static String printCommandOutput(CommandResult cmdResult) {
    assertNotNull(cmdResult);
    getLogWriter().info("Command Output : ");
    StringBuilder sb = new StringBuilder();
    cmdResult.resetToFirstLine();
    while (cmdResult.hasNextLine()) {
      sb.append(cmdResult.nextLine()).append(DataCommandRequest.NEW_LINE);
    }
    getLogWriter().info(sb.toString());
    getLogWriter().info("");
    return sb.toString();
  }

  private void printAllEnvs(Gfsh gfsh) {
    getLogWriter().info("printAllEnvs : " + StringUtils.objectToString(gfsh.getEnv(), false, 0));
    /*
    getLogWriter().info("Gfsh printAllEnvs : " + HydraUtil.ObjectToString(getDefaultShell().getEnv()));    
    getLogWriter().info("gfsh " + gfsh + " default shell " + getDefaultShell());*/
  }
}
