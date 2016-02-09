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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.domain.DataCommandRequest;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;

import org.junit.Before;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ShellCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  public ShellCommandsDUnitTest(String name) {
    super(name);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    getDefaultShell();
  }

  protected CommandResult connectToLocator(final int locatorPort) {
    return executeCommand(new CommandStringBuilder(CliStrings.CONNECT).addOption(CliStrings.CONNECT__LOCATOR,
        "localhost[" + locatorPort + "]").toString());
  }

  public void testConnectToLocatorBecomesManager() {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    System.setProperty("gemfire.jmx-manager-port", String.valueOf(jmxManagerPort));
    System.setProperty("gemfire.jmx-manager-http-port", "0");

    assertEquals(String.valueOf(jmxManagerPort), System.getProperty("gemfire.jmx-manager-port"));
    assertEquals("0", System.getProperty("gemfire.jmx-manager-http-port"));

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

  public void testEchoWithVariableAtEnd() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testEcho command gfshInstance is null");
    }
    LogWriterUtils.getLogWriter().info("Gsh " + gfshInstance);

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

  public void testHistory() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testHistory command gfshInstance is null");
    }

    gfshInstance.setDebug(false);
    String command = "history";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testHistory failed");
    }
  }

  public void testHistoryWithFileName() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testHistory command gfshInstance is null");
    }

    String historyFileName = gfshInstance.getGfshConfig().getHistoryFileName();
    File historyFile = new File(historyFileName);
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
  }

  public void testClearHistory() {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testClearHistory command gfshInstance is null");
    }

    gfshInstance.setDebug(false);
    String command = "history --clear";
    CommandResult cmdResult = executeCommand(command);
    printCommandOutput(cmdResult);

    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      LogWriterUtils.getLogWriter().info("testClearHistory cmdResult=" + commandResultToString(cmdResult));
      String resultString = commandResultToString(cmdResult);
      LogWriterUtils.getLogWriter().info("testClearHistory resultString=" + resultString);
      assertTrue(resultString.contains(CliStrings.HISTORY__MSG__CLEARED_HISTORY));
      assertTrue(gfshInstance.getGfshHistory().size()<= 1);
    } else {
      fail("testClearHistory failed");
    }
  }

  private static void printCommandOutput(CommandResult cmdResult) {
    assertNotNull(cmdResult);
    LogWriterUtils.getLogWriter().info("Command Output : ");
    StringBuilder sb = new StringBuilder();
    cmdResult.resetToFirstLine();
    while (cmdResult.hasNextLine()) {
      sb.append(cmdResult.nextLine()).append(DataCommandRequest.NEW_LINE);
    }
    LogWriterUtils.getLogWriter().info(sb.toString());
    LogWriterUtils.getLogWriter().info("");
  }

  private void printAllEnvs(Gfsh gfsh) {
    LogWriterUtils.getLogWriter().info("printAllEnvs : " + StringUtils.objectToString(gfsh.getEnv(), false, 0));
    /*
    getLogWriter().info("Gfsh printAllEnvs : " + HydraUtil.ObjectToString(getDefaultShell().getEnv()));    
    getLogWriter().info("gfsh " + gfsh + " default shell " + getDefaultShell());*/
  }
}
