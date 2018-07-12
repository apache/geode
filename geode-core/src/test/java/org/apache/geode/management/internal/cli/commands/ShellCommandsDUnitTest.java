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

import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

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

@Category({DistributedTest.class})
@SuppressWarnings("serial")
public class ShellCommandsDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;

  public static final int locatorID = 0;

  @BeforeClass
  public static void setupClass() {
    locator = cluster.startLocatorVM(locatorID);
  }

  @Test
  public void testConnectToLocatorBecomesManager() {
    final Result result =
        gfsh.executeCommand("connect --locator=\"localhost[" + locator.getPort() + "]\"");

    assertThat(result).isNotNull();
    assertThat(result.getStatus()).as("Result is not OK: " + result).isEqualTo(Result.Status.OK);
  }

  @Test
  public void testEchoWithVariableAtEnd() {
    gfsh.getGfsh().setEnvProperty("TESTSYS", "SYS_VALUE");

    String command = "echo --string=\"Hello World! This is ${TESTSYS}\"";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Hello World! This is SYS_VALUE");
  }

  @Test
  public void testEchoWithNoVariable() {
    gfsh.getGfsh().setEnvProperty("TESTSYS", "SYS_VALUE");

    String command = "echo --string=\"Hello World! This is Pivotal\"";

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Hello World! This is Pivotal");
  }

  @Test
  public void testEchoWithVariableAtStart() {
    gfsh.getGfsh().setEnvProperty("TESTSYS", "SYS_VALUE");

    String command = "echo --string=\"${TESTSYS} Hello World! This is Pivotal\"";

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE Hello World! This is Pivotal");
  }

  @Test
  public void testEchoWithMultipleVariables() {
    gfsh.getGfsh().setEnvProperty("TESTSYS", "SYS_VALUE");

    String command = "echo --string=\"${TESTSYS} Hello World! This is Pivotal ${TESTSYS}\"";

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE Hello World! This is Pivotal SYS_VALUE");
  }

  @Test
  public void testEchoAllPropertyVariables() {
    String command = "echo --string=\"$*\"";

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  @Test
  public void testEchoForSingleVariable() {
    gfsh.getGfsh().setEnvProperty("TESTSYS", "SYS_VALUE");

    String command = "echo --string=${TESTSYS}";

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE");
  }

  @Test
  public void testEchoForSingleVariable2() {
    gfsh.getGfsh().setEnvProperty("TESTSYS", "SYS_VALUE");

    String command = "echo --string=\"${TESTSYS} ${TESTSYS}\"";

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE");
  }

  @Test
  public void testDebug() {
    gfsh.getGfsh().setDebug(false);

    String command = "debug --state=ON";

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  @Test
  public void testHistoryWithEntry() {
    // Generate a line of history
    gfsh.executeCommand("help");
    gfsh.executeCommand("connect");

    String command = "history";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("  1  0: help\n" + "  2  1: connect\n\n\n");
  }

  @Test
  public void testEmptyHistory() {
    String command = "history";
    CommandResult cmdResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .getCommandResult();

    cmdResult.resetToFirstLine();
    assertEquals("", cmdResult.nextLine());
  }

  @Test
  public void testHistoryWithFileName() {
    gfsh.executeCommand("help");

    String historyFileName = gfsh.getGfsh().getGfshConfig().getHistoryFileName();
    File historyFile = new File(historyFileName);
    historyFile.deleteOnExit();
    String fileName = historyFile.getParent();
    fileName = fileName + File.separator + getClass().getSimpleName() + "_testHistoryWithFileName"
        + "-exported.history";

    String command = "history --file=" + fileName;
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertTrue(historyFile.exists());
    assertTrue(0L != historyFile.length());
  }

  @Test
  public void testClearHistory() {
    // Generate a line of history
    gfsh.executeCommand("help");

    String command = "history --clear";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput(CliStrings.HISTORY__MSG__CLEARED_HISTORY);

    assertTrue(gfsh.getGfsh().getGfshHistory().size() <= 1);
  }
}
