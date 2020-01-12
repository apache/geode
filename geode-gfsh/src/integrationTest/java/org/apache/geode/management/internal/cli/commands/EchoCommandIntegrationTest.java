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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class EchoCommandIntegrationTest {

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void beforeClass() {
    gfsh.executeAndAssertThat("set variable --name=TESTSYS --value=SYS_VALUE").statusIsSuccess()
        .containsOutput("Value for variable TESTSYS is now: SYS_VALUE");
  }

  @Test
  public void echoWithNoVariable() {
    gfsh.executeAndAssertThat("echo  --string=\"Hello World! This is pivotal\"").statusIsSuccess()
        .containsOutput("Hello World! This is pivotal");
  }

  @Test
  public void echoWithVariableAtEnd() {
    gfsh.executeAndAssertThat("echo  --string=\"Hello World! This is ${TESTSYS}\"")
        .statusIsSuccess()
        .containsOutput("Hello World! This is SYS_VALUE");
  }

  @Test
  public void testEchoWithVariableAtStart() {
    String command = "echo --string=\"${TESTSYS} Hello World! This is Pivotal\"";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE Hello World! This is Pivotal");
  }

  @Test
  public void testEchoWithMultipleVariables() {
    String command = "echo --string=\"${TESTSYS} Hello World! This is Pivotal ${TESTSYS}\"";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE Hello World! This is Pivotal SYS_VALUE");
  }

  @Test
  public void testEchoAllPropertyVariables() {
    String command = "echo --string=\"$*\"";
    CommandResult commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .getCommandResult();
    assertThat(
        commandResult.getResultData().getTableSections().get(0).getValuesInColumn("Property"))
            .containsExactlyInAnyOrder("APP_COLLECTION_LIMIT",
                "APP_FETCH_SIZE",
                "APP_LAST_EXIT_STATUS",
                "APP_LOGGING_ENABLED",
                "APP_LOG_FILE",
                "APP_NAME",
                "APP_PWD",
                "APP_QUERY_RESULTS_DISPLAY_MODE",
                "APP_QUIET_EXECUTION",
                "APP_RESULT_VIEWER",
                "SYS_CLASSPATH",
                "SYS_GEODE_HOME_DIR",
                "SYS_HOST_NAME",
                "SYS_JAVA_VERSION",
                "SYS_OS",
                "SYS_OS_LINE_SEPARATOR",
                "SYS_USER",
                "SYS_USER_HOME",
                "TESTSYS");
  }

  @Test
  public void testEchoForSingleVariable() {
    String command = "echo --string=${TESTSYS}";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE");
  }

  @Test
  public void testEchoForSingleVariable2() {
    String command = "echo --string=\"${TESTSYS} ${TESTSYS}\"";

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("SYS_VALUE SYS_VALUE");
  }
}
