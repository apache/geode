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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class ExportStackTraceCommandDUnitTest {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, locator.getPort());
  }

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void exportStackTrace_no_file() {
    gfsh.executeAndAssertThat("export stack-traces").statusIsSuccess()
        .containsOutput("stack-trace(s) exported to file").containsOutput("On host : ");
    File[] files = locator.getWorkingDir().listFiles(x -> x.getName().startsWith("stacktrace_"));
    assertThat(files.length).isEqualTo(1);
    // delete this file afterwards so that we won't pollute the other tests in this class
    files[0].delete();
  }

  @Test
  @Parameters({"server-1", "locator-0"})
  public void exportStackTrace_on_one_member(String memberName) {
    gfsh.executeAndAssertThat("export stack-traces --member=" + memberName).statusIsSuccess()
        .containsOutput("stack-trace(s) exported to file").containsOutput("On host : ");
    File[] files = locator.getWorkingDir().listFiles(x -> x.getName().startsWith("stacktrace_"));
    assertThat(files.length).isEqualTo(1);
    // delete this file afterwards so that we won't pollute the other tests in this class
    files[0].delete();
  }

  @Test
  public void exportStackTrace_with_file() {
    File stackTraceFile = new File(locator.getWorkingDir(), "my_file");
    gfsh.executeAndAssertThat("export stack-traces --file=" + stackTraceFile.getAbsolutePath())
        .statusIsSuccess().containsOutput("stack-trace(s) exported to file");

    // make sure file exists afterwards
    File[] files = locator.getWorkingDir().listFiles(x -> x.getName().startsWith("my_file"));
    assertThat(files.length).isEqualTo(1);

    // execute the command again with the abort flag
    gfsh.executeAndAssertThat(
        "export stack-traces --abort-if-file-exists --file=" + stackTraceFile.getAbsolutePath())
        .statusIsError().containsOutput("already present");

    // execute the command again without the abort flag
    gfsh.executeAndAssertThat("export stack-traces --file=" + stackTraceFile.getAbsolutePath())
        .statusIsSuccess().containsOutput("stack-trace(s) exported to file");
    // make sure the file is overwritten
    files = locator.getWorkingDir().listFiles(x -> x.getName().startsWith("my_file"));
    assertThat(files.length).isEqualTo(1);

    // delete this file afterwards so that we won't pollute the other tests in this class
    files[0].delete();
  }

  @Test
  public void exportStackTraceCheckFileContent() throws IOException {
    File stackTraceFile = new File(locator.getWorkingDir(), "my_file");

    gfsh.executeAndAssertThat("export stack-traces --file=" + stackTraceFile.getAbsolutePath())
        .statusIsSuccess().containsOutput("stack-trace(s) exported to file");

    // make sure file exists afterwards
    File[] files = locator.getWorkingDir().listFiles(x -> x.getName().startsWith("my_file"));
    assertThat(files.length).isEqualTo(1);

    BufferedReader bufferedReader = new BufferedReader(new FileReader(stackTraceFile));
    String firstLine = bufferedReader.readLine();

    String regex = "(\\d{4}\\/\\d{2}\\/\\d{2}\\s\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3})";
    boolean dateExist = Pattern.compile(regex).matcher(firstLine).find();

    assertThat(dateExist).isEqualTo(true);

    // delete this file afterwards so that we won't pollute the other tests in this class
    files[0].delete();
  }
}
