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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class HistoryCommandIntegrationTest {
  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    gfsh.getGfsh().clearHistory();
  }

  @Test
  public void testHistoryWithEntry() {
    // Generate a line of history
    gfsh.executeAndAssertThat("echo --string=string");
    gfsh.executeAndAssertThat("connect");

    gfsh.executeAndAssertThat("history").statusIsSuccess()
        .hasInfoSection()
        .hasLines()
        .containsExactly("0: echo --string=string", "1: connect");
  }

  @Test
  public void testEmptyHistory() {
    gfsh.executeAndAssertThat("history").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).isEqualToIgnoringWhitespace("");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHistoryWithFileName() throws IOException {
    gfsh.executeCommand("echo --string=string");
    File historyFile = temporaryFolder.newFile("history.txt");
    Files.deleteIfExists(historyFile.toPath());
    assertThat(historyFile).doesNotExist();

    String command = "history --file=" + historyFile.getAbsolutePath();
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Wrote successfully to file");

    assertThat(historyFile).exists();
    assertThat(historyFile).hasContent("0: echo --string=string");
  }

  @Test
  public void testClearHistory() {
    // Generate a line of history
    gfsh.executeAndAssertThat("echo --string=string");
    gfsh.executeAndAssertThat("history --clear").statusIsSuccess()
        .containsOutput(CliStrings.HISTORY__MSG__CLEARED_HISTORY);

    // only the history --clear is in the history now.
    assertThat(gfsh.getGfsh().getGfshHistory().size()).isEqualTo(1);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHistoryContainsRedactedPasswordWithEquals() throws IOException {
    gfsh.executeCommand("connect --password=redacted");
    File historyFile = temporaryFolder.newFile("history.txt");
    Files.deleteIfExists(historyFile.toPath());
    assertThat(historyFile).doesNotExist();

    String command = "history --file=" + historyFile.getAbsolutePath();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertThat(historyFile).exists();

    List<String> historyLines = Files.readAllLines(historyFile.toPath());
    assertThat(historyLines.get(0)).isEqualTo("0: connect --password=********");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHistoryContainsRedactedPasswordWithoutEquals() throws IOException {
    gfsh.executeCommand("connect --password redacted");
    File historyFile = temporaryFolder.newFile("history.txt");
    Files.deleteIfExists(historyFile.toPath());
    assertThat(historyFile).doesNotExist();

    String command = "history --file=" + historyFile.getAbsolutePath();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertThat(historyFile).exists();

    List<String> historyLines = Files.readAllLines(historyFile.toPath());
    assertThat(historyLines.get(0)).isEqualTo("0: connect --password ********");
  }
}
