package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category(IntegrationTest.class)
public class HistoryCommandIntegrationTest {
  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() throws Exception {
    gfsh.getGfsh().clearHistory();
  }

  @Test
  public void testHistoryWithEntry() {
    // Generate a line of history
    gfsh.executeCommand("echo --string=string");
    gfsh.executeCommand("connect");

    gfsh.executeAndAssertThat("history").statusIsSuccess()
        .containsOutput("  1  0: echo --string=string\n" + "  2  1: connect\n\n\n");
  }

  @Test
  public void testEmptyHistory() {
    gfsh.executeAndAssertThat("history").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).isEqualToIgnoringWhitespace("");
  }

  @Test
  public void testHistoryWithFileName() throws IOException {
    gfsh.executeCommand("echo --string=string");
    File historyFile = temporaryFolder.newFile("history.txt");
    historyFile.delete();
    assertThat(historyFile).doesNotExist();

    String command = "history --file=" + historyFile.getAbsolutePath();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertThat(historyFile).exists();
    assertThat(historyFile).hasContent("0: echo --string=string");
  }

  @Test
  public void testClearHistory() {
    // Generate a line of history
    gfsh.executeCommand("echo --string=string");
    gfsh.executeAndAssertThat("history --clear").statusIsSuccess()
        .containsOutput(CliStrings.HISTORY__MSG__CLEARED_HISTORY);

    // only the history --clear is in the history now.
    assertThat(gfsh.getGfsh().getGfshHistory().size()).isEqualTo(1);
  }
}
