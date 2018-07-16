package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category(IntegrationTest.class)
public class DebugCommandIntegrationTest {
  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void debugWithCorrectValues() {
    assertThat(gfsh.getGfsh().getDebug()).isFalse();
    gfsh.executeAndAssertThat("debug --state=ON").statusIsSuccess();
    assertThat(gfsh.getGfsh().getDebug()).isTrue();

    gfsh.executeAndAssertThat("debug --state=off").statusIsSuccess();
    assertThat(gfsh.getGfsh().getDebug()).isFalse();
  }

  @Test
  public void debugWithIncorrectValues() {
    String errorMsg = "Invalid state value : true. It should be \"ON\" or \"OFF\"";
    gfsh.executeAndAssertThat("debug --state=true").statusIsError()
        .containsOutput(errorMsg);
    gfsh.executeAndAssertThat("debug --state=0").statusIsError()
        .containsOutput(errorMsg);
  }
}
