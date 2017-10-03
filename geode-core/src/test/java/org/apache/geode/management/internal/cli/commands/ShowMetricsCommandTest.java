package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.rules.GfshParserRule;
import org.junit.Rule;
import org.junit.Test;

public class ShowMetricsCommandTest {

  @Rule
  public GfshParserRule parser = new GfshParserRule();

  @Test
  public void testPortAndRegion() throws Exception {
    ShowMetricsCommand command = spy(ShowMetricsCommand.class);
    CommandResult result = parser.executeCommandWithInstance(command, "show metrics --port=0 --region=regionA");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("The --region and --port parameters are mutually exclusive");
  }

  @Test
  public void testPortOnly() throws Exception {
    ShowMetricsCommand command = spy(ShowMetricsCommand.class);
    CommandResult result = parser.executeCommandWithInstance(command, "show metrics --port=0");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("If the --port parameter is specified, then the --member parameter must also be specified.");
  }

  @Test
  public void invalidPortNumber() throws Exception {
    ShowMetricsCommand command = spy(ShowMetricsCommand.class);
    CommandResult result = parser.executeCommandWithInstance(command, "show metrics --port=abc");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("Invalid port");
  }
}