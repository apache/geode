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
package org.apache.geode.management.internal.cli;

import static java.lang.System.lineSeparator;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.Completion;

import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshParserRule;
import org.apache.geode.test.junit.rules.GfshParserRule.CommandCandidate;

@Category(GfshTest.class)
public class GfshParserAutoCompletionIntegrationTest {

  @Rule
  public GfshParserRule gfshParserRule = new GfshParserRule();

  @Test
  public void testCompletionDescribe() {
    String buffer = "describe";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(9);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDescribeWithSpace() {
    String buffer = "describe ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(9);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDeploy() {
    String buffer = "deploy";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --dir");
  }

  @Test
  public void testCompletionDeployWithSpace() {
    String buffer = "deploy ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--dir");
  }

  @Test
  public void testCompleteWithRequiredOption() {
    String buffer = "describe config";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --member");
  }

  @Test
  public void testCompleteWithRequiredOptionWithSpace() {
    String buffer = "describe config ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--member");
  }

  @Test
  public void testCompletionStart() {
    String buffer = "start";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isEqualTo(8);
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("gateway-receiver")))
            .isTrue();
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("vsd")))
            .isTrue();
  }

  @Test
  public void testCompletionStartWithSpace() {
    String buffer = "start ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isEqualTo(8);
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("gateway-receiver")))
            .isTrue();
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("vsd")))
            .isTrue();
  }

  @Test
  public void testCompleteCommand() {
    String buffer = "start ser";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat("start server").isEqualTo(candidate.getFirstCandidate());
  }

  @Test
  public void testCompleteOptionWithOnlyOneCandidate() {
    String buffer = "start server --nam";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "e");
  }

  @Test
  public void testCompleteOptionWithMultipleCandidates() {
    String buffer = "start server --name=jinmei --loc";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ator-wait-time");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "ators");
    assertThat(candidate.getCandidate(2)).isEqualTo(buffer + "k-memory");
  }

  @Test
  public void testCompleteWithExtraSpace() {
    String buffer = "start server --name=name1  --se";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo("start server --name=name1  ".length());
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getCandidates()).contains(new Completion("--server-port"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "curity-properties-file");
  }

  @Test
  public void testCompleteWithDashInTheEnd() {
    String buffer = "start server --name=name1 --";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.getCandidates()).hasSize(53);
    assertThat(candidate.getCandidates()).contains(new Completion("--properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "J");
  }

  @Test
  public void testCompleteWithSpace() {
    String buffer = "start server --name=name1 ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 1);
    assertThat(candidate.getCandidates()).hasSize(53);
    assertThat(candidate.getCandidates()).contains(new Completion(" --properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--J");
  }

  @Test
  public void testCompleteWithOutSpace() {
    String buffer = "start server --name=name1";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(53);
    assertThat(candidate.getCandidates()).contains(new Completion(" --properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --J");
  }

  @Test
  public void testCompleteJ() {
    String buffer = "start server --name=name1 --J=";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.getCandidates()).hasSize(1);
  }

  @Test
  public void testCompleteWithValue() {
    String buffer = "start server --name=name1 --J";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.getCandidates()).hasSize(1);
  }

  @Test
  public void testCompleteWithDash() {
    String buffer = "start server --name=name1 --J=-Dfoo.bar --";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(52);
  }

  @Test
  public void testCompleteWithMultipleJ() {
    String buffer = "start server --name=name1 --J=-Dme=her --J=-Dfoo=bar --l";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor())
        .isEqualTo("start server --name=name1 --J=-Dme=her --J=-Dfoo=bar ".length());
    assertThat(candidate.getCandidates()).hasSize(4);
    assertThat(candidate.getCandidates()).contains(new Completion("--locators"));
  }

  @Test
  public void testMultiJComplete() {
    String buffer = "start server --name=name1 --J=-Dtest=test1 --J=-Dfoo=bar";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(52);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testMultiJCompleteWithDifferentOrder() {
    String buffer = "start server --J=-Dtest=test1 --J=-Dfoo=bar --name=name1";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(52);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete3() {
    String buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(51);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete4() {
    String buffer = "start server --name=name1 --locators=localhost  --J=-Dfoo=bar --";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.getCandidates()).hasSize(51);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "assign-buckets");
  }

  @Test
  public void testCompleteRegionType() {
    String buffer = "create region --name=test --type";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(23);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=LOCAL");
  }

  @Test
  public void testCompletePartialRegionType() {
    String buffer = "create region --name=test --type=LO";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "CAL");
  }

  @Test
  public void testCompleteWithRegionTypeWithNoSpace() {
    String buffer = "create region --name=test --type=REPLICATE";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "_HEAP_LRU");
  }

  @Test
  public void testCompleteWithRegionTypeWithSpace() {
    String buffer = "create region --name=test --type=REPLICATE ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(45);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--async-event-queue-id");
  }

  @Test
  public void testCompleteLogLevel() {
    String buffer = "change loglevel --loglevel";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=ALL");
  }

  @Test
  public void testCompleteLogLevelWithEqualSign() {
    String buffer = "change loglevel --loglevel=";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ALL");
  }

  @Test
  public void testCompleteHintNonexistemt() {
    String buffer = "hint notfound";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(0);
  }

  @Test
  public void testCompleteHintNada() {
    String buffer = "hint";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isGreaterThan(10);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase("hint client");
  }

  @Test
  public void testCompleteHintSpace() {
    String buffer = "hint ";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isGreaterThan(10);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase("hint client");
  }

  @Test
  public void testCompleteHintPartial() {
    String buffer = "hint d";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase("hint data");
  }

  @Test
  public void testCompleteHintAlreadyComplete() {
    String buffer = "hint data";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase(buffer);
  }

  @Test
  public void testCompleteHelpFirstWord() {
    String buffer = "help start";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " gateway-receiver");
  }

  @Test
  public void testCompleteHelpPartialFirstWord() {
    String buffer = "help st";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(17);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "art gateway-receiver");
  }

  @Test
  public void testObtainHelp() {
    String command = CliStrings.START_PULSE;
    String helpString = "NAME" + lineSeparator() + "start pulse" + lineSeparator() + "IS AVAILABLE"
        + lineSeparator() + "true" + lineSeparator() + "SYNOPSIS" + lineSeparator()
        + "Open a new window in the default Web browser with the URL for the Pulse application."
        + lineSeparator()
        + "SYNTAX" + lineSeparator() + "start pulse [--url=value]" + lineSeparator() + "PARAMETERS"
        + lineSeparator() + "url" + lineSeparator()
        + "URL of the Pulse Web application." + lineSeparator() + "Required: false"
        + lineSeparator()
        + "Default (if the parameter is not specified): http://localhost:7070/pulse"
        + lineSeparator();
    assertThat(gfshParserRule.getCommandManager().obtainHelp(command)).isEqualTo(helpString);
  }

  @Test
  public void testObtainHelpForStart() {
    String command = "start";
    String helpProvided = gfshParserRule.getCommandManager().getHelper().getHelp(command, 1000);
    String[] helpProvidedArray = helpProvided.split(lineSeparator());
    assertThat(helpProvidedArray.length).isEqualTo(8 * 2 + 3);
    for (int i = 0; i < helpProvidedArray.length - 3; i++) {
      if (i % 2 != 0) {
        assertThat(helpProvidedArray[i]).startsWith("    ");
      } else {
        assertThat(helpProvidedArray[i]).startsWith(command);
      }
    }
  }

  @Test
  public void testObtainHintForData() {
    String hintArgument = "data";
    String hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    String[] hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray.length).isEqualTo(15);
    assertThat(hintsProvidedArray[0])
        .isEqualTo("User data as stored in regions of the Geode distributed system.");
  }

  @Test
  public void testObtainHintWithoutArgument() {
    String hintArgument = "";
    String hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    String[] hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray.length).isEqualTo(21);
    assertThat(hintsProvidedArray[0]).isEqualTo(
        "Hints are available for the following topics. Use \"hint <topic-name>\" for a specific hint.");
  }

  @Test
  public void testObtainHintWithNonExistingCommand() {
    String hintArgument = "fortytwo";
    String hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    String[] hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray.length).isEqualTo(1);
    assertThat(hintsProvidedArray[0]).isEqualTo(
        "Unknown topic: " + hintArgument + ". Use hint to view the list of available topics.");
  }

  @Test
  public void testObtainHintWithPartialCommand() {
    String hintArgument = "d";
    String hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    System.out.println(hintsProvided);
    String[] hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray.length).isEqualTo(5);
    assertThat(hintsProvidedArray[0]).isEqualTo(
        "Hints are available for the following topics. Use \"hint <topic-name>\" for a specific hint.");
    assertThat(hintsProvidedArray).contains("Data");
    assertThat(hintsProvidedArray).contains("Debug-Utility");
    assertThat(hintsProvidedArray).contains("Disk Store");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testIndexType() {
    String buffer = "create index --type=";
    CommandCandidate candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates())
        .hasSize(org.apache.geode.cache.query.IndexType.values().length);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "hash");
  }
}
