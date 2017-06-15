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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.GfshParserRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.Completion;

@Category(IntegrationTest.class)
public class GfshParserAutoCompletionTest {
  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private String buffer;
  private GfshParserRule.CommandCandidate candidate;

  @Test
  public void testCompletionDescibe() throws Exception {
    buffer = "describe";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(7);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDescibeWithSpace() throws Exception {
    buffer = "describe ";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(7);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDeploy() throws Exception {
    buffer = "deploy";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --dir");
  }

  @Test
  public void testCompletionDeployWithSpace() throws Exception {
    buffer = "deploy ";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--dir");
  }

  @Test
  public void testCompleteWithRequiredOption() throws Exception {
    buffer = "describe config";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --member");
  }

  @Test
  public void testCompleteWithRequiredOptionWithSpace() throws Exception {
    buffer = "describe config ";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--member");
  }

  @Test
  public void testCompleteCommand() throws Exception {
    buffer = "start ser";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(1);
    assertThat("start server").isEqualTo(candidate.getFirstCandidate());
  }

  @Test
  public void testCompleteOptionWithOnlyOneCandidate() throws Exception {
    buffer = "start server --nam";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "e");
  }

  @Test
  public void testCompleteOptionWithMultipleCandidates() throws Exception {
    buffer = "start server --name=jinmei --loc";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ator-wait-time");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "ators");
    assertThat(candidate.getCandidate(2)).isEqualTo(buffer + "k-memory");
  }

  @Test
  public void testCompleteWithExtraSpace() throws Exception {
    buffer = "start server --name=name1  --se";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo("start server --name=name1  ".length());
    assertThat(candidate.size()).isEqualTo(3);
    assertTrue(candidate.getCandidates().contains(new Completion("--server-port")));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "curity-properties-file");
  }

  @Test
  public void testCompleteWithDashInTheEnd() throws Exception {
    buffer = "start server --name=name1 --";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.size()).isEqualTo(51);
    assertTrue(candidate.getCandidates().contains(new Completion("--properties-file")));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "J");
  }

  @Test
  public void testCompleteWithSpace() throws Exception {
    buffer = "start server --name=name1 ";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 1);
    assertThat(candidate.size()).isEqualTo(51);
    assertTrue(candidate.getCandidates().contains(new Completion(" --properties-file")));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--J");
  }

  @Test
  public void testCompleteWithOutSpace() throws Exception {
    buffer = "start server --name=name1";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.size()).isEqualTo(51);
    assertTrue(candidate.getCandidates().contains(new Completion(" --properties-file")));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --J");
  }

  @Test
  public void testCompleteJ() throws Exception {
    buffer = "start server --name=name1 --J=";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.size()).isEqualTo(1);
  }

  @Test
  public void testCompleteWithValue() throws Exception {
    buffer = "start server --name=name1 --J";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.size()).isEqualTo(1);
  }

  @Test
  public void testCompleteWithDash() throws Exception {
    buffer = "start server --name=name1 --J=-Dfoo.bar --";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(50);
  }

  @Test
  public void testCompleteWithMultipleJ() throws Exception {
    buffer = "start server --name=name1 --J=-Dme=her --J=-Dfoo=bar --l";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor())
        .isEqualTo("start server --name=name1 --J=-Dme=her --J=-Dfoo=bar ".length());
    assertThat(candidate.size()).isEqualTo(4);
    assertTrue(candidate.getCandidates().contains(new Completion("--locators")));
  }

  @Test
  public void testMultiJComplete() throws Exception {
    buffer = "start server --name=name1 --J=-Dtest=test1 --J=-Dfoo=bar";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.size()).isEqualTo(50);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testMultiJCompleteWithDifferentOrder() throws Exception {
    buffer = "start server --J=-Dtest=test1 --J=-Dfoo=bar --name=name1";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.size()).isEqualTo(50);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete3() throws Exception {
    buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.size()).isEqualTo(49);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete4() throws Exception {
    buffer = "start server --name=name1 --locators=localhost  --J=-Dfoo=bar --";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.size()).isEqualTo(49);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "assign-buckets");
  }

  @Test
  public void testCompletRegionType() throws Exception {
    buffer = "create region --name=test --type";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(23);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=LOCAL");
  }

  @Test
  public void testCompletPartialRegionType() throws Exception {
    buffer = "create region --name=test --type=LO";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "CAL");
  }

  @Test
  public void testCompletWithRegionTypeWithNoSpace() throws Exception {
    buffer = "create region --name=test --type=REPLICATE";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "_HEAP_LRU");
  }

  @Test
  public void testCompletWithRegionTypeWithSpace() throws Exception {
    buffer = "create region --name=test --type=REPLICATE ";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(38);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--async-event-queue-id");
  }

  @Test
  public void testCompletLogLevel() throws Exception {
    buffer = "change loglevel --loglevel";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=ALL");
  }

  @Test
  public void testCompletLogLevelWithEqualSign() throws Exception {
    buffer = "change loglevel --loglevel=";
    candidate = parser.complete(buffer);
    assertThat(candidate.size()).isEqualTo(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ALL");
  }

  @Test
  public void testObtainHelp() {
    String command = CliStrings.START_PULSE;
    String helpString = "NAME\n" + "start pulse\n" + "IS AVAILABLE\n" + "true\n" + "SYNOPSIS\n"
        + "Open a new window in the default Web browser with the URL for the Pulse application.\n"
        + "SYNTAX\n" + "start pulse [--url=value]\n" + "PARAMETERS\n" + "url\n"
        + "URL of the Pulse Web application.\n" + "Required: false\n"
        + "Default (if the parameter is not specified): http://localhost:7070/pulse\n";
    assertThat(parser.getCommandManager().obtainHelp(command)).isEqualTo(helpString);
  }

  private String getCompleted(String buffer, int cursor, Completion completed) {
    return buffer.substring(0, cursor) + completed.getValue();
  }



}
