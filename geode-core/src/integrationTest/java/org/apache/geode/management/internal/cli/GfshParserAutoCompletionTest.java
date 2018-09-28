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

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.Completion;

import org.apache.geode.cache.query.IndexType;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(GfshTest.class)
public class GfshParserAutoCompletionTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private String buffer;
  private GfshParserRule.CommandCandidate candidate;

  @Test
  public void testCompletionDescribe() {
    buffer = "describe";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDescribeWithSpace() {
    buffer = "describe ";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDeploy() {
    buffer = "deploy";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --dir");
  }

  @Test
  public void testCompletionDeployWithSpace() {
    buffer = "deploy ";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--dir");
  }

  @Test
  public void testCompleteWithRequiredOption() {
    buffer = "describe config";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --member");
  }

  @Test
  public void testCompleteWithRequiredOptionWithSpace() {
    buffer = "describe config ";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--member");
  }

  @Test
  public void testCompleteCommand() {
    buffer = "start ser";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat("start server").isEqualTo(candidate.getFirstCandidate());
  }

  @Test
  public void testCompleteOptionWithOnlyOneCandidate() {
    buffer = "start server --nam";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "e");
  }

  @Test
  public void testCompleteOptionWithMultipleCandidates() {
    buffer = "start server --name=jinmei --loc";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ator-wait-time");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "ators");
    assertThat(candidate.getCandidate(2)).isEqualTo(buffer + "k-memory");
  }

  @Test
  public void testCompleteWithExtraSpace() {
    buffer = "start server --name=name1  --se";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo("start server --name=name1  ".length());
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getCandidates()).contains(new Completion("--server-port"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "curity-properties-file");
  }

  @Test
  public void testCompleteWithDashInTheEnd() {
    buffer = "start server --name=name1 --";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.getCandidates()).hasSize(53);
    assertThat(candidate.getCandidates()).contains(new Completion("--properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "J");
  }

  @Test
  public void testCompleteWithSpace() {
    buffer = "start server --name=name1 ";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 1);
    assertThat(candidate.getCandidates()).hasSize(53);
    assertThat(candidate.getCandidates()).contains(new Completion(" --properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--J");
  }

  @Test
  public void testCompleteWithOutSpace() {
    buffer = "start server --name=name1";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(53);
    assertThat(candidate.getCandidates()).contains(new Completion(" --properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --J");
  }

  @Test
  public void testCompleteJ() {
    buffer = "start server --name=name1 --J=";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.getCandidates()).hasSize(1);
  }

  @Test
  public void testCompleteWithValue() {
    buffer = "start server --name=name1 --J";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.getCandidates()).hasSize(1);
  }

  @Test
  public void testCompleteWithDash() {
    buffer = "start server --name=name1 --J=-Dfoo.bar --";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(52);
  }

  @Test
  public void testCompleteWithMultipleJ() {
    buffer = "start server --name=name1 --J=-Dme=her --J=-Dfoo=bar --l";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor())
        .isEqualTo("start server --name=name1 --J=-Dme=her --J=-Dfoo=bar ".length());
    assertThat(candidate.getCandidates()).hasSize(4);
    assertThat(candidate.getCandidates()).contains(new Completion("--locators"));
  }

  @Test
  public void testMultiJComplete() {
    buffer = "start server --name=name1 --J=-Dtest=test1 --J=-Dfoo=bar";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(52);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testMultiJCompleteWithDifferentOrder() {
    buffer = "start server --J=-Dtest=test1 --J=-Dfoo=bar --name=name1";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(52);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete3() {
    buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(51);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete4() {
    buffer = "start server --name=name1 --locators=localhost  --J=-Dfoo=bar --";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.getCandidates()).hasSize(51);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "assign-buckets");
  }

  @Test
  public void testCompleteRegionType() {
    buffer = "create region --name=test --type";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(23);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=LOCAL");
  }

  @Test
  public void testCompletePartialRegionType() {
    buffer = "create region --name=test --type=LO";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "CAL");
  }

  @Test
  public void testCompleteWithRegionTypeWithNoSpace() {
    buffer = "create region --name=test --type=REPLICATE";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "_HEAP_LRU");
  }

  @Test
  public void testCompleteWithRegionTypeWithSpace() {
    buffer = "create region --name=test --type=REPLICATE ";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(45);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--async-event-queue-id");
  }

  @Test
  public void testCompleteLogLevel() {
    buffer = "change loglevel --loglevel";
    candidate = parser.complete(buffer);
    assertThat(removeExtendedLevels(candidate.getCandidates())).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=ALL");
  }

  @Test
  public void testCompleteLogLevelWithEqualSign() {
    buffer = "change loglevel --loglevel=";
    candidate = parser.complete(buffer);
    assertThat(removeExtendedLevels(candidate.getCandidates())).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ALL");
  }

  @Test
  public void testObtainHelp() {
    String command = CliStrings.START_PULSE;
    String helpString = "NAME" + LINE_SEPARATOR + "start pulse" + LINE_SEPARATOR + "IS AVAILABLE"
        + LINE_SEPARATOR + "true" + LINE_SEPARATOR + "SYNOPSIS" + LINE_SEPARATOR
        + "Open a new window in the default Web browser with the URL for the Pulse application."
        + LINE_SEPARATOR
        + "SYNTAX" + LINE_SEPARATOR + "start pulse [--url=value]" + LINE_SEPARATOR + "PARAMETERS"
        + LINE_SEPARATOR + "url" + LINE_SEPARATOR
        + "URL of the Pulse Web application." + LINE_SEPARATOR + "Required: false" + LINE_SEPARATOR
        + "Default (if the parameter is not specified): http://localhost:7070/pulse"
        + LINE_SEPARATOR;
    assertThat(parser.getCommandManager().obtainHelp(command)).isEqualTo(helpString);
  }

  @Test
  public void testIndexType() {
    buffer = "create index --type=";
    candidate = parser.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(IndexType.values().length);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "hash");
  }

  /**
   * The log4j-core:tests jar contains {@code ExtendedLevels} which adds 2 levels when that jar
   * is on the classpath for integrationTest target.
   */
  private List<Completion> removeExtendedLevels(List<Completion> candidates) {
    Collection<Completion> toRemove = new HashSet<>();
    for (Completion completion : candidates) {
      if (completion.getValue().contains("DETAIL") || completion.getValue().contains("NOTE")) {
        toRemove.add(completion);
      }
    }
    candidates.removeAll(toRemove);
    return candidates;
  }
}
