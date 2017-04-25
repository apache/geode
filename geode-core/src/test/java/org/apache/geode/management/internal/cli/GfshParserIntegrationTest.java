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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.Completion;
import org.springframework.shell.event.ParseResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Category(IntegrationTest.class)
public class GfshParserIntegrationTest {
  private static GfshParser parser;
  private List<Completion> candidates;
  private String buffer;
  private int cursor;

  @BeforeClass
  public static void setUpClass() throws Exception {
    parser = new GfshParser();
  }

  @Before
  public void setUp() throws Exception {
    this.candidates = new ArrayList<>();
  }

  private Map<String, String> parseParams(String input, String commandMethod) {
    ParseResult parseResult = parser.parse(input);

    GfshParseResult gfshParseResult = (GfshParseResult) parseResult;
    Map<String, String> params = gfshParseResult.getParamValueStrings();
    for (String param : params.keySet()) {
      System.out.println(param + "=" + params.get(param));
    }

    assertThat(gfshParseResult.getMethod().getName()).isEqualTo(commandMethod);
    assertThat(gfshParseResult.getUserInput()).isEqualTo(input.trim());

    return params;
  }

  @Test
  public void getSimpleParserInputTest() {
    buffer = "start locator  --J=\"-Dgemfire.http-service-port=8080\" --name=loc1";
    assertEquals("start locator --J \"-Dgemfire.http-service-port=8080\" --name loc1",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere";
    assertEquals(
        "start locator --J \"-Dgemfire.http-service-port=8080,-Ddummythinghere\" --name loc1",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start locator --";
    assertThat(GfshParser.convertToSimpleParserInput(buffer)).isEqualTo("start locator --");

    buffer =
        "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere --";
    assertEquals(
        "start locator --J \"-Dgemfire.http-service-port=8080,-Ddummythinghere\" --name loc1 --",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    assertEquals("start server --name name1 --locators localhost --J \"-Dfoo=bar\"",
        GfshParser.convertToSimpleParserInput(buffer));
  }

  @Test
  public void testParseOptionStartsWithHyphenWithoutQuotes() throws Exception {
    String input =
        "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    Map<String, String> params = parseParams(input, "rebalance");
    assertThat(params.get("exclude-region")).isEqualTo("/GemfireDataCommandsDUnitTestRegion2");
    assertThat(params.get("simulate")).isEqualTo("true");
    assertThat(params.get("time-out")).isEqualTo("-1");
  }

  @Test
  public void testParseOptionStartsWithHyphenWithQuotes() throws Exception {
    String input =
        "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    Map<String, String> params = parseParams(input, "rebalance");

    assertThat(params.get("exclude-region")).isEqualTo("/GemfireDataCommandsDUnitTestRegion2");
    assertThat(params.get("simulate")).isEqualTo("true");
    assertThat(params.get("time-out")).isEqualTo("-1");
  }

  @Test
  public void testParseOptionContainingHyphen() throws Exception {
    String input = "rebalance --exclude-region=/The-Region --simulate=true";
    Map<String, String> params = parseParams(input, "rebalance");

    assertThat(params.get("exclude-region")).isEqualTo("/The-Region");
    assertThat(params.get("simulate")).isEqualTo("true");
  }

  @Test
  public void testParseOptionContainingUnderscore() throws Exception {
    String input = "rebalance --exclude-region=/The_region --simulate=true";
    Map<String, String> params = parseParams(input, "rebalance");

    assertThat(params.get("exclude-region")).isEqualTo("/The_region");
    assertThat(params.get("simulate")).isEqualTo("true");
  }

  @Test
  public void testParseOneJOptionWithQuotes() throws Exception {
    String input = "start locator  --J=\"-Dgemfire.http-service-port=8080\" --name=loc1";
    Map<String, String> params = parseParams(input, "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testParseOneJOptionWithSpaceInQuotes() throws Exception {
    String input = "start locator  --J=\"-Dgemfire.http-service-port= 8080\" --name=loc1";
    Map<String, String> params = parseParams(input, "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("'-Dgemfire.http-service-port= 8080'");
  }

  @Test
  public void testParseOneJOption() throws Exception {
    String input = "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1";
    Map<String, String> params = parseParams(input, "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testParseTwoJOptions() throws Exception {
    String input =
        "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere";
    Map<String, String> params = parseParams(input, "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("-Dgemfire.http-service-port=8080,-Ddummythinghere");
  }

  @Test
  public void testParseTwoJOptionsOneWithQuotesOneWithout() throws Exception {
    String input =
        "start locator --J=\"-Dgemfire.http-service-port=8080\" --name=loc1 --J=-Ddummythinghere";
    Map<String, String> params = parseParams(input, "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("-Dgemfire.http-service-port=8080,-Ddummythinghere");
  }

  @Test
  public void testParseOneJOptionWithQuotesAndLotsOfSpaces() throws Exception {
    String input =
        "start locator       --J=\"-Dgemfire.http-service-port=8080\"      --name=loc1         ";
    Map<String, String> params = parseParams(input, "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testCompletionDescibe() throws Exception {
    buffer = "describe";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(7);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDescibeWithSpace() throws Exception {
    buffer = "describe ";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(7);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDeploy() throws Exception {
    buffer = "deploy";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(3);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + " --dir");
  }

  @Test
  public void testCompletionDeployWithSpace() throws Exception {
    buffer = "deploy ";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(3);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "--dir");
  }

  @Test
  public void testCompleteWithRequiredOption() throws Exception {
    buffer = "describe config";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(1);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + " --member");
  }

  @Test
  public void testCompleteWithRequiredOptionWithSpace() throws Exception {
    buffer = "describe config ";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(1);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "--member");
  }

  @Test
  public void testCompleteCommand() throws Exception {
    buffer = "start ser";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(1);
    assertThat("start server").isEqualTo(getCompleted(buffer, cursor, candidates.get(0)));
  }

  @Test
  public void testCompleteOptionWithOnlyOneCandidate() throws Exception {
    buffer = "start server --nam";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(1);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "e");
  }

  @Test
  public void testCompleteOptionWithMultipleCandidates() throws Exception {
    buffer = "start server --name=jinmei --loc";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(3);
    assertThat(getCompleted(buffer, cursor, candidates.get(0)))
        .isEqualTo(buffer + "ator-wait-time");
    assertThat(getCompleted(buffer, cursor, candidates.get(1))).isEqualTo(buffer + "ators");
    assertThat(getCompleted(buffer, cursor, candidates.get(2))).isEqualTo(buffer + "k-memory");
  }

  @Test
  public void testCompleteWithExtraSpace() throws Exception {
    buffer = "start server --name=name1  --se";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo("start server --name=name1  ".length());
    assertThat(candidates.size()).isEqualTo(3);
    assertTrue(candidates.contains(new Completion("--server-port")));
    assertThat(getCompleted(buffer, cursor, candidates.get(0)))
        .isEqualTo(buffer + "curity-properties-file");
  }

  @Test
  public void testCompleteWithDashInTheEnd() throws Exception {
    buffer = "start server --name=name1 --";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length() - 2);
    assertThat(candidates.size()).isEqualTo(50);
    assertTrue(candidates.contains(new Completion("--properties-file")));
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "J");
  }

  @Test
  public void testCompleteWithSpace() throws Exception {
    buffer = "start server --name=name1 ";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length() - 1);
    assertThat(candidates.size()).isEqualTo(50);
    assertTrue(candidates.contains(new Completion(" --properties-file")));
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "--J");
  }

  @Test
  public void testCompleteWithOutSpace() throws Exception {
    buffer = "start server --name=name1";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length());
    assertThat(candidates.size()).isEqualTo(50);
    assertTrue(candidates.contains(new Completion(" --properties-file")));
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + " --J");
  }

  @Test
  public void testCompleteJ() throws Exception {
    buffer = "start server --name=name1 --J=";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length());
    assertThat(candidates.size()).isEqualTo(0);
  }

  @Test
  public void testCompleteWithValue() throws Exception {
    buffer = "start server --name=name1 --J";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length());
    assertThat(candidates.size()).isEqualTo(0);
  }

  @Test
  public void testCompleteWithDash() throws Exception {
    buffer = "start server --name=name1 --J=-Dfoo.bar --";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(49);
  }

  @Test
  public void testCompleteWithMultipleJ() throws Exception {
    buffer = "start server --name=name1 --J=-Dme=her --J=-Dfoo=bar --l";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo("start server --name=name1 --J=-Dme=her --J=-Dfoo=bar ".length());
    assertThat(candidates.size()).isEqualTo(4);
    assertTrue(candidates.contains(new Completion("--locators")));
  }

  @Test
  public void testMultiJComplete() throws Exception {
    buffer = "start server --name=name1 --J=-Dtest=test1 --J=-Dfoo=bar";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length());
    assertThat(candidates.size()).isEqualTo(49);
    assertThat(getCompleted(buffer, cursor, candidates.get(0)))
        .isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testMultiJCompleteWithDifferentOrder() throws Exception {
    buffer = "start server --J=-Dtest=test1 --J=-Dfoo=bar --name=name1";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length());
    assertThat(candidates.size()).isEqualTo(49);
    assertThat(getCompleted(buffer, cursor, candidates.get(0)))
        .isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete3() throws Exception {
    buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length());
    assertThat(candidates.size()).isEqualTo(48);
    assertThat(getCompleted(buffer, cursor, candidates.get(0)))
        .isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete4() throws Exception {
    buffer = "start server --name=name1 --locators=localhost  --J=-Dfoo=bar --";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(cursor).isEqualTo(buffer.length() - 2);
    assertThat(candidates.size()).isEqualTo(48);
    assertThat(getCompleted(buffer, cursor, candidates.get(0)))
        .isEqualTo(buffer + "assign-buckets");
  }

  @Test
  public void testCompletRegionType() throws Exception {
    buffer = "create region --name=test --type";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(23);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "=LOCAL");
  }

  @Test
  public void testCompletPartialRegionType() throws Exception {
    buffer = "create region --name=test --type=LO";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(5);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "CAL");
  }

  @Test
  public void testCompletHelp() throws Exception {
    buffer = "create region --name=test --type LO";
    cursor = parser.completeSuperAdvanced(buffer, candidates);
    System.out.println("");
  }

  @Test
  public void testCompletLogLevel() throws Exception {
    buffer = "change loglevel --loglevel";
    cursor = parser.completeAdvanced(buffer, candidates);
    assertThat(candidates.size()).isEqualTo(8);
    assertThat(getCompleted(buffer, cursor, candidates.get(0))).isEqualTo(buffer + "=ALL");
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


  @Test
  public void testStringArrayConverter() {
    String command = "create disk-store --name=foo --dir=bar";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("dir")).isEqualTo("bar");
  }

  @Test
  public void testDirConverter() {
    String command = "compact offline-disk-store --name=foo --disk-dirs=bar";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("disk-dirs")).isEqualTo("bar");
  }

  @Test
  public void testMultiDirInvalid() throws Exception {
    String command = "create disk-store --name=testCreateDiskStore1 --group=Group1 "
        + "--allow-force-compaction=true --auto-compact=false --compaction-threshold=67 "
        + "--max-oplog-size=355 --queue-size=5321 --time-interval=2023 --write-buffer-size=3110 "
        + "--dir=/testCreateDiskStore1.1#1452637463 " + "--dir=/testCreateDiskStore1.2";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNull();
  }

  @Test
  public void testMultiDirValid() throws Exception {
    String command = "create disk-store --name=testCreateDiskStore1 --group=Group1 "
        + "--allow-force-compaction=true --auto-compact=false --compaction-threshold=67 "
        + "--max-oplog-size=355 --queue-size=5321 --time-interval=2023 --write-buffer-size=3110 "
        + "--dir=/testCreateDiskStore1.1#1452637463,/testCreateDiskStore1.2";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("dir"))
        .isEqualTo("/testCreateDiskStore1.1#1452637463,/testCreateDiskStore1.2");
  }

  @Test
  public void testEmptyKey() throws Exception {
    String command = "remove  --key=\"\" --region=/GemfireDataCommandsTestRegion";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("key")).isEqualTo("");
  }

  @Test
  public void testJsonKey() throws Exception {
    String command = "get --key=('id':'testKey0') --region=regionA";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
  }

  @Test
  public void testUnspecifiedValueToStringArray() {
    String command = "change loglevel --loglevel=finer --groups=group1,group2";
    ParseResult result = parser.parse(command);
    String[] memberIdValue = (String[]) result.getArguments()[0];
    assertThat(memberIdValue).isNull();
  }

  private String getCompleted(String buffer, int cursor, Completion completed) {
    return buffer.substring(0, cursor) + completed.getValue();
  }

}
