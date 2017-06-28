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

import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.GfshParserRule;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class GfshParserParsingTest {
  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();
  private String buffer;


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
    assertEquals("start locator --J \"-Dgemfire.http-service-port=8080"
        + GfshParser.J_ARGUMENT_DELIMITER + "-Ddummythinghere\" --name loc1",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start locator --";
    assertThat(GfshParser.convertToSimpleParserInput(buffer)).isEqualTo("start locator --");

    buffer =
        "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere --";
    assertEquals("start locator --J \"-Dgemfire.http-service-port=8080"
        + GfshParser.J_ARGUMENT_DELIMITER + "-Ddummythinghere\" --name loc1 --",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    assertEquals("start server --name name1 --locators localhost --J \"-Dfoo=bar\"",
        GfshParser.convertToSimpleParserInput(buffer));
  }

  @Test
  public void testStartLocatorJOptionWithComma() throws Exception {
    buffer =
        "start locator --name=test --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J=-Dfoo=bar";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result).isNotNull();
    Object[] arguments = result.getArguments();
    int indexOfJvmArgumentsParameterInStartLocator = 18;

    String[] jvmArgs = (String[]) arguments[indexOfJvmArgumentsParameterInStartLocator];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
  }

  @Test
  public void testStartServerJOptionWithComma() throws Exception {
    buffer =
        "start server --name=test --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J='-Dfoo=bar'";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result).isNotNull();
    Object[] arguments = result.getArguments();
    int indexOfJvmArgumentsParameterInStartServer = 19;
    String[] jvmArgs = (String[]) arguments[indexOfJvmArgumentsParameterInStartServer];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
  }

  @Test
  public void testStartJConsoleJOptionWithComma() throws Exception {
    buffer =
        "start jconsole --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J=-Dfoo=bar";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result).isNotNull();
    Object[] arguments = result.getArguments();
    // the 4th argument is the jvmarguments;
    String[] jvmArgs = (String[]) arguments[4];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
  }

  @Test
  public void testStartJvisulvmOptionWithComma() throws Exception {
    buffer =
        "start jvisualvm --J=\"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000\" --J=-Dfoo=bar";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result).isNotNull();
    Object[] arguments = result.getArguments();
    // the 1st argument is the jvmarguments;
    String[] jvmArgs = (String[]) arguments[0];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
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
  public void testDeployCommand() throws Exception {
    String command = "deploy --jar=/tmp/junit7552412945092669041/jar1.jar";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
  }


  @Test
  public void testCommandWithBackSlash() throws Exception {
    String command =
        "describe offline-disk-store --name=testDiskStore --disk-dirs=R:\\regrResults\\test";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("disk-dirs")).isEqualTo("R:\\regrResults\\test");
  }

  @Test
  public void testCommandWithBackSlashTwo() throws Exception {
    String command = "start locator --name=\\test";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("name")).isEqualTo("\\test");
  }

  @Test
  public void testCommandWithBackSlashThree() throws Exception {
    String command = "start locator --name=\\myName";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("name")).isEqualTo("\\myName");
  }

  @Test
  public void testCommandWithBackSlashFour() throws Exception {
    String command = "start locator --name=\\u0005Name";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("name")).isEqualTo("\\u0005Name");
  }

  @Test
  public void testAlterRegion() throws Exception {
    String command =
        "alter region --name=/Person --cache-writer='' --cache-listener='' --cache-loader=''";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("cache-writer")).isNotNull().isEmpty();
    assertThat(result.getParamValue("cache-listener")).isNotNull().isEmpty();
    assertThat(result.getParamValue("cache-loader")).isNotNull().isEmpty();
  }
}
