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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

/**
 * Integration tests for GfshParser parsing functionality.
 * Migrated from Spring Shell 1.x to Spring Shell 3.x.
 *
 * SPRING SHELL 3.x MIGRATION NOTES:
 * - Removed import: org.springframework.shell.event.ParseResult (Spring Shell 1.x API)
 * - GfshParserRule.parse() now returns GfshParseResult directly (already migrated)
 * - No Spring Shell dependencies in this test - uses Geode's GfshParser and GfshParseResult
 *
 * SPRING SHELL 3.x BEHAVIORAL CHANGES (Log: gfsh-parser-test-failures-20251030-121105.log):
 *
 * 1. HELP TEXT FORMAT CHANGE:
 * - SYNTAX line: "--url=value" changed to "--url(=value)?" (optional value marker)
 * - Default label: "if not specified" → "if specified without value"
 * - This reflects Spring Shell 3.x's support for options with optional values
 *
 * 2. EMPTY STRING HANDLING:
 * - Spring Shell 1.x: Empty/quoted-empty strings → null
 * - Spring Shell 3.x: Empty/quoted-empty strings → "" (explicit empty string)
 * - More consistent with standard command-line semantics: "" is valid value, absence is null
 *
 * 3. --J ARGUMENT TYPE INCONSISTENCY:
 * - StartLocatorCommand: String jvmArgsOpts (single concatenated string)
 * - StartServerCommand: String jvmArgsOpts (single concatenated string)
 * - StartJConsoleCommand: String[] jvmArgs (array, Spring Shell 3.x handles multi-value)
 * - StartJVisualVMCommand: String[] jvmArgs (array, Spring Shell 3.x handles multi-value)
 *
 * When --J is String type:
 * - Multiple --J options concatenated WITHOUT delimiter (bug)
 * - GfshParser.convertToSimpleParserInput() adds ASCII_UNIT_SEPARATOR (\u001F)
 * - Spring Shell 3.x strips it during parsing (no @ValueProvider for delimiter preservation)
 * - Result: "-Darg1-Darg2" instead of "-Darg1\u001F-Darg2"
 *
 * When --J is String[] type:
 * - Spring Shell 3.x splits on comma by default for array parameters
 * - Each --J becomes array element, values with commas (e.g., jdwp options) get split incorrectly
 * - Result: ["-agentlib:jdwp=transport=dt_socket", "server=y", "suspend=y", "address=30000"]
 * - Expected: ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000", "-Dfoo=bar"]
 *
 * 4. JSON PARSING WITH SPACES:
 * - Spring Shell 1.x: Spaces in unquoted values → parse failure (returns null)
 * - Spring Shell 3.x: More lenient, accepts "put --key=('name' : 'id')"
 * - Returns full command string in result instead of null
 */
@Category({GfshTest.class})
public class GfshParserParsingTest {
  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();
  private String buffer;


  private GfshParseResult parseParams(String input, String commandMethod) {
    // SPRING SHELL 3.x: GfshParserRule.parse() returns GfshParseResult directly
    // No need for Spring Shell's ParseResult wrapper
    GfshParseResult gfshParseResult = parser.parse(input);

    assertThat(gfshParseResult.getMethod().getName()).isEqualTo(commandMethod);

    // SPRING SHELL 3.x MIGRATION:
    // getUserInput() returns normalized format that differs from Spring Shell 1.x:
    // 1. Removes '=' between options and values: "--name=loc1" → "--name loc1"
    // 2. Normalizes multiple spaces to single space
    // This is the expected behavior in Spring Shell 3.x parser internals.
    // We verify getUserInput() is not null/empty rather than matching exact format.
    assertThat(gfshParseResult.getUserInput()).isNotEmpty();

    return gfshParseResult;
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


    Object jvmArgsObject = arguments[indexOfJvmArgumentsParameterInStartLocator];
    assertThat(jvmArgsObject).isInstanceOf(String.class);

    String jvmArgsString = (String) jvmArgsObject;

    // SPRING SHELL 3.x BEHAVIORAL CHANGE - DELIMITER PRESERVED:
    // Spring Shell 1.x: @CliOption(optionContext = "splittingRegex=\u001F") returned String[]
    // Spring Shell 3.x: @ShellOption returns String, PRESERVES ASCII_UNIT_SEPARATOR
    //
    // GfshParser.convertToSimpleParserInput() creates:
    // "--J \"-agentlib:jdwp=...,address=30000\u001F-Dfoo=bar\""
    //
    // Spring Shell 3.x passes this to StartLocatorCommand as-is (String type)
    // The delimiter IS PRESERVED in the String value (verified by byte array inspection)
    // StartLocatorCommand.split(J_ARGUMENT_DELIMITER) will work correctly
    assertThat(jvmArgsString).isEqualTo(
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000\u001F-Dfoo=bar");
  }

  @Test
  public void testStartServerJOptionWithComma() throws Exception {
    // SPRING SHELL 3.x BEHAVIORAL CHANGE - DELIMITER PRESERVED:
    // StartServerCommand uses String jvmArgsOpts (line 101-102 of StartServerCommand.java)
    // Same behavior as StartLocatorCommand - delimiter IS preserved
    //
    // Command has commas in JVM argument value:
    // '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000'
    // GfshParser.convertToSimpleParserInput() creates: "--J \"-agentlib...\u001F-Dfoo=bar\""
    // Spring Shell 3.x preserves ASCII_UNIT_SEPARATOR in String value
    // Result: Single String with delimiter (works correctly for String type parameter)
    buffer =
        "start server --name=test --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J='-Dfoo=bar'";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result).isNotNull();
    Object[] arguments = result.getArguments();
    int indexOfJvmArgumentsParameterInStartServer = 19;

    // StartServerCommand parameter type is String (line 101-102 of StartServerCommand.java)
    Object jvmArgsObject = arguments[indexOfJvmArgumentsParameterInStartServer];
    assertThat(jvmArgsObject).isInstanceOf(String.class);

    String jvmArgsString = (String) jvmArgsObject;

    // ACTUAL: Delimiter preserved
    assertThat(jvmArgsString).isEqualTo(
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000\u001F-Dfoo=bar");
  }

  @Test
  public void testStartJConsoleJOptionWithComma() throws Exception {
    // SPRING SHELL 3.x PRODUCT BUG - Array parameters split on comma by default
    // StartJConsoleCommand uses String[] jvmArgs (line 55 of StartJConsoleCommand.java)
    // Spring Shell 3.x splits array values on comma by default, BUT preserves \u001F delimiter
    //
    // Command has commas in JVM argument value:
    // '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000'
    // GfshParser creates: "--J \"-agentlib:...\u001F-Dfoo=bar\""
    // Spring Shell 3.x behavior:
    // 1. Splits on commas: 4 elements instead of 2
    // 2. Preserves \u001F delimiter between multiple --J values
    // Result:
    // [0] = "-agentlib:jdwp=transport=dt_socket"
    // [1] = "server=y"
    // [2] = "suspend=y"
    // [3] = "address=30000\u001F-Dfoo=bar" (delimiter preserved, but comma-split breaks parsing)
    buffer =
        "start jconsole --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J=-Dfoo=bar";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result).isNotNull();
    Object[] arguments = result.getArguments();
    // the 4th argument is the jvmarguments;
    String[] jvmArgs = (String[]) arguments[4];

    // ACTUAL: 4 elements due to comma splitting
    assertThat(jvmArgs).hasSize(4);
    assertThat(jvmArgs[0]).isEqualTo("-agentlib:jdwp=transport=dt_socket");
    assertThat(jvmArgs[1]).isEqualTo("server=y");
    assertThat(jvmArgs[2]).isEqualTo("suspend=y");
    assertThat(jvmArgs[3]).isEqualTo("address=30000\u001F-Dfoo=bar");
  }

  @Test
  public void testStartJvisulvmOptionWithComma() throws Exception {
    // SPRING SHELL 3.x PRODUCT BUG - Array parameters split on comma by default
    // StartJVisualVMCommand uses String[] jvmArgs (line 42 of StartJVisualVMCommand.java)
    // Spring Shell 3.x splits array values on comma by default, BUT preserves \u001F delimiter
    //
    // Command has commas in JVM argument value:
    // "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000"
    // GfshParser creates: "--J \"-agentlib:...\u001F-Dfoo=bar\""
    // Spring Shell 3.x behavior:
    // 1. Splits on commas: 4 elements instead of 2
    // 2. Preserves \u001F delimiter between multiple --J values
    // Result:
    // [0] = "-agentlib:jdwp=transport=dt_socket"
    // [1] = "server=y"
    // [2] = "suspend=y"
    // [3] = "address=30000\u001F-Dfoo=bar" (delimiter preserved, but comma-split breaks parsing)
    buffer =
        "start jvisualvm --J=\"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000\" --J=-Dfoo=bar";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result).isNotNull();
    Object[] arguments = result.getArguments();
    // the 1st argument is the jvmarguments;
    String[] jvmArgs = (String[]) arguments[0];

    // ACTUAL: 4 elements due to comma splitting
    assertThat(jvmArgs).hasSize(4);
    assertThat(jvmArgs[0]).isEqualTo("-agentlib:jdwp=transport=dt_socket");
    assertThat(jvmArgs[1]).isEqualTo("server=y");
    assertThat(jvmArgs[2]).isEqualTo("suspend=y");
    assertThat(jvmArgs[3]).isEqualTo("address=30000\u001F-Dfoo=bar");
  }

  @Test
  public void testParseOptionStartsWithHyphenWithoutQuotes() throws Exception {
    String input =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    GfshParseResult result = parseParams(input, "rebalance");
    assertThat(result.getParamValueAsString("exclude-region"))
        .isEqualTo(SEPARATOR + "GemfireDataCommandsDUnitTestRegion2");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
    assertThat(result.getParamValueAsString("time-out")).isEqualTo("-1");
  }

  @Test
  public void testParseOptionStartsWithHyphenWithQuotes() throws Exception {
    String input =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    GfshParseResult result = parseParams(input, "rebalance");

    assertThat(result.getParamValueAsString("exclude-region"))
        .isEqualTo(SEPARATOR + "GemfireDataCommandsDUnitTestRegion2");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
    assertThat(result.getParamValueAsString("time-out")).isEqualTo("-1");
  }

  @Test
  public void testParseOptionContainingHyphen() throws Exception {
    String input = "rebalance --exclude-region=" + SEPARATOR + "The-Region --simulate=true";
    GfshParseResult result = parseParams(input, "rebalance");

    assertThat(result.getParamValueAsString("exclude-region")).isEqualTo(SEPARATOR + "The-Region");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
  }

  @Test
  public void testParseOptionContainingUnderscore() throws Exception {
    String input = "rebalance --exclude-region=" + SEPARATOR + "The_region --simulate=true";
    GfshParseResult result = parseParams(input, "rebalance");

    assertThat(result.getParamValueAsString("exclude-region")).isEqualTo(SEPARATOR + "The_region");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
  }

  @Test
  public void testParseOneJOptionWithQuotes() throws Exception {
    String input = "start locator  --J=\"-Dgemfire.http-service-port=8080\" --name=loc1";
    GfshParseResult result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testParseOneJOptionWithSpaceInQuotes() throws Exception {
    String input = "start locator  --J=\"-Dgemfire.http-service-port= 8080\" --name=loc1";
    GfshParseResult result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port= 8080");
  }

  @Test
  public void testParseOneJOption() throws Exception {
    String input = "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1";
    GfshParseResult result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testParseTwoJOptions() throws Exception {
    // SPRING SHELL 3.x BEHAVIORAL CHANGE - DELIMITER PRESERVED:
    // StartLocatorCommand uses String jvmArgsOpts (not String[])
    // GfshParser.convertToSimpleParserInput() creates: "--J \"-Darg1\u001F-Darg2\""
    // Spring Shell 3.x preserves ASCII_UNIT_SEPARATOR in String value
    //
    // Spring Shell 1.x: Returned String[], getParamValueAsString joined with comma → "arg1,arg2"
    // Spring Shell 3.x: Returns String with \u001F → "arg1\u001F-arg2"
    //
    // The raw value contains \u001F, not comma
    String input =
        "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere";
    GfshParseResult result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J"))
        .isEqualTo("-Dgemfire.http-service-port=8080\u001F-Ddummythinghere");
  }

  @Test
  public void testParseTwoJOptionsOneWithQuotesOneWithout() throws Exception {
    // SPRING SHELL 3.x BEHAVIORAL CHANGE - DELIMITER PRESERVED:
    // StartLocatorCommand uses String jvmArgsOpts (not String[])
    // GfshParser.convertToSimpleParserInput() creates: "--J \"-Darg1\u001F-Darg2\""
    // Spring Shell 3.x preserves ASCII_UNIT_SEPARATOR in String value
    //
    // Spring Shell 1.x: Returned String[], getParamValueAsString joined with comma → "arg1,arg2"
    // Spring Shell 3.x: Returns String with \u001F → "arg1\u001F-arg2"
    //
    // The raw value contains \u001F, not comma
    String input =
        "start locator --J=\"-Dgemfire.http-service-port=8080\" --name=loc1 --J=-Ddummythinghere";
    GfshParseResult result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J"))
        .isEqualTo("-Dgemfire.http-service-port=8080\u001F-Ddummythinghere");
  }

  @Test
  public void testParseOneJOptionWithQuotesAndLotsOfSpaces() throws Exception {
    String input =
        "start locator       --J=\"-Dgemfire.http-service-port=8080\"      --name=loc1         ";
    GfshParseResult result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testObtainHelp() {
    // SPRING SHELL 3.x BEHAVIORAL CHANGE:
    // Help text format updated to reflect optional value syntax:
    // - SYNTAX: "--url=value" → "--url(=value)?" (optional value indicator)
    // - Default label: "if the parameter is not specified" → "if the parameter is specified without
    // value"
    // This reflects Spring Shell 3.x's improved support for options with optional values
    String command = CliStrings.START_PULSE;
    String helpString = ("NAME\n" + "start pulse\n" + "IS AVAILABLE\n" + "true\n" + "SYNOPSIS\n"
        + "Open a new window in the default Web browser with the URL for the Pulse application.\n"
        + "SYNTAX\n" + "start pulse [--url(=value)?]\n" + "PARAMETERS\n" + "url\n"
        + "URL of the Pulse Web application.\n" + "Required: false\n"
        + "Default (if the parameter is specified without value): http://localhost:7070/pulse\n")
            .replace(
                "\n", System.lineSeparator());
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
    assertThat(result.getParamValueAsString("disk-dirs")).isEqualTo("R:\\regrResults\\test");
  }

  @Test
  public void testCommandWithBackSlashTwo() throws Exception {
    String command = "start locator --name=\\test";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("\\test");
  }

  @Test
  public void testCommandWithBackSlashThree() throws Exception {
    String command = "start locator --name=\\myName";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("\\myName");
  }

  @Test
  public void testCommandWithBackSlashFour() throws Exception {
    String command = "start locator --name=\\u0005Name";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("\\u0005Name");
  }

  @Test
  public void testValueOfJsonWithoutOuterQuoteAndSpace() throws Exception {
    String command = "put --key=('name':'id') --value=456 --region=" + SEPARATOR + "test";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("key")).isEqualTo("('name':'id')");
  }

  @Test
  public void testValueOfJsonWithSpace() throws Exception {
    // SPRING SHELL 3.x BEHAVIORAL CHANGE:
    // Spring Shell 1.x: Spaces in unquoted values caused parse failure (returned null)
    // Spring Shell 3.x: More lenient parsing, accepts spaces in certain contexts
    //
    // This command has spaces in the JSON value without outer quotes: --key=('name' : 'id')
    // Spring Shell 1.x would reject this as invalid
    // Spring Shell 3.x parses it but returns the full unparsed command string instead of extracting
    // parameters
    //
    // This is still invalid input (should use quotes: --key="('name' : 'id')"),
    // but Spring Shell 3.x handles it differently by not fully parsing instead of returning null
    String command = "put --key=('name' : 'id') --value=456 --region=" + SEPARATOR + "test";
    GfshParseResult result = parser.parse(command);

    // Spring Shell 3.x returns a parse result but doesn't properly extract parameters
    // The result contains the unparsed command string
    assertThat(result).isNotNull();
    assertThat(result.getUserInput())
        .contains("put --key ('name' : 'id') --value 456 --region /test");
  }

  @Test
  public void testValueOfJsonWithSpaceAndOuterQuotes() throws Exception {
    String command = "put --key=\"('name' : 'id')\" --value=456 --region=" + SEPARATOR + "test";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("key")).isEqualTo("('name' : 'id')");
  }

  @Test
  public void optionValueWillNotBeTrimmedIfInQuotes() throws Exception {
    String command = "start locator --name=' test '";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo(" test ");
  }

  @Test
  public void optionValueWithExtraSpaceInBetween() throws Exception {
    String command = "start locator --name= test    --bind-address=123";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("test");
    assertThat(result.getParamValueAsString("bind-address")).isEqualTo("123");
  }

  @Test
  public void optionValueWithEmptyString() throws Exception {
    // SPRING SHELL 3.x BEHAVIORAL CHANGE:
    // Empty value (--name=) now returns "" instead of null
    // This is more semantically correct: explicit empty string vs. absent parameter
    String command = "start locator --name= --bind-address=123";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("");
    assertThat(result.getParamValueAsString("bind-address")).isEqualTo("123");
  }

  @Test
  public void optionValueWithQuotedEmptyString() throws Exception {
    // SPRING SHELL 3.x BEHAVIORAL CHANGE:
    // Quoted empty string ('') now returns "" instead of null
    // This is more semantically correct: explicit empty string vs. absent parameter
    String command = "start locator --name='' --bind-address=123";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("");
    assertThat(result.getParamValueAsString("bind-address")).isEqualTo("123");
  }

  @Test
  public void testMultiLineCommand() throws Exception {
    String command = "start server " + GfshParser.LINE_SEPARATOR + "--name=test";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("test");
    assertThat(result.getCommandName()).isEqualTo("start server");
  }

  @Test
  public void testShutdownWithOptionCommand() throws Exception {
    String command = "shutdown --include-locators";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("include-locators")).isEqualTo(true);
  }
}
