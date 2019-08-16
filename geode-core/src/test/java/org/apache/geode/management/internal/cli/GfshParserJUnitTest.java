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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;


/**
 * GfshParserJUnitTest - Includes tests to check the parsing and auto-completion capabilities of
 * {@link GfshParser}
 */
public class GfshParserJUnitTest {

  private String input;
  private List<String> tokens;

  @Before
  public void before() {
    tokens = new ArrayList<>();
  }

  @Test
  public void testSplitUserInputDoubleQuotes() {
    input = "query --query=\"select * from /region\"";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(3);
    assertThat(tokens.get(0)).isEqualTo("query");
    assertThat(tokens.get(1)).isEqualTo("--query");
    assertThat(tokens.get(2)).isEqualTo("\"select * from /region\"");
  }

  @Test
  public void testSplitUserInputSingleQuotes() {
    input = "query --query='select * from /region'";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(3);
    assertThat(tokens.get(0)).isEqualTo("query");
    assertThat(tokens.get(1)).isEqualTo("--query");
    assertThat(tokens.get(2)).isEqualTo("'select * from /region'");
  }

  @Test
  public void testSplitUserInputWithMixedQuotes() {
    input = "command option='test1 \"test \" test1'";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(3);
    assertThat(tokens.get(0)).isEqualTo("command");
    assertThat(tokens.get(1)).isEqualTo("option");
    assertThat(tokens.get(2)).isEqualTo("'test1 \"test \" test1'");
  }


  @Test
  public void testSplitUserInputWithJ() {
    input =
        "start server --name=server1  --J=\"-Dgemfire.start-dev-rest-api=true\" --J='-Dgemfire.http-service-port=8080' --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000'";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(10);
    assertThat(tokens.get(5)).isEqualTo("\"-Dgemfire.start-dev-rest-api=true\"");
    assertThat(tokens.get(7)).isEqualTo("'-Dgemfire.http-service-port=8080'");
    assertThat(tokens.get(9))
        .isEqualTo("'-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000'");
  }

  @Test
  public void splitWithWhiteSpacesExceptQuoted() {
    input = "create region --cache-writer=\"my.abc{'k1' : 'v   1', 'k2' : 'v2'}\"";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(4);
    assertThat(tokens.get(3)).isEqualTo("\"my.abc{'k1' : 'v   1', 'k2' : 'v2'}\"");
  }

  @Test
  public void testSplitUserInputWithJNoQuotes() {
    input =
        "start server --name=server1  --J=-Dgemfire.start-dev-rest-api=true --J=-Dgemfire.http-service-port=8080";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(8);
    assertThat(tokens.get(5)).isEqualTo("-Dgemfire.start-dev-rest-api=true");
    assertThat(tokens.get(7)).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testSplitJsonValue() throws Exception {
    input = "get --key=('id':'testKey0') --region=regionA";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(5);
    assertThat(tokens.get(2)).isEqualTo("('id':'testKey0')");
  }

  @Test
  public void testGetSimpleParserInput() throws Exception {
    String[] strings = {"command", "--option1", "value1", "--option2", "'test value'"};
    Arrays.stream(strings).forEach(tokens::add);
    assertThat(GfshParser.getSimpleParserInputFromTokens(tokens))
        .isEqualTo("command --option1 value1 --option2 'test value'");
  }

  @Test
  public void testGetSimpleParserInputWithJ() throws Exception {
    String[] strings =
        {"command", "--J", "-Dkey=value", "--option", "'test value'", "--J", "-Dkey2=value2"};
    Arrays.stream(strings).forEach(tokens::add);
    assertThat(GfshParser.getSimpleParserInputFromTokens(tokens))
        .isEqualTo("command --J \"-Dkey=value" + GfshParser.J_ARGUMENT_DELIMITER
            + "-Dkey2=value2\" --option 'test value'");
  }

  @Test
  public void testGetSimpleParserInputWithJWithSingleQuotes() throws Exception {
    String[] strings = {"command", "--J", "'-Dkey=value value'"};
    Arrays.stream(strings).forEach(tokens::add);
    assertThat(GfshParser.getSimpleParserInputFromTokens(tokens))
        .isEqualTo("command --J \"-Dkey=value value\"");
  }

  @Test
  public void testGetSimpleParserInputWithJWithDoubleQuotes() throws Exception {
    String[] strings = {"command", "--J", "\"-Dkey=value value\""};
    Arrays.stream(strings).forEach(tokens::add);
    assertThat(GfshParser.getSimpleParserInputFromTokens(tokens))
        .isEqualTo("command --J \"-Dkey=value value\"");
  }

  @Test
  public void testGetSimpleParserInputWithJAtTheEnd() throws Exception {
    String[] strings =
        {"command", "--option", "'test value'", "--J", "-Dkey=value", "--J", "-Dkey2=value2"};
    Arrays.stream(strings).forEach(tokens::add);
    assertThat(GfshParser.getSimpleParserInputFromTokens(tokens))
        .isEqualTo("command --option 'test value' --J \"-Dkey=value"
            + GfshParser.J_ARGUMENT_DELIMITER + "-Dkey2=value2\"");
  }

  @Test
  public void spaceOrEmptyStringIsParsedCorrectly() {
    input = "alter region --name=/Person --cache-writer='' --cache-loader=' '";
    tokens = GfshParser.splitUserInput(input);
    assertThat(tokens.size()).isEqualTo(8);
    assertThat(tokens.get(7)).isEqualTo("' '");
    assertThat(tokens.get(5)).isEqualTo("''");
  }

  @Test
  public void testShorthandReplacement() {
    assertThat(GfshParser.convertToSimpleParserInput("command --option=~/foo/bar"))
      .isEqualTo("command --option " + System.getProperty("user.home") + "/foo/bar");
  }
}
