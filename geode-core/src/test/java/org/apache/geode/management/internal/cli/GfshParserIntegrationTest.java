/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class GfshParserIntegrationTest {

  private CommandManager commandManager;
  private GfshParser parser;

  @Before
  public void setUp() throws Exception {
    CommandManager.clearInstance();
    this.commandManager = CommandManager.getInstance(true);
    this.parser = new GfshParser(commandManager);
  }

  @After
  public void tearDown() {
    CommandManager.clearInstance();
  }

  private Map<String, String> params(String input, String commandName, String commandMethod) {
    ParseResult parseResult = parser.parse(input);
    GfshParseResult gfshParseResult = (GfshParseResult) parseResult;
    Map<String, String> params = gfshParseResult.getParamValueStrings();
    for (String param : params.keySet()) {
      System.out.println(param + "=" + params.get(param));
    }

    assertThat(gfshParseResult.getMethod().getName()).isEqualTo(commandMethod);
    assertThat(gfshParseResult.getUserInput()).isEqualTo(input.trim());
    assertThat(gfshParseResult.getCommandName()).isEqualTo(commandName);

    return params;
  }

  @Test
  public void optionStartsWithHyphenWithoutQuotes() throws Exception {
    String input = "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    Map<String, String> params = params(input, "rebalance", "rebalance");

    assertThat(params.get("exclude-region")).isEqualTo("/GemfireDataCommandsDUnitTestRegion2");
    assertThat(params.get("simulate")).isEqualTo("true");
    assertThat(params.get("time-out")).isEqualTo("\"-1\"");
  }

  @Test
  public void optionStartsWithHyphenWithQuotes() throws Exception {
    String input = "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    Map<String, String> params = params(input, "rebalance", "rebalance");

    assertThat(params.get("exclude-region")).isEqualTo("/GemfireDataCommandsDUnitTestRegion2");
    assertThat(params.get("simulate")).isEqualTo("true");
    assertThat(params.get("time-out")).isEqualTo("\"-1\"");
  }

  @Test
  public void optionContainingHyphen() throws Exception {
    String input = "rebalance --exclude-region=/The-Region --simulate=true";
    Map<String, String> params = params(input, "rebalance", "rebalance");

    assertThat(params.get("exclude-region")).isEqualTo("/The-Region");
    assertThat(params.get("simulate")).isEqualTo("true");
  }

  @Test
  public void optionContainingUnderscore() throws Exception {
    String input = "rebalance --exclude-region=/The_region --simulate=true";
    Map<String, String> params = params(input, "rebalance", "rebalance");

    assertThat(params.get("exclude-region")).isEqualTo("/The_region");
    assertThat(params.get("simulate")).isEqualTo("true");
  }

  @Test
  public void oneJOptionWithQuotes() throws Exception {
    String input = "start locator  --J=\"-Dgemfire.http-service-port=8080\" --name=loc1";
    Map<String, String> params = params(input, "start locator", "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("\"-Dgemfire.http-service-port=8080\"");
  }

  @Test
  public void oneJOptionWithSpaceInQuotes() throws Exception {
    String input = "start locator  --J=\"-Dgemfire.http-service-port= 8080\" --name=loc1";
    Map<String, String> params = params(input, "start locator", "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("\"-Dgemfire.http-service-port= 8080\"");
  }

  @Test
  public void oneJOption() throws Exception {
    String input = "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1";
    Map<String, String> params = params(input, "start locator", "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("\"-Dgemfire.http-service-port=8080\"");
  }

  @Test
  public void twoJOptions() throws Exception {
    String input = "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere";
    Map<String, String> params = params(input, "start locator", "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("\"-Dgemfire.http-service-port=8080\",\"-Ddummythinghere\"");
  }

  @Test
  public void twoJOptionsOneWithQuotesOneWithout() throws Exception {
    String input = "start locator --J=\"-Dgemfire.http-service-port=8080\" --name=loc1 --J=-Ddummythinghere";
    Map<String, String> params = params(input, "start locator", "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("\"-Dgemfire.http-service-port=8080\",\"-Ddummythinghere\"");
  }

  @Test
  public void oneJOptionWithQuotesAndLotsOfSpaces() throws Exception {
    String input = "start locator       --J=\"-Dgemfire.http-service-port=8080\"      --name=loc1         ";
    Map<String, String> params = params(input, "start locator", "startLocator");

    assertThat(params.get("name")).isEqualTo("loc1");
    assertThat(params.get("J")).isEqualTo("\"-Dgemfire.http-service-port=8080\"");
  }

}
