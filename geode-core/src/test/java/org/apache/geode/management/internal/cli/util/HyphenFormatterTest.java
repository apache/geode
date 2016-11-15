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
package org.apache.geode.management.internal.cli.util;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class HyphenFormatterTest {

  private HyphenFormatter formatter;

  @Before
  public void setUp() {
    this.formatter = new HyphenFormatter();
  }

  @Test
  public void containsOptionWithOneOptionReturnsTrue() {
    String cmd = "start locator --name=loc1";
    assertTrue(this.formatter.containsOption(cmd));
  }

  @Test
  public void containsOptionWithNoOptionReturnsFalse() {
    String cmd = "start locator";
    assertFalse(this.formatter.containsOption(cmd));
  }

  @Test
  public void containsOptionWithMultipleOptionsReturnsTrue() {
    String cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=-Dbar=foo";
    assertTrue(this.formatter.containsOption(cmd));
  }

  @Test
  public void valueWithoutQuotesReturnsWithQuotes() {
    String cmd = "start locator --name=loc1 --J=-Dfoo=bar";
    String formattedCmd = this.formatter.formatCommand(cmd);

    String expected = "start locator --name=loc1 --J=\"-Dfoo=bar\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithoutQuotesReturnsWithQuotes_2() {
    String cmd = "start locator --J=-Dfoo=bar --name=loc1";
    String formattedCmd = this.formatter.formatCommand(cmd);

    String expected = "start locator --J=\"-Dfoo=bar\" --name=loc1";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithHyphenWithoutQuotesFails() {
    String cmd =
        "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    String formattedCmd = this.formatter.formatCommand(cmd);

    String expected =
        "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithHyphenWithoutQuotes() {
    String cmd =
        "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    String formattedCmd = this.formatter.formatCommand(cmd);

    String expected =
        "rebalance --exclude-region=/GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void nullShouldThrowNullPointerException() {
    assertThatThrownBy(() -> this.formatter.formatCommand(null))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void emptyShouldThrowNullPointerException() {
    assertThat(this.formatter.formatCommand("")).isEqualTo("");
  }

  @Test
  public void multipleJOptions() {
    String cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=-Dbar=foo";
    String formattedCmd = this.formatter.formatCommand(cmd);

    String expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dbar=foo\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void multipleJOptionsWithSomethingAfter() {
    String cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=-Dbar=foo --group=locators";
    String formattedCmd = this.formatter.formatCommand(cmd);

    String expected =
        "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dbar=foo\" --group=locators";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void multipleJOptionsWithSomethingBetween() {
    String cmd = "start locator --name=loc1 --J=-Dfoo=bar --group=locators --J=-Dbar=foo";
    String formattedCmd = this.formatter.formatCommand(cmd);

    String expected =
        "start locator --name=loc1 --J=\"-Dfoo=bar\" --group=locators --J=\"-Dbar=foo\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithQuotes() {
    String cmd = "start locator --name=loc1 --J=\"-Dfoo=bar\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    assertThat(formattedCmd).isEqualTo(cmd);
  }

  @Test
  public void oneValueWithQuotesOneWithout() {
    String cmd = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=-Dfoo=bar";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dfoo=bar\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void oneValueWithoutQuotesOneWith() {
    String cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=\"-Dfoo=bar\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dfoo=bar\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void twoValuesWithQuotes() {
    String cmd = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dfoo=bar\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    assertThat(formattedCmd).as(cmd).isEqualTo(cmd);
  }

  @Test
  public void valueContainingQuotes() {
    String cmd = "start locator --name=loc1 --J=\"-Dfoo=region\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected = "start locator --name=loc1 --J=\"-Dfoo=region\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingQuotesAndSpace() {
    String cmd = "start locator --name=loc1 --J=\"-Dfoo=my phrase\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected = "start locator --name=loc1 --J=\"-Dfoo=my phrase\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingQuotesAndMultipleSpaces() {
    String cmd = "start locator --name=loc1 --J=\"-Dfoo=this is a phrase\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected = "start locator --name=loc1 --J=\"-Dfoo=this is a phrase\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingMultipleJWithSpaces() {
    String cmd =
        "start locator --name=loc1 --J=-Dfoo=this is a phrase             --J=\"-Dfoo=a short sentence\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected =
        "start locator --name=loc1 --J=\"-Dfoo=this is a phrase\" --J=\"-Dfoo=a short sentence\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingMultipleJWithSpaces2() {
    String cmd =
        "start locator --name=loc1 --J=\"-Dfoo=this is a phrase            \" --J=\"-Dfoo=a short sentence\"";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected =
        "start locator --name=loc1 --J=\"-Dfoo=this is a phrase            \" --J=\"-Dfoo=a short sentence\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionAfterOneJOption() {
    String cmd = "start locator --name=loc1 --J=-Dfoo=bar --http-service=8080";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --http-service=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionWithMoreThanOneHyphen() {
    String cmd = "start locator --name=loc1 --http-service-port=8080";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected = "start locator --name=loc1 --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionWithOneHyphenAfterOneJOption() {
    String cmd = "start server --name=me3 --J=-Dgemfire.jmx-manager=true --redis-port=8080";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --redis-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test // reproduces GEODE-2104
  public void optionWithMoreThanOneHyphenAfterOneJOption() {
    String cmd = "start server --name=me3 --J=-Dgemfire.jmx-manager=true --http-service-port=8080";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionWithOneHyphenAfterTwoJOptions() {
    String cmd =
        "start server --name=me3 --J=-Dgemfire.jmx-manager=true --J=-Dgemfire.jmx-manager-start=true --redis-port=8080";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --J=\"-Dgemfire.jmx-manager-start=true\" --redis-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test // reproduces GEODE-2104
  public void optionWithMoreThanOneHyphenAfterTwoJOptions() {
    String cmd =
        "start server --name=me3 --J=-Dgemfire.jmx-manager=true --J=-Dgemfire.jmx-manager-start=true --http-service-port=8080";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --J=\"-Dgemfire.jmx-manager-start=true\" --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test // reproduces GEODE-2075
  public void optionWithMoreThanOneHyphenWithoutValueAfterJOptions() {
    String cmd =
        "start server --name=Server2 --log-level=config --J=-Dgemfire.locators=localhost[10334] --disable-default-server";
    String formattedCmd = this.formatter.formatCommand(cmd);
    String expected =
        "start server --name=Server2 --log-level=config --J=\"-Dgemfire.locators=localhost[10334]\" --disable-default-server";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }



}
