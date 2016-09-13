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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Categories.ExcludeCategory;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class CommandRequestTest {

  private Map<String, String> paramValues;
  private GfshParseResult mockParseResult;
  private Map<String, String> mockEnvironment;
  private CommandRequest commandRequest;

  @Before
  public void setUp() {
    this.paramValues = new HashMap<>();

    this.mockParseResult = mock(GfshParseResult.class);
    when(this.mockParseResult.getUserInput()).thenReturn("rebalance --simulate=true --time-out=-1");
    when(this.mockParseResult.getParamValueStrings()).thenReturn(this.paramValues);

    this.mockEnvironment = new HashMap<>();
    this.commandRequest = new CommandRequest(this.mockParseResult, this.mockEnvironment, null);
  }

  @Test
  public void getParametersRemovesQuotesAroundNegativeNumbers() {
    String key = "time-out";
    String value = "\"-1\"";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value.substring(1, value.length()-1));
  }

  @Test
  public void getParametersWithNullValue() {
    String key = "key-with-null-value";
    String value = null;
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isNull();
  }

  @Test
  public void getParametersWithEmptyValue() {
    String key = "key-with-empty-value";
    String value = "";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEmpty();
  }

  @Test
  public void getParametersWithEmptyQuotesValue() {
    String key = "key-with-empty-quotes-value";
    String value = "\"\"";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value);
  }

  @Test
  public void getParametersWithNumberValue() {
    String key = "key-with-number-value";
    String value = "1";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value);
  }

  @Test
  public void getParametersWithNegativeNumberValue() {
    String key = "key-with-negative-number-value";
    String value = "-1";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value);
  }

  @Test
  public void getParametersWithHyphenAlphaValue() {
    String key = "key-with-hyphen-alpha-value";
    String value = "-A";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value);
  }

  @Test
  public void getParametersWithHyphenHyphenNumberValue() {
    String key = "key-with-hyphen-alpha-value";
    String value = "--1";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value);
  }

  @Test
  public void getParametersWithQuotesAndMoreValue() {
    String key = "key-with-hyphen-alpha-value";
    String value = "\"-1 this is giberish\"";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value);
  }

  @Test
  public void getParametersWithLotsaQuotesValue() {
    String key = "key-with-hyphen-alpha-value";
    String value = "\"\"-1\"\"";
    this.paramValues.put(key, value);

    Map<String, String> parameters = this.commandRequest.getParameters();
    assertThat(parameters).containsKey(key);
    assertThat(parameters.get(key)).isEqualTo(value);
  }
}
