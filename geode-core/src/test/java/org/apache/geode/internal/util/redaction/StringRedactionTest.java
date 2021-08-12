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
package org.apache.geode.internal.util.redaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class StringRedactionTest {

  private static final String REDACTED = "redacted";

  private SensitiveDataDictionary sensitiveDataDictionary;
  private RedactionStrategy redactionStrategy;

  private StringRedaction stringRedaction;

  @Before
  public void setUp() {
    sensitiveDataDictionary = mock(SensitiveDataDictionary.class);
    redactionStrategy = mock(RedactionStrategy.class);

    stringRedaction =
        new StringRedaction(REDACTED, sensitiveDataDictionary, redactionStrategy);
  }

  @Test
  public void redactDelegatesString() {
    String input = "line";
    String expected = "expected";

    when(redactionStrategy.redact(input)).thenReturn(expected);

    String result = stringRedaction.redact(input);

    verify(redactionStrategy).redact(input);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void redactDelegatesNullString() {
    String input = null;

    stringRedaction.redact(input);

    verify(redactionStrategy).redact(input);
  }

  @Test
  public void redactDelegatesEmptyString() {
    String input = "";

    stringRedaction.redact(input);

    verify(redactionStrategy).redact(input);
  }

  @Test
  public void redactDelegatesIterable() {
    String line1 = "line1";
    String line2 = "line2";
    String line3 = "line3";
    Collection<String> input = new ArrayList<>();
    input.add(line1);
    input.add(line2);
    input.add(line3);
    String joinedLine = String.join(" ", input);
    String expected = "expected";

    when(redactionStrategy.redact(joinedLine)).thenReturn(expected);

    String result = stringRedaction.redact(input);

    verify(redactionStrategy).redact(joinedLine);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void redactNullIterableThrowsNullPointerException() {
    Collection<String> input = null;

    Throwable thrown = catchThrowable(() -> {
      stringRedaction.redact(input);
    });

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesSensitiveKey() {
    String key = "key";
    String value = "value";

    when(sensitiveDataDictionary.isSensitive(key)).thenReturn(true);

    String result = stringRedaction.redactArgumentIfNecessary(key, value);

    verify(sensitiveDataDictionary).isSensitive(key);
    assertThat(result).isEqualTo(REDACTED);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesNonSensitiveKey() {
    String key = "key";
    String value = "value";

    when(sensitiveDataDictionary.isSensitive(key)).thenReturn(false);

    String result = stringRedaction.redactArgumentIfNecessary(key, value);

    verify(sensitiveDataDictionary).isSensitive(key);
    assertThat(result).isEqualTo(value);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesNullKey() {
    String key = null;

    stringRedaction.redactArgumentIfNecessary(key, "value");

    verify(sensitiveDataDictionary).isSensitive(key);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesEmptyKey() {
    String key = "";

    stringRedaction.redactArgumentIfNecessary(key, "value");

    verify(sensitiveDataDictionary).isSensitive(key);
  }

  @Test
  public void redactArgumentIfNecessaryReturnsNullValue() {
    String value = null;

    String result = stringRedaction.redactArgumentIfNecessary("key", value);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void redactArgumentIfNecessaryReturnsEmptyValue() {
    String value = "";

    String result = stringRedaction.redactArgumentIfNecessary("key", value);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void redactEachInListDelegatesEachStringInIterable() {
    String string1 = "string1";
    String string2 = "string2";
    String string3 = "string3";
    List<String> input = new ArrayList<>();
    input.add(string1);
    input.add(string2);
    input.add(string3);

    when(redactionStrategy.redact(anyString())).then(returnsFirstArg());

    List<String> result = stringRedaction.redactEachInList(input);

    verify(redactionStrategy).redact(string1);
    verify(redactionStrategy).redact(string2);
    verify(redactionStrategy).redact(string3);
    assertThat(result).isEqualTo(input);
  }

  @Test
  public void redactEachInListDoesNotDelegateEmptyIterable() {
    List<String> input = Collections.emptyList();

    when(redactionStrategy.redact(anyString())).then(returnsFirstArg());

    List<String> result = stringRedaction.redactEachInList(input);

    verifyZeroInteractions(redactionStrategy);
    assertThat(result).isEqualTo(input);
  }

  @Test
  public void redactEachInListNullIterableThrowsNullPointerException() {
    List<String> input = null;

    when(redactionStrategy.redact(anyString())).then(returnsFirstArg());

    Throwable thrown = catchThrowable(() -> {
      stringRedaction.redactEachInList(input);
    });

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void isSensitiveDelegatesString() {
    String input = "input";

    when(sensitiveDataDictionary.isSensitive(anyString())).thenReturn(true);

    boolean result = stringRedaction.isSensitive(input);

    assertThat(result).isTrue();
  }

  @Test
  public void isSensitiveDelegatesNullString() {
    String input = null;

    when(sensitiveDataDictionary.isSensitive(isNull())).thenReturn(true);

    boolean result = stringRedaction.isSensitive(input);

    assertThat(result).isTrue();
  }

  @Test
  public void isSensitiveDelegatesEmptyString() {
    String input = "";

    when(sensitiveDataDictionary.isSensitive(anyString())).thenReturn(true);

    boolean result = stringRedaction.isSensitive(input);

    assertThat(result).isTrue();
  }
}
