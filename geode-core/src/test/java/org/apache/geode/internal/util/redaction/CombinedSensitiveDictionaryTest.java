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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class CombinedSensitiveDictionaryTest {

  @Test
  public void isFalseWhenZeroDelegates() {
    CombinedSensitiveDictionary combined = new CombinedSensitiveDictionary();

    boolean result = combined.isSensitive("string");

    assertThat(result).isFalse();
  }

  @Test
  public void delegatesInputToSingleDictionary() {
    String input = "string";
    SensitiveDataDictionary dictionary = mock(SensitiveDataDictionary.class);
    CombinedSensitiveDictionary combined = new CombinedSensitiveDictionary(dictionary);

    combined.isSensitive(input);

    verify(dictionary).isSensitive(same(input));
  }

  @Test
  public void delegatesInputToTwoDictionaries() {
    String input = "string";
    SensitiveDataDictionary dictionary1 = mock(SensitiveDataDictionary.class);
    SensitiveDataDictionary dictionary2 = mock(SensitiveDataDictionary.class);
    CombinedSensitiveDictionary combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2);

    combined.isSensitive(input);

    verify(dictionary1).isSensitive(same(input));
    verify(dictionary2).isSensitive(same(input));
  }

  @Test
  public void delegatesInputToManyDictionaries() {
    String input = "string";
    SensitiveDataDictionary dictionary1 = mock(SensitiveDataDictionary.class);
    SensitiveDataDictionary dictionary2 = mock(SensitiveDataDictionary.class);
    SensitiveDataDictionary dictionary3 = mock(SensitiveDataDictionary.class);
    SensitiveDataDictionary dictionary4 = mock(SensitiveDataDictionary.class);
    CombinedSensitiveDictionary combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    combined.isSensitive(input);

    verify(dictionary1).isSensitive(same(input));
    verify(dictionary2).isSensitive(same(input));
    verify(dictionary3).isSensitive(same(input));
    verify(dictionary4).isSensitive(same(input));
  }

  @Test
  public void isFalseWhenManyDictionariesAreFalse() {
    String input = "string";
    SensitiveDataDictionary dictionary1 = createDictionary(false);
    SensitiveDataDictionary dictionary2 = createDictionary(false);
    SensitiveDataDictionary dictionary3 = createDictionary(false);
    SensitiveDataDictionary dictionary4 = createDictionary(false);
    CombinedSensitiveDictionary combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    boolean result = combined.isSensitive(input);

    assertThat(result).isFalse();
  }

  @Test
  public void isTrueWhenManyDictionariesAreTrue() {
    String input = "string";
    SensitiveDataDictionary dictionary1 = createDictionary(true);
    SensitiveDataDictionary dictionary2 = createDictionary(true);
    SensitiveDataDictionary dictionary3 = createDictionary(true);
    SensitiveDataDictionary dictionary4 = createDictionary(true);
    CombinedSensitiveDictionary combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    boolean result = combined.isSensitive(input);

    assertThat(result).isTrue();
  }

  @Test
  public void isTrueWhenOneOfManyDictionariesIsTrue() {
    String input = "string";
    SensitiveDataDictionary dictionary1 = createDictionary(false);
    SensitiveDataDictionary dictionary2 = createDictionary(false);
    SensitiveDataDictionary dictionary3 = createDictionary(false);
    SensitiveDataDictionary dictionary4 = createDictionary(true);
    CombinedSensitiveDictionary combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    boolean result = combined.isSensitive(input);

    assertThat(result).isTrue();
  }

  private SensitiveDataDictionary createDictionary(boolean isSensitive) {
    SensitiveDataDictionary dictionary = mock(SensitiveDataDictionary.class);
    when(dictionary.isSensitive(anyString())).thenReturn(isSensitive);
    return dictionary;
  }
}
