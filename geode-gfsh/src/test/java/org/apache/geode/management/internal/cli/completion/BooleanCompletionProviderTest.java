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
package org.apache.geode.management.internal.cli.completion;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;

/**
 * Unit tests for {@link BooleanCompletionProvider}.
 *
 * @since Spring Shell 3.x completion implementation
 */
public class BooleanCompletionProviderTest {

  private BooleanCompletionProvider provider;

  @Before
  public void setUp() {
    provider = new BooleanCompletionProvider();
  }

  @Test
  public void supports_shouldReturnTrueForBooleanClass() {
    assertThat(provider.supports(Boolean.class)).isTrue();
  }

  @Test
  public void supports_shouldReturnTrueForPrimitiveBooleanType() {
    assertThat(provider.supports(boolean.class)).isTrue();
  }

  @Test
  public void supports_shouldReturnFalseForNonBooleanTypes() {
    assertThat(provider.supports(String.class)).isFalse();
    assertThat(provider.supports(Integer.class)).isFalse();
    assertThat(provider.supports(int.class)).isFalse();
  }

  @Test
  public void getCompletions_shouldReturnBothValues_whenPartialValueIsEmpty() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "");

    List<Completion> completions = provider.getCompletions(Boolean.class, "", context);

    assertThat(completions).hasSize(2);
    assertThat(completions.stream().map(Completion::getValue))
        .containsExactlyInAnyOrder("true", "false");
  }

  @Test
  public void getCompletions_shouldReturnTrueOnly_whenPartialValueStartsWithT() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "t");

    List<Completion> completions = provider.getCompletions(Boolean.class, "t", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("true");
  }

  @Test
  public void getCompletions_shouldReturnTrueOnly_whenPartialValueIsTru() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "tru");

    List<Completion> completions = provider.getCompletions(Boolean.class, "tru", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("true");
  }

  @Test
  public void getCompletions_shouldReturnFalseOnly_whenPartialValueStartsWithF() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "f");

    List<Completion> completions = provider.getCompletions(Boolean.class, "f", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("false");
  }

  @Test
  public void getCompletions_shouldReturnFalseOnly_whenPartialValueIsFal() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "fal");

    List<Completion> completions = provider.getCompletions(Boolean.class, "fal", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("false");
  }

  @Test
  public void getCompletions_shouldBeCaseInsensitive_uppercase() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "T");

    List<Completion> completions = provider.getCompletions(Boolean.class, "T", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("true");
  }

  @Test
  public void getCompletions_shouldBeCaseInsensitive_mixedCase() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "FaL");

    List<Completion> completions = provider.getCompletions(Boolean.class, "FaL", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("false");
  }

  @Test
  public void getCompletions_shouldReturnEmptyList_whenNoMatch() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "xyz");

    List<Completion> completions = provider.getCompletions(Boolean.class, "xyz", context);

    assertThat(completions).isEmpty();
  }

  @Test
  public void getCompletions_shouldWorkForPrimitiveBooleanType() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "t");

    List<Completion> completions = provider.getCompletions(boolean.class, "t", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("true");
  }
}
