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
 * Unit tests for {@link EnumCompletionProvider}.
 *
 * @since Spring Shell 3.x completion implementation
 */
public class EnumCompletionProviderTest {

  private enum TestEnum {
    APPLY, STAGE, VALIDATE, ABORT
  }

  private EnumCompletionProvider provider;

  @Before
  public void setUp() {
    provider = new EnumCompletionProvider();
  }

  @Test
  public void supports_shouldReturnTrueForEnumTypes() {
    assertThat(provider.supports(TestEnum.class)).isTrue();
  }

  @Test
  public void supports_shouldReturnFalseForNonEnumTypes() {
    assertThat(provider.supports(String.class)).isFalse();
    assertThat(provider.supports(Integer.class)).isFalse();
    assertThat(provider.supports(Boolean.class)).isFalse();
  }

  @Test
  public void getCompletions_shouldReturnAllEnumConstants_whenPartialValueIsEmpty() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--action", "");

    List<Completion> completions = provider.getCompletions(TestEnum.class, "", context);

    assertThat(completions).hasSize(4);
    assertThat(completions.stream().map(Completion::getValue))
        .containsExactlyInAnyOrder("APPLY", "STAGE", "VALIDATE", "ABORT");
  }

  @Test
  public void getCompletions_shouldFilterByPartialValue_caseInsensitive() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--action", "ap");

    List<Completion> completions = provider.getCompletions(TestEnum.class, "ap", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("APPLY");
  }

  @Test
  public void getCompletions_shouldFilterByPartialValue_lowercaseInput() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--action", "sta");

    List<Completion> completions = provider.getCompletions(TestEnum.class, "sta", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("STAGE");
  }

  @Test
  public void getCompletions_shouldFilterByPartialValue_uppercaseInput() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--action", "VA");

    List<Completion> completions = provider.getCompletions(TestEnum.class, "VA", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("VALIDATE");
  }

  @Test
  public void getCompletions_shouldReturnEmptyList_whenNoMatch() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--action", "xyz");

    List<Completion> completions = provider.getCompletions(TestEnum.class, "xyz", context);

    assertThat(completions).isEmpty();
  }

  @Test
  public void getCompletions_shouldReturnEmptyList_whenTypeIsNotEnum() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--name", "test");

    List<Completion> completions = provider.getCompletions(String.class, "test", context);

    assertThat(completions).isEmpty();
  }

  @Test
  public void getCompletions_shouldMatchMultipleConstants_whenPartialMatches() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--action", "a");

    List<Completion> completions = provider.getCompletions(TestEnum.class, "a", context);

    assertThat(completions).hasSize(2);
    assertThat(completions.stream().map(Completion::getValue))
        .containsExactlyInAnyOrder("APPLY", "ABORT");
  }
}
