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
 * Unit tests for {@link CompletionProviderRegistry}.
 *
 * @since Spring Shell 3.x completion implementation
 */
public class CompletionProviderRegistryTest {

  private enum TestEnum {
    OPTION_A, OPTION_B, OPTION_C
  }

  private CompletionProviderRegistry registry;

  @Before
  public void setUp() {
    registry = new CompletionProviderRegistry();
  }

  @Test
  public void constructor_shouldRegisterDefaultProviders() {
    // Registry should have enum and boolean providers by default
    assertThat(registry.getProviderCount()).isEqualTo(2);
  }

  @Test
  public void findProvider_shouldReturnEnumProvider_forEnumTypes() {
    ValueCompletionProvider provider = registry.findProvider(TestEnum.class);

    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(EnumCompletionProvider.class);
  }

  @Test
  public void findProvider_shouldReturnBooleanProvider_forBooleanClass() {
    ValueCompletionProvider provider = registry.findProvider(Boolean.class);

    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(BooleanCompletionProvider.class);
  }

  @Test
  public void findProvider_shouldReturnBooleanProvider_forPrimitiveBooleanType() {
    ValueCompletionProvider provider = registry.findProvider(boolean.class);

    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(BooleanCompletionProvider.class);
  }

  @Test
  public void findProvider_shouldReturnNull_whenNoProviderSupportsType() {
    ValueCompletionProvider provider = registry.findProvider(String.class);

    assertThat(provider).isNull();
  }

  @Test
  public void getCompletions_shouldReturnEnumCompletions_forEnumType() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--option", "");

    List<Completion> completions = registry.getCompletions(TestEnum.class, "", context);

    assertThat(completions).hasSize(3);
    assertThat(completions.stream().map(Completion::getValue))
        .containsExactlyInAnyOrder("OPTION_A", "OPTION_B", "OPTION_C");
  }

  @Test
  public void getCompletions_shouldReturnFilteredEnumCompletions_withPartialValue() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--option", "opt");

    List<Completion> completions = registry.getCompletions(TestEnum.class, "opt", context);

    assertThat(completions).hasSize(3);
    assertThat(completions.stream().map(Completion::getValue))
        .containsExactlyInAnyOrder("OPTION_A", "OPTION_B", "OPTION_C");
  }

  @Test
  public void getCompletions_shouldReturnBooleanCompletions_forBooleanType() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "");

    List<Completion> completions = registry.getCompletions(Boolean.class, "", context);

    assertThat(completions).hasSize(2);
    assertThat(completions.stream().map(Completion::getValue))
        .containsExactlyInAnyOrder("true", "false");
  }

  @Test
  public void getCompletions_shouldReturnFilteredBooleanCompletions_withPartialValue() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--enabled", "t");

    List<Completion> completions = registry.getCompletions(Boolean.class, "t", context);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).isEqualTo("true");
  }

  @Test
  public void getCompletions_shouldReturnEmptyList_whenNoProviderFound() {
    CompletionContext context = CompletionContext.optionValue("test-command", "--name", "test");

    List<Completion> completions = registry.getCompletions(String.class, "test", context);

    assertThat(completions).isEmpty();
  }

  @Test
  public void registerProvider_shouldAddProviderToRegistry() {
    int initialCount = registry.getProviderCount();

    // Create a simple test provider
    ValueCompletionProvider customProvider = new ValueCompletionProvider() {
      @Override
      public List<Completion> getCompletions(Class<?> targetType, String partialValue,
          CompletionContext context) {
        return List.of(new Completion("custom"));
      }

      @Override
      public boolean supports(Class<?> targetType) {
        return targetType == String.class;
      }
    };

    registry.registerProvider(customProvider);

    assertThat(registry.getProviderCount()).isEqualTo(initialCount + 1);
  }

  @Test
  public void findProvider_shouldReturnFirstMatchingProvider_whenMultipleSupport() {
    // Add a custom provider that also supports enums
    ValueCompletionProvider customEnumProvider = new ValueCompletionProvider() {
      @Override
      public List<Completion> getCompletions(Class<?> targetType, String partialValue,
          CompletionContext context) {
        return List.of(new Completion("custom-enum"));
      }

      @Override
      public boolean supports(Class<?> targetType) {
        return targetType.isEnum();
      }
    };

    registry.registerProvider(customEnumProvider);

    // Should return the first provider (EnumCompletionProvider registered in constructor)
    ValueCompletionProvider provider = registry.findProvider(TestEnum.class);

    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(EnumCompletionProvider.class);
  }
}
