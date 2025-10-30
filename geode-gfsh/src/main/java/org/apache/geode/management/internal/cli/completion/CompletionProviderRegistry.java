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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;

/**
 * Central registry for value completion providers.
 * Dispatches completion requests to the appropriate provider based on parameter type.
 *
 * Registry maintains a chain of responsibility:
 * 1. Check type-specific providers (enum, boolean)
 * 2. Check custom providers registered for specific types
 * 3. Return empty list if no provider found
 *
 * Thread-safe for read operations (providers registered in constructor).
 *
 * @since Spring Shell 3.x completion implementation
 */
public class CompletionProviderRegistry {

  private final List<ValueCompletionProvider> providers;

  /**
   * Creates a new registry with default providers for common types.
   */
  public CompletionProviderRegistry() {
    this.providers = new ArrayList<>();

    // Register default providers (order matters - first match wins)

    // REASONING: IndexTypeCompletionProvider must be registered BEFORE EnumCompletionProvider
    // to provide IndexType-specific completion behavior. Returns lowercase synonyms
    // ("hash", "key", "range") in alphabetical order instead of uppercase enum names
    // (FUNCTIONAL, HASH, PRIMARY_KEY), matching Shell 1.x behavior. Fixes testIndexType.
    registerProvider(new IndexTypeCompletionProvider());

    registerProvider(new EnumCompletionProvider());
    registerProvider(new BooleanCompletionProvider());

    // ROOT CAUSE #10: HintTopicCompletionProvider provides topic name completions for the
    // "hint" command's topic parameter. In Spring Shell 2.x, HintTopicConverter handled this,
    // but it was never migrated during the Shell 3.x migration. Fixes 4 hint tests:
    // testCompleteHintNada, testCompleteHintSpace, testCompleteHintPartial,
    // testCompleteHintAlreadyComplete
    // MUST be registered BEFORE LogLevelCompletionProvider to get first priority for String params!
    registerProvider(new HintTopicCompletionProvider());

    // ROOT CAUSE #12: HelpCommandCompletionProvider provides command name completions for the
    // "help" command's command parameter. Similar to HintTopicCompletionProvider, this was
    // never migrated from Spring Shell 2.x HelpConverter. Fixes 2 help tests:
    // testCompleteHelpFirstWord, testCompleteHelpPartialFirstWord
    registerProvider(new HelpCommandCompletionProvider());

    // REASONING: LogLevelCompletionProvider provides log level completions for String parameters //
    // REASONING: LogLevelCompletionProvider provides log level completions for String parameters
    // whose name contains "loglevel". This maintains Shell 1.x backward compatibility where
    // the "change loglevel --loglevel" command offered 8 log levels: ALL, TRACE, DEBUG, INFO,
    // WARN, ERROR, FATAL, OFF. Fixes testCompleteLogLevel and testCompleteLogLevelWithEqualSign.
    registerProvider(new LogLevelCompletionProvider());

    // Future: Add more providers here
    // registerProvider(new FilePathCompletionProvider());
    // registerProvider(new MemberNameCompletionProvider());
    // registerProvider(new RegionNameCompletionProvider());
  }

  /**
   * Registers a completion provider in the chain.
   *
   * @param provider the provider to register
   */
  public void registerProvider(ValueCompletionProvider provider) {
    providers.add(provider);
  }

  /**
   * Finds the appropriate provider for the given target type.
   *
   * @param targetType the parameter type to find a provider for
   * @return the first provider that supports the type, or null if none found
   */
  public ValueCompletionProvider findProvider(Class<?> targetType) {
    for (ValueCompletionProvider provider : providers) {
      if (provider.supports(targetType)) {
        return provider;
      }
    }
    return null;
  }

  /**
   * Gets completion candidates for a parameter value.
   *
   * ROOT CAUSE #11: When multiple providers support the same type (e.g., String),
   * we need to try them all and return the first non-empty result. This implements
   * a "chain of responsibility" pattern with fallback.
   *
   * REASONING: HintTopicCompletionProvider and LogLevelCompletionProvider both support
   * String.class, but only work for specific parameters (topic vs loglevel). The first
   * provider might return empty, so we need to try subsequent providers until we find
   * one that returns completions.
   *
   * @param targetType the parameter type (e.g., OrderPolicy.class)
   * @param partialValue the partial value typed by user
   * @param context the completion context
   * @return list of completion candidates, empty if no provider found
   */
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    // ROOT CAUSE #11 FIX: Try ALL providers that support this type, return first non-empty result
    // This allows multiple String providers to coexist (HintTopicCompletionProvider,
    // LogLevelCompletionProvider)
    for (ValueCompletionProvider provider : providers) {
      if (provider.supports(targetType)) {
        List<Completion> completions = provider.getCompletions(targetType, partialValue, context);

        if (completions != null && !completions.isEmpty()) {
          return completions;
        }
      }
    }

    return Collections.emptyList();
  }

  /**
   * Returns the number of registered providers.
   * Used primarily for testing and diagnostics.
   *
   * @return the number of providers
   */
  public int getProviderCount() {
    return providers.size();
  }
}
