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
    registerProvider(new EnumCompletionProvider());
    registerProvider(new BooleanCompletionProvider());

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
   * @param targetType the parameter type (e.g., OrderPolicy.class)
   * @param partialValue the partial value typed by user
   * @param context the completion context
   * @return list of completion candidates, empty if no provider found
   */
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    ValueCompletionProvider provider = findProvider(targetType);

    if (provider == null) {
      return Collections.emptyList();
    }

    return provider.getCompletions(targetType, partialValue, context);
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
