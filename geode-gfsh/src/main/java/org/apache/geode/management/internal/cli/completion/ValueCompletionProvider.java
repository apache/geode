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

import java.util.List;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;

/**
 * Strategy interface for providing completion candidates for command option values.
 * Implementations provide completions for specific parameter types (enums, booleans, files, etc.).
 *
 * @since Spring Shell 3.x completion implementation
 */
public interface ValueCompletionProvider {

  /**
   * Get completion candidates for a parameter value.
   *
   * @param targetType The parameter type (e.g., OrderPolicy.class, Boolean.class)
   * @param partialValue The partial value typed by the user (may be empty)
   * @param context The completion context with command and option information
   * @return List of completion candidates, empty if no completions available
   */
  List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context);

  /**
   * Check if this provider supports the given target type.
   * Default implementation returns true - providers should override if they only support
   * specific types.
   *
   * @param targetType The parameter type to check
   * @return true if this provider can provide completions for this type
   */
  default boolean supports(Class<?> targetType) {
    return true;
  }
}
