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
import java.util.List;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;

/**
 * Provides completion candidates for boolean parameters.
 * Returns "true" and "false" filtered by partial input.
 *
 * Example:
 * Partial input "t" returns ["true"]
 * Partial input "f" returns ["false"]
 * Partial input "" returns ["true", "false"]
 *
 * @since Spring Shell 3.x completion implementation
 */
public class BooleanCompletionProvider implements ValueCompletionProvider {

  private static final String[] BOOLEAN_VALUES = {"true", "false"};

  @Override
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    List<Completion> completions = new ArrayList<>();
    String lowerPartial = partialValue.toLowerCase();

    for (String value : BOOLEAN_VALUES) {
      if (lowerPartial.isEmpty() || value.startsWith(lowerPartial)) {
        completions.add(new Completion(value));
      }
    }

    return completions;
  }

  @Override
  public boolean supports(Class<?> targetType) {
    return targetType == Boolean.class || targetType == boolean.class;
  }
}
