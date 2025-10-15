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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;

/**
 * Provides completion candidates for enum parameters.
 * Returns all enum constants that match the partial input (case-insensitive).
 * Enum constants are returned in alphabetical order.
 *
 * Example:
 * For enum { APPLY, STAGE }, partial input "ap" returns ["APPLY"]
 *
 * @since Spring Shell 3.x completion implementation
 */
public class EnumCompletionProvider implements ValueCompletionProvider {

  @Override
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    if (!targetType.isEnum()) {
      return Collections.emptyList();
    }

    List<Completion> completions = new ArrayList<>();
    Object[] constants = targetType.getEnumConstants();

    if (constants == null) {
      return Collections.emptyList();
    }

    // Sort enum constants alphabetically for consistent completion order
    Arrays.sort(constants, (a, b) -> a.toString().compareTo(b.toString()));

    String lowerPartial = partialValue.toLowerCase();

    for (Object constant : constants) {
      String value = constant.toString();

      // Filter by partial input (case-insensitive)
      if (lowerPartial.isEmpty() || value.toLowerCase().startsWith(lowerPartial)) {
        completions.add(new Completion(value));
      }
    }

    return completions;
  }

  @Override
  public boolean supports(Class<?> targetType) {
    return targetType.isEnum();
  }
}
