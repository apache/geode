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

import org.apache.geode.cache.query.IndexType;
import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;

/**
 * CompletionProvider for IndexType parameters that returns lowercase synonym values
 * in alphabetical order.
 *
 * REASONING: Spring Shell 1.x IndexTypeConverter returned completions as lowercase
 * synonyms in order: "range", "key", "hash". Tests expect alphabetical ordering:
 * "hash", "key", "range". This provider returns the IndexType synonym values
 * (getName()) in lowercase and alphabetically sorted to match test expectations.
 *
 * The synonyms are:
 * - "hash" (HASH)
 * - "key" (PRIMARY_KEY)
 * - "range" (FUNCTIONAL)
 *
 * @since GemFire 1.0
 */
@SuppressWarnings("deprecation")
public class IndexTypeCompletionProvider implements ValueCompletionProvider {

  @Override
  public boolean supports(Class<?> targetType) {
    // Check if the parameter type is IndexType enum
    return targetType != null && targetType.equals(IndexType.class);
  }

  @Override
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    List<Completion> completions = new ArrayList<>();

    // REASONING: IndexType enum has getName() method that returns synonym values:
    // - FUNCTIONAL.getName() = "RANGE"
    // - HASH.getName() = "HASH"
    // - PRIMARY_KEY.getName() = "KEY"
    //
    // Old Spring Shell 1.x returned these as lowercase. Tests expect alphabetical
    // order, so we return: "hash", "key", "range"

    String lowerPartial = partialValue.toLowerCase();

    // Collect synonym values in alphabetical order
    List<String> synonyms = new ArrayList<>();
    for (IndexType indexType : IndexType.values()) {
      String synonym = indexType.getName().toLowerCase();
      if (lowerPartial.isEmpty() || synonym.startsWith(lowerPartial)) {
        synonyms.add(synonym);
      }
    }

    // Sort alphabetically (hash, key, range)
    synonyms.sort(String::compareTo);

    // Add to completions
    for (String synonym : synonyms) {
      completions.add(new Completion(synonym));
    }

    return completions;
  }
}
