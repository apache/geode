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
package org.apache.geode.management.internal.cli.converters;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import org.apache.geode.cache.query.IndexType;

/**
 * Converter for Spring Shell 3.x to convert string values to IndexType enum.
 *
 * <p>
 * Spring Shell 3.x requires explicit converters for custom types, including enums.
 * This converter handles string-to-IndexType conversion with synonym support:
 * <ul>
 * <li>"range" → FUNCTIONAL</li>
 * <li>"hash" → HASH</li>
 * <li>"key" → PRIMARY_KEY</li>
 * <li>Case-insensitive enum names (FUNCTIONAL, HASH, PRIMARY_KEY)</li>
 * </ul>
 *
 * <p>
 * Used by CreateIndexCommand and DefineIndexCommand for the --type parameter.
 *
 * @since GemFire 1.0
 */
@Component
@SuppressWarnings("deprecation")
public class IndexTypeConverter implements Converter<String, IndexType> {

  /**
   * Converts a string value to an IndexType enum.
   *
   * @param source the string to convert (e.g., "range", "hash", "key", "FUNCTIONAL")
   * @return the corresponding IndexType enum value
   * @throws IllegalArgumentException if the string is not a valid IndexType or synonym
   */
  @Override
  public IndexType convert(@NonNull String source) {
    // IndexType.valueOfSynonym handles:
    // - Synonyms: "range" -> FUNCTIONAL, "hash" -> HASH, "key" -> PRIMARY_KEY
    // - Case-insensitive enum names: "FUNCTIONAL", "functional", etc.
    // - Throws IllegalArgumentException for invalid values
    return IndexType.valueOfSynonym(source);
  }
}
