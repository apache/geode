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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link IndexTypeConverter}.
 *
 * SPRING SHELL 3.x MIGRATION:
 * - Spring Shell 1.x API: Converter.convertFromText(value, targetType, optionContext)
 * - Spring Shell 3.x API: Converter.convert(source) - implements Spring's Converter<S, T>
 * - Spring Shell 1.x API: Converter.supports(type, optionContext) - no longer needed
 * - Spring Shell 3.x: Type safety enforced by generics (Converter<String, IndexType>)
 *
 * Removed:
 * - supports() method tests - not part of Spring Shell 3.x Converter interface
 * - EnumConverter tests - Spring Shell 3.x has built-in enum conversion
 * - ConverterHint.INDEX_TYPE context - not used in Spring Shell 3.x
 *
 * Migration Notes:
 * - IndexTypeConverter now implements org.springframework.core.convert.converter.Converter<String,
 * IndexType>
 * - Conversion logic unchanged: uses IndexType.valueOfSynonym() for synonym support
 * - Test focus: verify convert() method handles synonyms and invalid inputs correctly
 */
public class IndexTypeConverterTest {

  IndexTypeConverter typeConverter;

  @Before
  public void before() {
    typeConverter = new IndexTypeConverter();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void convert() {
    // SPRING SHELL 3.x: convert() method takes only source string
    // Type information encoded in generic Converter<String, IndexType>
    assertThat(typeConverter.convert("hash"))
        .isEqualTo(org.apache.geode.cache.query.IndexType.HASH);
    assertThat(typeConverter.convert("range"))
        .isEqualTo(org.apache.geode.cache.query.IndexType.FUNCTIONAL);
    assertThat(typeConverter.convert("key"))
        .isEqualTo(org.apache.geode.cache.query.IndexType.PRIMARY_KEY);

    // Test case-insensitive enum names
    assertThat(typeConverter.convert("HASH"))
        .isEqualTo(org.apache.geode.cache.query.IndexType.HASH);
    assertThat(typeConverter.convert("FUNCTIONAL"))
        .isEqualTo(org.apache.geode.cache.query.IndexType.FUNCTIONAL);
    assertThat(typeConverter.convert("PRIMARY_KEY"))
        .isEqualTo(org.apache.geode.cache.query.IndexType.PRIMARY_KEY);

    // Test invalid value throws exception
    assertThatThrownBy(() -> typeConverter.convert("invalid"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
