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

import org.apache.geode.management.cli.ConverterHint;

public class IndexTypeConverterTest {

  IndexTypeConverter typeConverter;
  EnumConverter enumConverter;

  @Before
  public void before() {
    typeConverter = new IndexTypeConverter();
    enumConverter = new EnumConverter();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void supports() {
    assertThat(typeConverter.supports(org.apache.geode.cache.query.IndexType.class,
        ConverterHint.INDEX_TYPE)).isTrue();
    assertThat(typeConverter.supports(Enum.class, ConverterHint.INDEX_TYPE)).isFalse();
    assertThat(typeConverter.supports(org.apache.geode.cache.query.IndexType.class, "")).isFalse();

    assertThat(enumConverter.supports(org.apache.geode.cache.query.IndexType.class, "")).isTrue();
    assertThat(enumConverter.supports(Enum.class, "")).isTrue();
    assertThat(enumConverter.supports(org.apache.geode.cache.query.IndexType.class,
        ConverterHint.INDEX_TYPE)).isFalse();
    assertThat(enumConverter.supports(Enum.class, ConverterHint.DISABLE_ENUM_CONVERTER)).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void convert() {
    assertThat(
        typeConverter.convertFromText("hash", org.apache.geode.cache.query.IndexType.class, ""))
            .isEqualTo(org.apache.geode.cache.query.IndexType.HASH);
    assertThat(
        typeConverter.convertFromText("range", org.apache.geode.cache.query.IndexType.class, ""))
            .isEqualTo(org.apache.geode.cache.query.IndexType.FUNCTIONAL);
    assertThat(
        typeConverter.convertFromText("key", org.apache.geode.cache.query.IndexType.class, ""))
            .isEqualTo(org.apache.geode.cache.query.IndexType.PRIMARY_KEY);
    assertThatThrownBy(() -> typeConverter.convertFromText("invalid",
        org.apache.geode.cache.query.IndexType.class, ""))
            .isInstanceOf(IllegalArgumentException.class);
  }
}
