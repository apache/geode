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

import org.apache.geode.cache.query.IndexType;
import org.apache.geode.management.cli.ConverterHint;

public class IndexTypeConverterTest {

  IndexTypeConverter typeConverter;
  EnumConverter enumConverter;

  @Before
  public void before() throws Exception {
    typeConverter = new IndexTypeConverter();
    enumConverter = new EnumConverter();
  }

  @Test
  public void supports() throws Exception {
    assertThat(typeConverter.supports(IndexType.class, ConverterHint.INDEX_TYPE)).isTrue();
    assertThat(typeConverter.supports(Enum.class, ConverterHint.INDEX_TYPE)).isFalse();
    assertThat(typeConverter.supports(IndexType.class, "")).isFalse();

    assertThat(enumConverter.supports(IndexType.class, "")).isTrue();
    assertThat(enumConverter.supports(Enum.class, "")).isTrue();
    assertThat(enumConverter.supports(IndexType.class, ConverterHint.INDEX_TYPE)).isFalse();
    assertThat(enumConverter.supports(Enum.class, ConverterHint.DISABLE_ENUM_CONVERTER)).isFalse();
  }

  @Test
  public void convert() throws Exception {
    assertThat(typeConverter.convertFromText("hash", IndexType.class, ""))
        .isEqualTo(IndexType.HASH);
    assertThat(typeConverter.convertFromText("range", IndexType.class, ""))
        .isEqualTo(IndexType.FUNCTIONAL);
    assertThat(typeConverter.convertFromText("key", IndexType.class, ""))
        .isEqualTo(IndexType.PRIMARY_KEY);
    assertThatThrownBy(() -> typeConverter.convertFromText("invalid", IndexType.class, ""))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
