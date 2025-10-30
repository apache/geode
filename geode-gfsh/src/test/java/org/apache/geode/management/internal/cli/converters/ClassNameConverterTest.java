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

import org.apache.geode.management.configuration.ClassName;

/**
 * Unit tests for {@link ClassNameConverter}.
 *
 * SPRING SHELL 3.x MIGRATION:
 * - Spring Shell 1.x API: convertFromText(value, targetType, optionContext)
 * - Spring Shell 3.x API: convert(source) - implements Converter<String, ClassName>
 * - Spring Shell 1.x API: supports(type, optionContext) - removed (type safety via generics)
 * - Spring Shell 1.x API: getAllPossibleValues() - removed (no auto-completion for class names)
 *
 * Migration Notes:
 * - ClassNameConverter now implements org.springframework.core.convert.converter.Converter<String,
 * ClassName>
 * - Type safety enforced by generics instead of supports() method
 * - Conversion logic unchanged: parses className{jsonProperties} format
 * - Test focus: verify convert() handles various input formats correctly
 */
public class ClassNameConverterTest {

  private ClassNameConverter converter;

  @Before
  public void before() throws Exception {
    converter = new ClassNameConverter();
  }

  @Test
  public void convertClassOnly() {
    // SPRING SHELL 3.x: convert() takes only source string
    ClassName declarable = converter.convert("abc");
    assertThat(declarable.getClassName()).isEqualTo("abc");
    assertThat(declarable.getInitProperties()).isEmpty();
  }

  @Test
  public void convertClassAndEmptyProp() {
    ClassName declarable = converter.convert("abc{}");
    assertThat(declarable.getClassName()).isEqualTo("abc");
    assertThat(declarable.getInitProperties()).isEmpty();
  }

  @Test
  public void convertWithOnlyDelimiter() {
    assertThat(converter.convert("{}")).isEqualTo(ClassName.EMPTY);
  }

  @Test
  public void convertWithInvalidClassName() {
    // Invalid characters in class name should throw exception
    // The ClassName constructor validates the class name format
    assertThatThrownBy(() -> converter.convert("abc?{}"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid className");
  }

  @Test
  public void convertWithEmptyString() {
    ClassName className = converter.convert("");
    assertThat(className).isEqualTo(ClassName.EMPTY);
  }

  @Test
  public void convertClassAndProperties() {
    String json = "{'k1':'v1','k2':'v2'}";
    ClassName declarable = converter.convert("abc" + json);
    assertThat(declarable.getClassName()).isEqualTo("abc");
    assertThat(declarable.getInitProperties()).containsOnlyKeys("k1", "k2")
        .containsEntry("k1", "v1").containsEntry("k2", "v2");
  }

  @Test
  public void convertClassAndPropertiesWithDoubleQuotes() {
    String json = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
    ClassName declarable = converter.convert("abc" + json);
    assertThat(declarable.getClassName()).isEqualTo("abc");
    assertThat(declarable.getInitProperties()).containsOnlyKeys("k1", "k2")
        .containsEntry("k1", "v1").containsEntry("k2", "v2");
  }

  @Test
  public void convertFullyQualifiedClassName() {
    ClassName declarable = converter.convert("com.example.MyCacheLoader");
    assertThat(declarable.getClassName()).isEqualTo("com.example.MyCacheLoader");
    assertThat(declarable.getInitProperties()).isEmpty();
  }

  @Test
  public void convertFullyQualifiedClassNameWithProperties() {
    String json = "{'url':'jdbc:mysql://localhost','username':'admin'}";
    ClassName declarable = converter.convert("com.example.DataSourceLoader" + json);
    assertThat(declarable.getClassName()).isEqualTo("com.example.DataSourceLoader");
    assertThat(declarable.getInitProperties()).containsOnlyKeys("url", "username")
        .containsEntry("url", "jdbc:mysql://localhost").containsEntry("username", "admin");
  }
}
