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

package org.apache.geode.management.configuration;

import static org.apache.geode.management.configuration.ClassName.isClassNameValid;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;


public class ClassNameTest {
  private final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Test
  public void constructWithoutProps() {
    ClassName klass = new ClassName("someClassName");
    Properties emptyProps = klass.getInitProperties();
    assertThat(klass.getClassName()).isEqualTo("someClassName");
    assertThat(emptyProps).isEmpty();
  }

  @Test
  public void empty() {
    assertThat(new ClassName("", "{}"))
        .isEqualTo(new ClassName(null, "{}"))
        .isEqualTo(new ClassName(" ", "{\"k\":\"v\"}"))
        .isEqualTo(ClassName.EMPTY);
  }

  @Test
  public void constructWithProperties() {
    ClassName klass = new ClassName("someClassName", "{\"key\" : \"value\"}");
    Properties keyValueProps = klass.getInitProperties();
    assertThat(klass.getClassName()).isEqualTo("someClassName");
    assertThat(keyValueProps).containsOnlyKeys("key").containsEntry("key", "value");
  }

  @Test
  public void constructWithPropertiesWithSpace() {
    String json = "{'k1' : 'v   1', 'k2' : 'v2'}";
    ClassName klass = new ClassName("someClassName", json);
    Properties keyValueProps = klass.getInitProperties();
    assertThat(klass.getClassName()).isEqualTo("someClassName");
    assertThat(keyValueProps).containsOnlyKeys("k1", "k2").containsEntry("k1", "v   1");
  }

  @Test
  public void constructWithSingleQuotes() {
    ClassName klass = new ClassName("someClassName", "{'key' : 'value'}");
    Properties keyValueProps = klass.getInitProperties();
    assertThat(klass.getClassName()).isEqualTo("someClassName");
    assertThat(keyValueProps).containsOnlyKeys("key").containsEntry("key", "value");
  }

  @Test
  public void constructWithEscapedComma() {
    ClassName klass = new ClassName("someClassName", "{'key':'value','key2':'value2'}");
    Properties keyValueProps = klass.getInitProperties();
    assertThat(klass.getClassName()).isEqualTo("someClassName");
    assertThat(keyValueProps).containsOnlyKeys("key", "key2").containsEntry("key", "value");
  }


  @Test
  public void emptyClassName() {
    assertThat(new ClassName(null)).isEqualTo(ClassName.EMPTY);
    assertThat(new ClassName("")).isEqualTo(ClassName.EMPTY);
    assertThat(new ClassName(" ")).isEqualTo(ClassName.EMPTY);
  }

  @Test
  public void illegalJson() {
    assertThatThrownBy(() -> new ClassName("test", "a:b"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void jsonMapper() throws Exception {
    Properties properties = new Properties();
    properties.put("key1", "value1");
    ClassName className = new ClassName("abc", properties);
    ClassName className1 = new ClassName("abc", mapper.writeValueAsString(properties));
    assertThat(className).usingRecursiveComparison().isEqualTo(className1);
    String json = mapper.writeValueAsString(className);
    String json1 = mapper.writeValueAsString(className1);
    assertThat(json).isEqualTo(json1);

    ClassName className2 = mapper.readValue(json, ClassName.class);
    assertThat(className2).usingRecursiveComparison().isEqualTo(className);
  }

  @Test
  public void deserializeIllegalClassName() throws Exception {
    assertThatThrownBy(() -> new ClassName("a b")).isInstanceOf(IllegalArgumentException.class);
    String json = "{\"className\":\"a b\"}";
    assertThatThrownBy(() -> mapper.readValue(json, ClassName.class))
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void serializable() throws Exception {
    Properties properties = new Properties();
    properties.put("key1", "value1");
    ClassName className = new ClassName("java.lang.String", properties);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    ObjectOutputStream outputStream = new ObjectOutputStream(stream);
    outputStream.writeObject(className);
    outputStream.close();

    ObjectInputStream inputStream =
        new ObjectInputStream(new ByteArrayInputStream(stream.toByteArray()));
    Object object = inputStream.readObject();
    assertThat(object).isEqualTo(className);
  }

  @Test
  public void isValidClassNameGivenEmptyStringReturnsFalse() {
    assertThat(isClassNameValid("")).isFalse();
  }

  @Test
  public void isValidClassNameGivenDashReturnsFalse() {
    assertThat(isClassNameValid("-")).isFalse();
  }

  @Test
  public void isValidClassNameGivenSpaceReturnsFalse() {
    assertThat(isClassNameValid(" ")).isFalse();
  }

  @Test
  public void isValidClassNameGivenCommaReturnsFalse() {
    assertThat(isClassNameValid(",")).isFalse();
  }

  @Test
  public void isValidClassNameGivenLeadingDotReturnsFalse() {
    assertThat(isClassNameValid(".a")).isFalse();
  }

  @Test
  public void isValidClassNameGivenTrailingDotReturnsFalse() {
    assertThat(isClassNameValid("a.")).isFalse();
  }

  @Test
  public void isValidClassNameGivenTwoDotsReturnsFalse() {
    assertThat(isClassNameValid("a..a")).isFalse();
  }

  @Test
  public void isValidClassNameGivenNameThatStartsWithDigitReturnsFalse() {
    assertThat(isClassNameValid("9a")).isFalse();
  }

  @Test
  public void isValidClassNameGivenNameReturnsTrue() {
    assertThat(isClassNameValid("a9")).isTrue();
  }

  @Test
  public void isValidClassNameGivenDotDelimitedNamesReturnsTrue() {
    assertThat(isClassNameValid("$a1._b2.c3$")).isTrue();
  }

  @Test
  public void isValidClassNameGivenMiddleNameThatStartsWithDigitReturnsFalse() {
    assertThat(isClassNameValid("a1.2b.c3")).isFalse();
  }

}
