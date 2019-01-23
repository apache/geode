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

package org.apache.geode.management.internal.cli.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.management.domain.ClassName;
import org.apache.geode.management.internal.configuration.domain.DeclarableTypeInstantiator;


public class ClassNameTest {

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
  public void emptyCanNotInstantiate() {
    assertThatThrownBy(() -> DeclarableTypeInstantiator.newInstance(ClassName.EMPTY, null))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Error instantiating class");
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
    assertThatThrownBy(() -> new ClassName("test", ""))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ClassName("test", "a:b"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getInstance() {
    ClassName<String> klass = new ClassName("java.lang.String");
    String s = DeclarableTypeInstantiator.newInstance(klass, null);
    assertThat(s.toString()).isEqualTo("");
  }

  @Test
  public void getInstanceWithProps() {
    String json = "{\"k\":\"v\"}";
    ClassName<MyCacheWriter> cacheWriter = new ClassName<>(MyCacheWriter.class.getName(), json);
    MyCacheWriter obj = DeclarableTypeInstantiator.newInstance(cacheWriter, null);
    assertThat(obj.getProperties()).containsEntry("k", "v").containsOnlyKeys("k");
  }
}
