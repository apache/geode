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
package org.apache.geode.connectors.jdbc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionConfigBuilderTest {

  @Test
  public void createsAllNullObjectIfNothingSet() {
    ConnectionConfiguration config = new ConnectionConfigBuilder().build();

    assertThat(config.getName()).isNull();
    assertThat(config.getUrl()).isNull();
    assertThat(config.getUser()).isNull();
    assertThat(config.getPassword()).isNull();
  }

  @Test
  public void createsObjectWithCorrectValues() {
    ConnectionConfiguration config = new ConnectionConfigBuilder().withName("name").withUrl("url")
        .withUser("user").withPassword("password")
        .withParameters(new String[] {"param1:value1", "param2:value2"}).build();

    assertThat(config.getName()).isEqualTo("name");
    assertThat(config.getUrl()).isEqualTo("url");
    assertThat(config.getUser()).isEqualTo("user");
    assertThat(config.getPassword()).isEqualTo("password");
    assertThat(config.getConnectionProperties()).containsEntry("param1", "value1")
        .containsEntry("param2", "value2");
  }

  @Test
  public void createsObjectWithEmptyParameterElement() {
    ConnectionConfiguration config = new ConnectionConfigBuilder().withName("name").withUrl("url")
        .withUser("user").withPassword("password")
        .withParameters(new String[] {"param1:value1", "", "param2:value2"}).build();

    assertThat(config.getName()).isEqualTo("name");
    assertThat(config.getUrl()).isEqualTo("url");
    assertThat(config.getUser()).isEqualTo("user");
    assertThat(config.getPassword()).isEqualTo("password");
    assertThat(config.getConnectionProperties()).containsEntry("param1", "value1")
        .containsEntry("param2", "value2");
  }

  @Test
  public void createsObjectWithNullParameters() {
    ConnectionConfiguration config =
        new ConnectionConfigBuilder().withName("name").withUrl("url").withParameters(null).build();

    assertThat(config.getName()).isEqualTo("name");
    assertThat(config.getUrl()).isEqualTo("url");
    assertThat(config.getParameters()).isNull();
  }

  @Test
  public void throwsExceptionWithInvalidParams() {
    ConnectionConfigBuilder config = new ConnectionConfigBuilder();
    assertThatThrownBy(() -> config.withParameters(new String[] {"param1value1"}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> config.withParameters(new String[] {":param1value1"}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> config.withParameters(new String[] {"param1value1:"}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> config.withParameters(new String[] {"1:2:3"}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> config.withParameters(new String[] {":"}))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
