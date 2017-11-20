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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionConfigurationTest {

  @Test
  public void initiatedWithNullValues() {
    ConnectionConfiguration config = new ConnectionConfiguration(null, null, null, null, null);
    assertThat(config.getName()).isNull();
    assertThat(config.getUrl()).isNull();
    assertThat(config.getUser()).isNull();
    assertThat(config.getPassword()).isNull();
  }

  @Test
  public void hasCorrectName() {
    String name = "name";
    ConnectionConfiguration config = new ConnectionConfiguration(name, null, null, null, null);
    assertThat(config.getName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectUrl() {
    String url = "url";
    ConnectionConfiguration config = new ConnectionConfiguration(null, url, null, null, null);
    assertThat(config.getUrl()).isEqualTo(url);
  }

  @Test
  public void hasCorrectUser() {
    String user = "user";
    ConnectionConfiguration config = new ConnectionConfiguration(null, null, user, null, null);
    assertThat(config.getUser()).isEqualTo(user);
  }

  @Test
  public void hasCorrectPassword() {
    String password = "password";
    ConnectionConfiguration config = new ConnectionConfiguration(null, null, null, password, null);
    assertThat(config.getPassword()).isEqualTo(password);
  }

  @Test
  public void hasCorrectProperties() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("param1", "value1");
    parameters.put("param2", "value2");
    ConnectionConfiguration config =
        new ConnectionConfiguration(null, null, "username", "secret", parameters);
    assertThat(config.getConnectionProperties()).containsEntry("user", "username")
        .containsEntry("password", "secret").containsEntry("param1", "value1")
        .containsEntry("param2", "value2");
  }
}
