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

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionConfigurationTest {

  private String name;
  private String url;
  private String user;
  private String password;
  private Map<String, String> parameters;

  private ConnectionConfiguration config;

  @Before
  public void setUp() {
    name = "name";
    url = "url";
    user = "username";
    password = "secret";

    parameters = new HashMap<>();
    parameters.put("param1", "value1");
    parameters.put("param2", "value2");

    config = new ConnectionConfiguration(null, null, null, null, null);
  }

  @Test
  public void initiatedWithNullValues() {
    assertThat(config.getName()).isNull();
    assertThat(config.getUrl()).isNull();
    assertThat(config.getUser()).isNull();
    assertThat(config.getPassword()).isNull();
  }

  @Test
  public void hasCorrectName() {
    config = new ConnectionConfiguration(name, null, null, null, null);

    assertThat(config.getName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectUrl() {
    config = new ConnectionConfiguration(null, url, null, null, null);

    assertThat(config.getUrl()).isEqualTo(url);
  }

  @Test
  public void hasCorrectUser() {
    config = new ConnectionConfiguration(null, null, user, null, null);

    assertThat(config.getUser()).isEqualTo(user);
  }

  @Test
  public void hasCorrectPassword() {
    config = new ConnectionConfiguration(null, null, null, password, null);

    assertThat(config.getPassword()).isEqualTo(password);
  }

  @Test
  public void hasCorrectProperties() {
    config = new ConnectionConfiguration(null, null, null, null, parameters);

    assertThat(config.getConnectionProperties()).containsEntry("param1", "value1")
        .containsEntry("param2", "value2");
  }
}
