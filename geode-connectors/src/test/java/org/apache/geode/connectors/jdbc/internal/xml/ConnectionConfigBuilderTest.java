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
package org.apache.geode.connectors.jdbc.internal.xml;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

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
    ConnectionConfigBuilder builder = new ConnectionConfigBuilder();
    builder.withName("name").withUrl("url").withUser("user").withPassword("password");
    ConnectionConfiguration config = builder.build();
    assertThat(config.getName()).isEqualTo("name");
    assertThat(config.getUrl()).isEqualTo("url");
    assertThat(config.getUser()).isEqualTo("user");
    assertThat(config.getPassword()).isEqualTo("password");
  }
}
