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
package org.apache.geode.internal.datasource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class GemFireBasicDataSourceJUnitTest {

  private GemFireBasicDataSource dataSource;
  private Map<String, String> params;

  @Before
  public void setUp() {
    params = new HashMap<>();

    params.put("connection-url", "jdbc:postgresql://myurl:5432");
    params.put("jdbc-driver-class", "org.apache.geode.internal.datasource.TestDriver");
    params.put("jndi-name", "datasource");
  }

  @Test
  public void connectWithoutUsernameOrPassword() throws DataSourceCreateException {
    dataSource = (GemFireBasicDataSource) DataSourceFactory.getSimpleDataSource(params);

    assertThatThrownBy(() -> dataSource.getConnection())
        .hasMessage("Test Driver Connection attempted!");
  }

  @Test
  public void connectWithUsernameButNoPassword() throws DataSourceCreateException {
    params.put("user-name", "myUser");

    dataSource = (GemFireBasicDataSource) DataSourceFactory.getSimpleDataSource(params);

    assertThatThrownBy(() -> dataSource.getConnection())
        .hasMessage("Test Driver Connection attempted!");
  }

  @Test
  public void connectWithPasswordButNoUsername() throws DataSourceCreateException {
    params.put("password", "myPassword");

    dataSource = (GemFireBasicDataSource) DataSourceFactory.getSimpleDataSource(params);

    assertThatThrownBy(() -> dataSource.getConnection())
        .hasMessage("Test Driver Connection attempted!");
  }
}
