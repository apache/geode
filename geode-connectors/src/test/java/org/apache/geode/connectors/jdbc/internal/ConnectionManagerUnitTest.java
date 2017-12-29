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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionManagerUnitTest {

  private static final String REGION_NAME = "testRegion";
  private static final String CONFIG_NAME = "configName";
  private JdbcConnectorService configService;
  private JdbcDataSource dataSource;
  private ConnectionManager manager;
  private Connection connection;

  private ConnectionConfiguration connectionConfig;


  @Before
  public void setup() throws Exception {
    dataSource = mock(JdbcDataSource.class);
    configService = mock(JdbcConnectorService.class);
    manager = spy(new ConnectionManager(configService));
    connection = mock(Connection.class);

    connectionConfig = new ConnectionConfiguration("name", "url", null, null, null);

    doReturn(dataSource).when(manager).buildJdbcDataSource(connectionConfig);
    doReturn(connection).when(dataSource).getConnection();
  }

  @Test
  public void getsCorrectMapping() {
    manager.getMappingForRegion(REGION_NAME);

    verify(configService).getMappingForRegion(REGION_NAME);
  }

  @Test
  public void getsCorrectConnectionConfig() {
    manager.getConnectionConfig(CONFIG_NAME);

    verify(configService).getConnectionConfig(CONFIG_NAME);
  }

  @Test
  public void retrievesANewConnection() throws Exception {
    Connection returnedConnection = manager.getConnection(connectionConfig);

    assertThat(returnedConnection).isNotNull().isSameAs(connection);
  }

  @Test
  public void retrievesSameConnectionForSameConnectionConfig() throws Exception {
    Connection returnedConnection = manager.getConnection(connectionConfig);
    Connection secondReturnedConnection = manager.getConnection(connectionConfig);

    assertThat(returnedConnection).isNotNull().isSameAs(connection);
    assertThat(secondReturnedConnection).isNotNull().isSameAs(connection);
  }

  @Test
  public void retrievesDifferentConnectionForEachConfig() throws Exception {
    Connection secondConnection = mock(Connection.class);
    JdbcDataSource dataSource2 = mock(JdbcDataSource.class);
    ConnectionConfiguration secondConnectionConfig =
        new ConnectionConfiguration("newName", "url", null, null, null);
    doReturn(dataSource2).when(manager).buildJdbcDataSource(secondConnectionConfig);
    doReturn(secondConnection).when(dataSource2).getConnection();

    Connection returnedConnection = manager.getConnection(connectionConfig);
    Connection secondReturnedConnection = manager.getConnection(secondConnectionConfig);

    assertThat(returnedConnection).isNotNull().isSameAs(connection);
    assertThat(secondReturnedConnection).isNotNull().isSameAs(secondConnection);
    assertThat(returnedConnection).isNotSameAs(secondReturnedConnection);
  }

  @Test
  public void closesAllDataSources() throws Exception {
    JdbcDataSource dataSource2 = mock(JdbcDataSource.class);
    ConnectionConfiguration secondConnectionConfig =
        new ConnectionConfiguration("newName", "url", null, null, null);
    doReturn(dataSource2).when(manager).buildJdbcDataSource(secondConnectionConfig);
    manager.getConnection(connectionConfig);
    manager.getConnection(secondConnectionConfig);
    manager.close();
    verify(dataSource).close();
    verify(dataSource2).close();
  }
}
