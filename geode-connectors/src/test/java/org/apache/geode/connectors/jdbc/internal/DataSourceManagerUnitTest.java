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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DataSourceManagerUnitTest {

  private DataSourceManager manager;

  private JdbcDataSource dataSource;
  private JdbcDataSource dataSource2;

  private ConnectionConfiguration connectionConfig;
  private ConnectionConfiguration connectionConfig2;

  @Before
  public void setup() throws Exception {
    dataSource = mock(JdbcDataSource.class);
    JdbcDataSourceFactory dataSourceFactory = new JdbcDataSourceFactory() {
      @Override
      public JdbcDataSource create(ConnectionConfiguration configuration) {
        return dataSource;
      }

    };
    manager = new DataSourceManager(dataSourceFactory);
    connectionConfig = new ConnectionConfiguration("dataSource1", "url", null, null, null);
    connectionConfig2 = new ConnectionConfiguration("dataSource2", "url", null, null, null);

    dataSource2 = mock(JdbcDataSource.class);
    doThrow(new Exception()).when(dataSource2).close();
  }

  @Test
  public void retrievesANewDataSource() throws Exception {
    JdbcDataSource returnedDataSource = manager.getDataSource(connectionConfig);

    assertThat(returnedDataSource).isNotNull().isSameAs(dataSource);
  }

  @Test
  public void retrievesSameDataSourceForSameConnectionConfig() throws Exception {
    JdbcDataSource returnedDataSource = manager.getDataSource(connectionConfig);
    JdbcDataSource secondReturnedDataSource = manager.getDataSource(connectionConfig);

    assertThat(returnedDataSource).isNotNull().isSameAs(dataSource);
    assertThat(secondReturnedDataSource).isNotNull().isSameAs(dataSource);
  }

  private void registerTwoDataSourceFactory() {
    JdbcDataSourceFactory dataSourceFactory = new JdbcDataSourceFactory() {
      @Override
      public JdbcDataSource create(ConnectionConfiguration configuration) {
        if (configuration.getName().equals("dataSource1")) {
          return dataSource;
        } else if (configuration.getName().equals("dataSource2")) {
          return dataSource2;
        } else {
          throw new IllegalStateException("unexpected " + configuration);
        }
      }
    };
    manager = new DataSourceManager(dataSourceFactory);
  }

  @Test
  public void retrievesDifferentConnectionForEachConfig() throws Exception {
    registerTwoDataSourceFactory();
    JdbcDataSource returnedDataSource = manager.getDataSource(connectionConfig);
    JdbcDataSource secondReturnedDataSource = manager.getDataSource(connectionConfig2);

    assertThat(returnedDataSource).isNotNull().isSameAs(dataSource);
    assertThat(secondReturnedDataSource).isNotNull().isSameAs(dataSource2);
    assertThat(returnedDataSource).isNotSameAs(secondReturnedDataSource);
  }

  @Test
  public void closesAllDataSources() throws Exception {
    registerTwoDataSourceFactory();
    manager.getDataSource(connectionConfig);
    manager.getDataSource(connectionConfig2);
    manager.close();

    verify(dataSource).close();
    verify(dataSource2).close();
  }
}
