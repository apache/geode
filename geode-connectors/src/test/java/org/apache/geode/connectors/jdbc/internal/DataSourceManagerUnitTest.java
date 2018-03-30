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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    JdbcDataSourceFactory dataSourceFactory = mock(JdbcDataSourceFactory.class);
    when(dataSourceFactory.create(any())).thenReturn(dataSource);
    manager = new DataSourceManager(dataSourceFactory);
    connectionConfig = new ConnectionConfiguration("dataSource1", "url", null, null, null);
    connectionConfig2 = new ConnectionConfiguration("dataSource2", "url", null, null, null);

    dataSource2 = mock(JdbcDataSource.class);
    doThrow(new Exception()).when(dataSource2).close();
  }

  @Test
  public void retrievesANewDataSource() throws Exception {
    JdbcDataSource returnedDataSource = manager.getOrCreateDataSource(connectionConfig);

    assertThat(returnedDataSource).isNotNull().isSameAs(dataSource);
  }

  @Test
  public void retrievesSameDataSourceForSameConnectionConfig() throws Exception {
    JdbcDataSource returnedDataSource = manager.getOrCreateDataSource(connectionConfig);
    JdbcDataSource secondReturnedDataSource = manager.getOrCreateDataSource(connectionConfig);

    assertThat(returnedDataSource).isNotNull().isSameAs(dataSource);
    assertThat(secondReturnedDataSource).isNotNull().isSameAs(dataSource);
  }

  private void registerTwoDataSourceFactory() {
    JdbcDataSourceFactory dataSourceFactory = mock(JdbcDataSourceFactory.class);
    when(dataSourceFactory.create(connectionConfig)).thenReturn(dataSource);
    when(dataSourceFactory.create(connectionConfig2)).thenReturn(dataSource2);
    manager = new DataSourceManager(dataSourceFactory);
  }

  @Test
  public void retrievesDifferentDataSourceForEachConfig() throws Exception {
    registerTwoDataSourceFactory();
    JdbcDataSource returnedDataSource = manager.getOrCreateDataSource(connectionConfig);
    JdbcDataSource secondReturnedDataSource = manager.getOrCreateDataSource(connectionConfig2);

    assertThat(returnedDataSource).isNotNull().isSameAs(dataSource);
    assertThat(secondReturnedDataSource).isNotNull().isSameAs(dataSource2);
    assertThat(returnedDataSource).isNotSameAs(secondReturnedDataSource);
  }

  @Test
  public void closesAllDataSources() throws Exception {
    registerTwoDataSourceFactory();
    manager.getOrCreateDataSource(connectionConfig);
    manager.getOrCreateDataSource(connectionConfig2);
    manager.close();

    verify(dataSource).close();
    verify(dataSource2).close();
  }
}
