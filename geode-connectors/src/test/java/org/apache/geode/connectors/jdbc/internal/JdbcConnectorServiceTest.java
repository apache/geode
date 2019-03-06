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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.JDBCType;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.ExtensionPoint;

public class JdbcConnectorServiceTest {

  private static final String TEST_REGION_NAME = "testRegion";

  private RegionMapping mapping;

  private JdbcConnectorServiceImpl service;

  @Before
  public void setUp() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    mapping = mock(RegionMapping.class);
    String[] parameters = new String[] {"key1:value1", "key2:value2"};

    when(cache.getExtensionPoint()).thenReturn(mock(ExtensionPoint.class));
    when(mapping.getRegionName()).thenReturn(TEST_REGION_NAME);

    service = new JdbcConnectorServiceImpl();
    service.init(cache);
  }

  @Test
  public void returnsNoMappingIfEmpty() throws Exception {
    assertThat(service.getMappingForRegion("foo")).isNull();
  }

  @Test
  public void returnsCorrectMapping() throws Exception {
    service.createRegionMapping(mapping);

    assertThat(service.getMappingForRegion(TEST_REGION_NAME)).isSameAs(mapping);
  }

  @Test
  public void doesNotReturnMappingForDifferentRegion() throws Exception {
    when(mapping.getRegionName()).thenReturn("theOtherMapping");
    service.createRegionMapping(mapping);

    assertThat(service.getMappingForRegion(TEST_REGION_NAME)).isNull();
  }

//  @Test
//  public void createSqlHandlerThrowsNoExceptionWithMatchingMapping() {
//    createSqlHandler();
//  }
//
//  @Test(expected = JdbcConnectorException.class)
//  public void createSqlHandlerThrowsExceptionWithAddedColumn() {
//    String extraColumn = "extra_column";
//    columnNames.add(extraColumn);
//    when(tableMetaDataView.getColumnDataType(extraColumn)).thenReturn(JDBCType.VARCHAR);
//    createSqlHandler();
//  }
//
//  @Test(expected = JdbcConnectorException.class)
//  public void createSqlHandlerThrowsExceptionWithRemovedColumn() {
//    columnNames.remove(fieldName);
//    createSqlHandler();
//  }
//
//  @Test(expected = JdbcConnectorException.class)
//  public void createSqlHandlerThrowsExceptionWithColumnNameChanged() {
//    columnNames.remove(fieldName);
//    columnNames.add(fieldName.toUpperCase());
//    when(tableMetaDataView.getColumnDataType(fieldName.toUpperCase())).thenReturn(JDBCType.VARCHAR);
//    createSqlHandler();
//  }
//
//  @Test(expected = JdbcConnectorException.class)
//  public void createSqlHandlerThrowsExceptionWithModifiedColumn() {
//    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(JDBCType.INTEGER);
//    createSqlHandler();
//  }
//
//  @Test(expected = JdbcConnectorException.class)
//  public void createSqlHandlerThrowsExceptionWithModifiedColumnIsNullable() {
//    when(tableMetaDataView.isColumnNullable(fieldName)).thenReturn(true);
//    createSqlHandler();
//  }
//
//  @Test(expected = JdbcConnectorException.class)
//  public void createSqlHandlerThrowsExceptionWithModifiedIdColumns() {
//    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList(KEY_COLUMN.toUpperCase()));
//    createSqlHandler();
//  }
}
