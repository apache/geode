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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.ExtensionPoint;

public class JdbcConnectorServiceTest {

  private static final String TEST_REGION_NAME = "testRegion";
  private static final String DATA_SOURCE_NAME = "dataSource";

  private static final String KEY_COLUMN_NAME = "id";
  private static final String COMPOSITE_KEY_COLUMN_NAME = "Key2";
  private static final String VALUE_COLUMN_NAME = "name";
  private static final String EXTRA_COLUMN_NAME = "extraColumn";

  private final List<String> keyColumns = new ArrayList<>();
  private final Set<String> allColumns = new HashSet<>();
  private final List<FieldMapping> fieldMappings = new ArrayList<>();

  private RegionMapping mapping;

  private JdbcConnectorServiceImpl service;

  TableMetaDataView view = mock(TableMetaDataView.class);
  TableMetaDataManager manager = mock(TableMetaDataManager.class);
  InternalCache cache = mock(InternalCache.class);
  DataSource dataSource = mock(DataSource.class);
  Connection connection = mock(Connection.class);

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    mapping = mock(RegionMapping.class);

    when(cache.getExtensionPoint()).thenReturn(mock(ExtensionPoint.class));
    when(mapping.getRegionName()).thenReturn(TEST_REGION_NAME);
    when(mapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);
    when(mapping.getFieldMappings()).thenReturn(fieldMappings);
    when(mapping.getIds()).thenReturn(KEY_COLUMN_NAME);
    when(mapping.getSpecifiedIds()).thenReturn(true);

    when(dataSource.getConnection()).thenReturn(connection);
    when(manager.getTableMetaDataView(connection, mapping)).thenReturn(view);
    when(view.getKeyColumnNames()).thenReturn(keyColumns);
    when(view.getColumnNames()).thenReturn(allColumns);
    when(view.getColumnDataType(KEY_COLUMN_NAME)).thenReturn(JDBCType.INTEGER);
    when(view.getColumnDataType(VALUE_COLUMN_NAME)).thenReturn(JDBCType.VARCHAR);
    when(view.isColumnNullable(KEY_COLUMN_NAME)).thenReturn(false);
    when(view.isColumnNullable(VALUE_COLUMN_NAME)).thenReturn(true);

    service = spy(JdbcConnectorServiceImpl.class);
    service.init(cache);

    keyColumns.add(KEY_COLUMN_NAME);
    allColumns.add(KEY_COLUMN_NAME);
    allColumns.add(VALUE_COLUMN_NAME);

    fieldMappings
        .add(new FieldMapping("id", "integer", KEY_COLUMN_NAME, JDBCType.INTEGER.getName(), false));
    fieldMappings.add(
        new FieldMapping("name", "string", VALUE_COLUMN_NAME, JDBCType.VARCHAR.getName(), true));

    doReturn(dataSource).when(service).getDataSource(DATA_SOURCE_NAME);
    doReturn(manager).when(service).getTableMetaDataManager();
  }

  @Test
  public void returnsNoMappingIfEmpty() {
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

  @Test
  public void validateMappingSucceedsWithMatchingMapping() {
    service.validateMapping(mapping);
  }

  @Test
  public void validateMappingSucceedsWithMatchingMappingAndUnspecifiedIds() {
    when(mapping.getSpecifiedIds()).thenReturn(false);
    service.validateMapping(mapping);
  }

  @Test
  public void validateMappingThrowsExceptionWhenGetConnectionHasSqlException() throws SQLException {
    when(dataSource.getConnection()).thenThrow(SQLException.class);
    Throwable throwable = catchThrowable(() -> service.validateMapping(mapping));
    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
        "Exception thrown while connecting to datasource \"dataSource\": null");
    verify(connection, never()).close();
  }

  @Test
  public void validateMappingClosesConnectionWhenGetTableMetaDataViewThrows() throws SQLException {
    when(manager.getTableMetaDataView(connection, mapping)).thenThrow(JdbcConnectorException.class);
    Throwable throwable = catchThrowable(() -> service.validateMapping(mapping));
    assertThat(throwable).isInstanceOf(JdbcConnectorException.class);
    verify(connection).close();
  }

  @Test(expected = JdbcConnectorException.class)
  public void validateMappingThrowsExceptionWithNullDataSource() {
    doReturn(null).when(service).getDataSource(DATA_SOURCE_NAME);
    service.validateMapping(mapping);
  }

  @Test(expected = JdbcConnectorException.class)
  public void validateMappingThrowsExceptionWithAddedColumn() {
    allColumns.add(EXTRA_COLUMN_NAME);
    when(view.getColumnDataType(EXTRA_COLUMN_NAME)).thenReturn(JDBCType.VARCHAR);
    service.validateMapping(mapping);
  }

  @Test(expected = JdbcConnectorException.class)
  public void validateMappingThrowsExceptionWithRemovedColumn() {
    allColumns.remove(VALUE_COLUMN_NAME);
    service.validateMapping(mapping);
  }

  @Test(expected = JdbcConnectorException.class)
  public void validateMappingThrowsExceptionWithColumnNameChanged() {
    allColumns.remove(VALUE_COLUMN_NAME);
    allColumns.add(VALUE_COLUMN_NAME.toUpperCase());
    when(view.getColumnDataType(VALUE_COLUMN_NAME.toUpperCase())).thenReturn(JDBCType.VARCHAR);
    service.validateMapping(mapping);
  }

  @Test(expected = JdbcConnectorException.class)
  public void validateMappingThrowsExceptionWithModifiedColumn() {
    when(view.getColumnDataType(VALUE_COLUMN_NAME)).thenReturn(JDBCType.INTEGER);
    service.validateMapping(mapping);
  }

  @Test(expected = JdbcConnectorException.class)
  public void validateMappingThrowsExceptionWithModifiedColumnIsNullable() {
    when(view.isColumnNullable(VALUE_COLUMN_NAME)).thenReturn(false);
    service.validateMapping(mapping);
  }

  @Test(expected = JdbcConnectorException.class)
  public void validateMappingThrowsExceptionWithModifiedIdColumns() {
    when(view.getKeyColumnNames()).thenReturn(singletonList(VALUE_COLUMN_NAME.toUpperCase()));
    when(mapping.getSpecifiedIds()).thenReturn(false);
    service.validateMapping(mapping);
  }

  @Test
  public void validateMappingSucceedsWithModifiedIdColumnsWithSpecifiedIds() {
    when(view.getKeyColumnNames()).thenReturn(singletonList(VALUE_COLUMN_NAME.toUpperCase()));
    service.validateMapping(mapping);
  }

  @Test
  public void validateMappingSucceedsWithCompositeKeys() {
    keyColumns.add(COMPOSITE_KEY_COLUMN_NAME);
    allColumns.add(COMPOSITE_KEY_COLUMN_NAME);
    when(view.getColumnDataType(COMPOSITE_KEY_COLUMN_NAME)).thenReturn(JDBCType.INTEGER);
    when(view.isColumnNullable(COMPOSITE_KEY_COLUMN_NAME)).thenReturn(false);
    fieldMappings.add(new FieldMapping("key2", "integer", COMPOSITE_KEY_COLUMN_NAME,
        JDBCType.INTEGER.getName(), false));
    when(mapping.getSpecifiedIds()).thenReturn(false);
    when(mapping.getIds()).thenReturn(KEY_COLUMN_NAME + "," + COMPOSITE_KEY_COLUMN_NAME);
    service.validateMapping(mapping);
  }
}
