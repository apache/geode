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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler.DataSourceFactory;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.TypeRegistry;

public class CreateMappingPreconditionCheckFunctionTest {

  private static final String REGION_NAME = "testRegion";
  private static final String PDX_CLASS_NAME = "testPdxClassName";
  private static final String DATA_SOURCE_NAME = "testDataSourceName";
  private static final String MEMBER_NAME = "testMemberName";

  private RegionMapping regionMapping;
  private FunctionContext<RegionMapping> context;
  private ResultSender<Object> resultSender;
  private InternalCache cache;
  private TypeRegistry typeRegistry;
  private DataSourceFactory dataSourceFactory;
  private TableMetaDataManager tableMetaDataManager;
  private TableMetaDataView tableMetaDataView;
  private DataSource dataSource;

  private CreateMappingPreconditionCheckFunction function;

  @Before
  public void setUp() throws SQLException {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    cache = mock(InternalCache.class);
    typeRegistry = mock(TypeRegistry.class);
    when(cache.getPdxRegistry()).thenReturn(typeRegistry);
    regionMapping = mock(RegionMapping.class);

    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getPdxName()).thenReturn(PDX_CLASS_NAME);
    when(regionMapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(context.getArguments()).thenReturn(regionMapping);
    when(context.getMemberName()).thenReturn(MEMBER_NAME);

    dataSourceFactory = mock(DataSourceFactory.class);
    dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(dataSourceFactory.getDataSource(DATA_SOURCE_NAME)).thenReturn(dataSource);
    tableMetaDataManager = mock(TableMetaDataManager.class);
    tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
        .thenReturn(tableMetaDataView);
    function = new CreateMappingPreconditionCheckFunction(dataSourceFactory, tableMetaDataManager);
  }

  @Test
  public void isHAReturnsFalse() {
    assertThat(function.isHA()).isFalse();
  }

  @Test
  public void getIdReturnsNameOfClass() {
    assertThat(function.getId()).isEqualTo(function.getClass().getName());
  }

  @Test
  public void serializes() {
    Serializable original = function;

    Object copy = SerializationUtils.clone(original);

    assertThat(copy).isNotSameAs(original)
        .isInstanceOf(CreateMappingPreconditionCheckFunction.class);
  }

  @Test
  public void executeFunctionThrowsIfDataSourceDoesNotExist() throws Exception {
    when(dataSourceFactory.getDataSource(DATA_SOURCE_NAME)).thenReturn(null);

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage("JDBC data-source named \"" + DATA_SOURCE_NAME
            + "\" not found. Create it with gfsh 'create data-source --pooled --name="
            + DATA_SOURCE_NAME + "'.");
  }

  @Test
  public void executeFunctionThrowsIfDataSourceGetConnectionThrows() throws SQLException {
    String reason = "connection failed";
    when(dataSource.getConnection()).thenThrow(new SQLException(reason));

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(reason);
  }

  @Test
  public void executeFunctionReturnsNoFieldMappingsIfNoColumns() throws Exception {
    Set<String> columnNames = Collections.emptySet();
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    ArrayList<FieldMapping> fieldsMappings = (ArrayList<FieldMapping>) outputs[1];
    assertThat(fieldsMappings).isEmpty();
  }

  @Test
  public void executeFunctionReturnsFieldMappingsThatMatchTableMetaData() throws Exception {
    Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("col1", "col2"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    when(tableMetaDataView.isColumnNullable("col2")).thenReturn(true);
    when(tableMetaDataView.getColumnDataType("col2")).thenReturn(JDBCType.DATE);

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    ArrayList<FieldMapping> fieldsMappings = (ArrayList<FieldMapping>) outputs[1];
    assertThat(fieldsMappings).hasSize(2);
    assertThat(fieldsMappings.get(0))
        .isEqualTo(
            new FieldMapping("", "", "col1", JDBCType.DATE.name(), false));
    assertThat(fieldsMappings.get(1))
        .isEqualTo(
            new FieldMapping("", "", "col2", JDBCType.DATE.name(), true));
  }

  @Test
  public void executeFunctionReturnsFieldMappingsThatMatchTableMetaDataAndExistingPdxField()
      throws Exception {
    Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("COL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.LONG);
    when(typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "col1"))
        .thenReturn(Collections.singleton(pdxField1));

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    ArrayList<FieldMapping> fieldsMappings = (ArrayList<FieldMapping>) outputs[1];
    assertThat(fieldsMappings).hasSize(1);
    assertThat(fieldsMappings.get(0))
        .isEqualTo(
            new FieldMapping("COL1", FieldType.LONG.name(), "col1", JDBCType.DATE.name(), false));
  }

  @Test
  public void executeFunctionThrowsGivenExistingPdxTypeWithMultipleInexactMatches()
      throws Exception {
    Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    when(typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "col1"))
        .thenThrow(new IllegalStateException("reason"));

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage(
            "Could not determine what pdx field to use for the column name col1 because reason");
  }

  @Test
  public void executeFunctionReturnsResultWithCorrectMemberName() throws Exception {
    when(regionMapping.getIds()).thenReturn("myId");

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberIdOrName()).isEqualTo(MEMBER_NAME);
  }

  @Test
  public void executeFunctionReturnsNullInSlotZeroIfRegionMappingHasIds() throws Exception {
    when(regionMapping.getIds()).thenReturn("myId");

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    assertThat(outputs[0]).isNull();
  }

  @Test
  public void executeFunctionReturnsViewsKeyColumnsInSlotZeroIfRegionMappingHasNullIds()
      throws Exception {
    when(regionMapping.getIds()).thenReturn(null);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("keyCol1", "keyCol2"));

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    assertThat(outputs[0]).isEqualTo("keyCol1,keyCol2");
  }

  @Test
  public void executeFunctionReturnsViewsKeyColumnsInSlotZeroIfRegionMappingHasEmptyIds()
      throws Exception {
    when(regionMapping.getIds()).thenReturn("");
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("keyCol1"));

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    assertThat(outputs[0]).isEqualTo("keyCol1");
  }
}
