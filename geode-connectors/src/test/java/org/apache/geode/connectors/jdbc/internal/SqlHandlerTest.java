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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SqlHandlerTest {
  private static final String REGION_NAME = "testRegion";
  private static final String TABLE_NAME = "testTable";
  private static final Object COLUMN_VALUE_1 = new Object();
  private static final String COLUMN_NAME_1 = "columnName1";
  private static final Object COLUMN_VALUE_2 = new Object();
  private static final String COLUMN_NAME_2 = "columnName2";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private ConnectionManager manager;
  private Region region;
  private InternalCache cache;
  private SqlHandler handler;
  private PreparedStatement statement;
  private RegionMapping regionMapping;
  private PdxInstanceImpl value;

  @Before
  public void setup() throws Exception {
    manager = mock(ConnectionManager.class);
    region = mock(Region.class);
    cache = mock(InternalCache.class);
    when(region.getRegionService()).thenReturn(cache);
    handler = new SqlHandler(manager);
    value = mock(PdxInstanceImpl.class);
    when(value.getPdxType()).thenReturn(mock(PdxType.class));
    setupManagerMock();
  }

  @Test
  public void readReturnsNullIfNoKeyProvided() {
    thrown.expect(IllegalArgumentException.class);
    handler.read(region, null);
  }

  @Test
  public void usesPdxFactoryForClassWhenExists() throws Exception {
    setupEmptyResultSet();
    String pdxClassName = "classname";
    when(regionMapping.getPdxClassName()).thenReturn(pdxClassName);
    handler.read(region, new Object());

    verify(cache).createPdxInstanceFactory(pdxClassName);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void readClearsPreparedStatementWhenFinished() throws Exception {
    setupEmptyResultSet();
    handler.read(region, new Object());
    verify(statement).clearParameters();
  }

  @Test
  public void usesPbxFactoryForNoPbxClassWhenClassNonExistent() throws Exception {
    setupEmptyResultSet();
    handler.read(region, new Object());

    verify(cache).createPdxInstanceFactory("no class", false);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void readReturnsNullIfNoResultsReturned() throws Exception {
    setupEmptyResultSet();
    assertThat(handler.read(region, new Object())).isNull();
  }

  @Test
  public void throwsExceptionIfQueryFails() throws Exception {
    when(statement.executeQuery()).thenThrow(SQLException.class);

    thrown.expect(IllegalStateException.class);
    handler.read(region, new Object());
  }

  @Test
  public void readReturnsDataFromAllResultColumns() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);

    when(manager.getKeyColumnName(any(), anyString())).thenReturn("key");
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);

    String filedName1 = COLUMN_NAME_1.toLowerCase();
    String filedName2 = COLUMN_NAME_2.toLowerCase();
    handler.read(region, new Object());
    verify(factory).writeField(filedName1, COLUMN_VALUE_1, Object.class);
    verify(factory).writeField(filedName2, COLUMN_VALUE_2, Object.class);
    verify(factory).create();
  }

  @Test
  public void readResultOmitsKeyColumnIfNotInValue() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);

    when(manager.getKeyColumnName(any(), anyString())).thenReturn(COLUMN_NAME_1);
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);

    String filedName2 = COLUMN_NAME_2.toLowerCase();
    handler.read(region, new Object());
    verify(factory).writeField(filedName2, COLUMN_VALUE_2, Object.class);
    verify(factory, times(1)).writeField(any(), any(), any());
    verify(factory).create();
  }

  @Test
  public void throwsExceptionIfMoreThatOneResultReturned() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true);
    when(result.getStatement()).thenReturn(mock(PreparedStatement.class));
    when(statement.executeQuery()).thenReturn(result);

    when(manager.getKeyColumnName(any(), anyString())).thenReturn("key");
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean()))
        .thenReturn(mock(PdxInstanceFactory.class));

    thrown.expect(IllegalStateException.class);
    handler.read(region, new Object());
  }

  @Test
  public void writeThrowsExceptionIfValueIsNullAndNotDoingDestroy() {
    thrown.expect(IllegalArgumentException.class);
    handler.write(region, Operation.UPDATE, new Object(), null);
  }

  @Test
  public void insertActionSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).setObject(1, COLUMN_VALUE_1);
    verify(statement).setObject(2, COLUMN_VALUE_2);
  }

  @Test
  public void updateActionSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    handler.write(region, Operation.UPDATE, new Object(), value);
    verify(statement).setObject(1, COLUMN_VALUE_1);
    verify(statement).setObject(2, COLUMN_VALUE_2);
  }

  @Test
  public void destroyActionSucceeds() throws Exception {
    List<ColumnValue> columnList = new ArrayList<>();
    columnList.add(new ColumnValue(true, COLUMN_NAME_1, COLUMN_VALUE_1));
    when(manager.getColumnToValueList(any(), any(), any(), any(), any())).thenReturn(columnList);
    when(statement.executeUpdate()).thenReturn(1);
    handler.write(region, Operation.DESTROY, new Object(), value);
    verify(statement).setObject(1, COLUMN_VALUE_1);
    verify(statement, times(1)).setObject(anyInt(), any());
  }

  @Test
  public void destroyActionThatRemovesNoRowCompletesUnexceptionally() throws Exception {
    List<ColumnValue> columnList = new ArrayList<>();
    columnList.add(new ColumnValue(true, COLUMN_NAME_1, COLUMN_VALUE_1));
    when(manager.getColumnToValueList(any(), any(), any(), any(), any())).thenReturn(columnList);
    when(statement.executeUpdate()).thenReturn(0);
    handler.write(region, Operation.DESTROY, new Object(), value);
    verify(statement).setObject(1, COLUMN_VALUE_1);
    verify(statement, times(1)).setObject(anyInt(), any());
  }

  @Test
  public void destroyThrowExceptionWhenFail() throws Exception {
    List<ColumnValue> columnList = new ArrayList<>();
    columnList.add(new ColumnValue(true, COLUMN_NAME_1, COLUMN_VALUE_1));
    when(manager.getColumnToValueList(any(), any(), any(), any(), any())).thenReturn(columnList);
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    thrown.expect(IllegalStateException.class);
    handler.write(region, Operation.DESTROY, new Object(), value);
  }

  @Test
  public void preparedStatementClearedAfterExecution() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).clearParameters();
  }

  @Test
  public void whenInsertFailsUpdateSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(0);

    PreparedStatement updateStatement = mock(PreparedStatement.class);
    when(updateStatement.executeUpdate()).thenReturn(1);
    when(manager.getPreparedStatement(any(), any(), any(), any(), anyInt())).thenReturn(statement)
        .thenReturn(updateStatement);

    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).executeUpdate();
    verify(updateStatement).executeUpdate();
    verify(statement).clearParameters();
    verify(updateStatement).clearParameters();
  }

  @Test
  public void whenUpdateFailsInsertSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(0);

    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(insertStatement.executeUpdate()).thenReturn(1);
    when(manager.getPreparedStatement(any(), any(), any(), any(), anyInt())).thenReturn(statement)
        .thenReturn(insertStatement);

    handler.write(region, Operation.UPDATE, new Object(), value);
    verify(statement).executeUpdate();
    verify(insertStatement).executeUpdate();
    verify(statement).clearParameters();
    verify(insertStatement).clearParameters();
  }

  @Test
  public void whenInsertFailsWithExceptionUpdateSucceeds() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    PreparedStatement updateStatement = mock(PreparedStatement.class);
    when(updateStatement.executeUpdate()).thenReturn(1);
    when(manager.getPreparedStatement(any(), any(), any(), any(), anyInt())).thenReturn(statement)
        .thenReturn(updateStatement);

    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).executeUpdate();
    verify(updateStatement).executeUpdate();
    verify(statement).clearParameters();
    verify(updateStatement).clearParameters();
  }

  @Test
  public void whenUpdateFailsWithExceptionInsertSucceeds() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(insertStatement.executeUpdate()).thenReturn(1);
    when(manager.getPreparedStatement(any(), any(), any(), any(), anyInt())).thenReturn(statement)
        .thenReturn(insertStatement);

    handler.write(region, Operation.UPDATE, new Object(), value);
    verify(statement).executeUpdate();
    verify(insertStatement).executeUpdate();
    verify(statement).clearParameters();
    verify(insertStatement).clearParameters();
  }

  @Test
  public void whenBothInsertAndUpdateFailExceptionIsThrown() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(insertStatement.executeUpdate()).thenThrow(SQLException.class);
    when(manager.getPreparedStatement(any(), any(), any(), any(), anyInt())).thenReturn(statement)
        .thenReturn(insertStatement);

    thrown.expect(IllegalStateException.class);
    handler.write(region, Operation.UPDATE, new Object(), value);
    verify(statement).clearParameters();
    verify(insertStatement).clearParameters();
  }

  @Test
  public void whenStatementUpdatesMultipleRowsExceptionThrown() throws Exception {
    when(statement.executeUpdate()).thenReturn(2);
    thrown.expect(IllegalStateException.class);
    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).clearParameters();
  }

  private void setupManagerMock() throws SQLException {
    ConnectionConfiguration connectionConfig = mock(ConnectionConfiguration.class);
    when(manager.getConnectionConfig(any())).thenReturn(connectionConfig);

    regionMapping = mock(RegionMapping.class);
    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getTableName()).thenReturn(TABLE_NAME);
    when(manager.getMappingForRegion(any())).thenReturn(regionMapping);

    List<ColumnValue> columnList = new ArrayList<>();
    columnList.add(new ColumnValue(true, COLUMN_NAME_1, COLUMN_VALUE_1));
    columnList.add(new ColumnValue(true, COLUMN_NAME_2, COLUMN_VALUE_2));
    when(manager.getColumnToValueList(any(), any(), any(), any(), any())).thenReturn(columnList);

    statement = mock(PreparedStatement.class);
    when(manager.getPreparedStatement(any(), any(), any(), any(), anyInt())).thenReturn(statement);
  }

  private void setupResultSet(ResultSet result) throws SQLException {
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(result.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(2);

    when(result.getObject(1)).thenReturn(COLUMN_VALUE_1);
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);

    when(result.getObject(2)).thenReturn(COLUMN_VALUE_2);
    when(metaData.getColumnName(2)).thenReturn(COLUMN_NAME_2);
  }

  private void setupEmptyResultSet() throws SQLException {
    ResultSet result = mock(ResultSet.class);
    when(result.next()).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);
  }
}
