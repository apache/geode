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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.pdx.PdxInstance;

@Experimental
public class SqlHandler {
  private final JdbcConnectorService configService;
  private final TableMetaDataManager tableMetaDataManager;
  private final DataSourceFactory dataSourceFactory;

  public SqlHandler(TableMetaDataManager tableMetaDataManager, JdbcConnectorService configService,
      DataSourceFactory dataSourceFactory) {
    this.tableMetaDataManager = tableMetaDataManager;
    this.configService = configService;
    this.dataSourceFactory = dataSourceFactory;
  }

  public SqlHandler(TableMetaDataManager tableMetaDataManager, JdbcConnectorService configService) {
    this(tableMetaDataManager, configService,
        dataSourceName -> JNDIInvoker.getDataSource(dataSourceName));
  }

  Connection getConnection(String connectionName) throws SQLException {
    return getDataSource(connectionName).getConnection();
  }

  DataSource getDataSource(String connectionName) {
    DataSource dataSource = this.dataSourceFactory.getDataSource(connectionName);
    if (dataSource == null) {
      throw new JdbcConnectorException("JDBC connection with name " + connectionName
          + " not found. Create the connection with the gfsh command 'create jndi-binding'");
    }
    return dataSource;
  }

  public <K, V> PdxInstance read(Region<K, V> region, K key) throws SQLException {
    if (key == null) {
      throw new IllegalArgumentException("Key for query cannot be null");
    }

    RegionMapping regionMapping = getMappingForRegion(region.getName());
    PdxInstance result;
    try (Connection connection = getConnection(regionMapping.getConnectionConfigName())) {
      TableMetaDataView tableMetaData = this.tableMetaDataManager.getTableMetaDataView(connection,
          regionMapping.getRegionToTableName());
      EntryColumnData entryColumnData =
          getEntryColumnData(tableMetaData, regionMapping, key, null, Operation.GET);
      try (PreparedStatement statement =
          getPreparedStatement(connection, tableMetaData, entryColumnData, Operation.GET)) {
        try (ResultSet resultSet = executeReadQuery(statement, entryColumnData)) {
          InternalCache cache = (InternalCache) region.getRegionService();
          SqlToPdxInstanceCreator sqlToPdxInstanceCreator =
              new SqlToPdxInstanceCreator(cache, regionMapping, resultSet, tableMetaData);
          result = sqlToPdxInstanceCreator.create();
        }
      }
    }
    return result;
  }

  private ResultSet executeReadQuery(PreparedStatement statement, EntryColumnData entryColumnData)
      throws SQLException {
    setValuesInStatement(statement, entryColumnData);
    return statement.executeQuery();
  }

  private RegionMapping getMappingForRegion(String regionName) {
    RegionMapping regionMapping =
        this.configService.getMappingForRegion(regionName);
    if (regionMapping == null) {
      throw new JdbcConnectorException("JDBC mapping for region " + regionName
          + " not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    }
    return regionMapping;
  }

  private void setValuesInStatement(PreparedStatement statement, EntryColumnData entryColumnData)
      throws SQLException {
    int index = 0;
    for (ColumnData columnData : entryColumnData.getEntryValueColumnData()) {
      index++;
      setValueOnStatement(statement, index, columnData);
    }

    ColumnData keyColumnData = entryColumnData.getEntryKeyColumnData();
    index++;
    setValueOnStatement(statement, index, keyColumnData);
  }

  private void setValueOnStatement(PreparedStatement statement, int index, ColumnData columnData)
      throws SQLException {
    Object value = columnData.getValue();
    if (value instanceof Character) {
      Character character = ((Character) value);
      // if null character, set to null string instead of a string with the null character
      value = character == Character.valueOf((char) 0) ? null : character.toString();
    } else if (value instanceof Date) {
      Date jdkDate = (Date) value;
      switch (columnData.getDataType()) {
        case Types.DATE:
          value = new java.sql.Date(jdkDate.getTime());
          break;
        case Types.TIME:
        case Types.TIME_WITH_TIMEZONE:
          value = new java.sql.Time(jdkDate.getTime());
          break;
        case Types.TIMESTAMP:
        case Types.TIMESTAMP_WITH_TIMEZONE:
          value = new java.sql.Timestamp(jdkDate.getTime());
          break;
        default:
          // no conversion needed
          break;
      }
    }
    if (value == null) {
      statement.setNull(index, columnData.getDataType());
    } else {
      statement.setObject(index, value);
    }
  }

  public <K, V> void write(Region<K, V> region, Operation operation, K key, PdxInstance value)
      throws SQLException {
    if (value == null && operation != Operation.DESTROY) {
      throw new IllegalArgumentException("PdxInstance cannot be null for non-destroy operations");
    }
    RegionMapping regionMapping = getMappingForRegion(region.getName());

    try (Connection connection = getConnection(regionMapping.getConnectionConfigName())) {
      TableMetaDataView tableMetaData = this.tableMetaDataManager.getTableMetaDataView(connection,
          regionMapping.getRegionToTableName());
      EntryColumnData entryColumnData =
          getEntryColumnData(tableMetaData, regionMapping, key, value, operation);
      int updateCount = 0;
      try (PreparedStatement statement =
          getPreparedStatement(connection, tableMetaData, entryColumnData, operation)) {
        updateCount = executeWriteStatement(statement, entryColumnData);
      } catch (SQLException e) {
        if (operation.isDestroy()) {
          throw e;
        }
      }

      // Destroy action not guaranteed to modify any database rows
      if (operation.isDestroy()) {
        return;
      }

      if (updateCount <= 0) {
        Operation upsertOp = getOppositeOperation(operation);
        try (PreparedStatement upsertStatement =
            getPreparedStatement(connection, tableMetaData, entryColumnData, upsertOp)) {
          updateCount = executeWriteStatement(upsertStatement, entryColumnData);
        }
      }

      assert updateCount == 1;
    }
  }

  private Operation getOppositeOperation(Operation operation) {
    return operation.isUpdate() ? Operation.CREATE : Operation.UPDATE;
  }

  private int executeWriteStatement(PreparedStatement statement, EntryColumnData entryColumnData)
      throws SQLException {
    setValuesInStatement(statement, entryColumnData);
    return statement.executeUpdate();
  }

  private PreparedStatement getPreparedStatement(Connection connection,
      TableMetaDataView tableMetaData, EntryColumnData entryColumnData, Operation operation)
      throws SQLException {
    String sqlStr = getSqlString(tableMetaData, entryColumnData, operation);
    return connection.prepareStatement(sqlStr);
  }

  private String getSqlString(TableMetaDataView tableMetaData, EntryColumnData entryColumnData,
      Operation operation) {
    SqlStatementFactory statementFactory =
        new SqlStatementFactory(tableMetaData.getIdentifierQuoteString());
    String tableName = tableMetaData.getTableName();
    if (operation.isCreate()) {
      return statementFactory.createInsertSqlString(tableName, entryColumnData);
    } else if (operation.isUpdate()) {
      return statementFactory.createUpdateSqlString(tableName, entryColumnData);
    } else if (operation.isDestroy()) {
      return statementFactory.createDestroySqlString(tableName, entryColumnData);
    } else if (operation.isGet()) {
      return statementFactory.createSelectQueryString(tableName, entryColumnData);
    } else {
      throw new InternalGemFireException("unsupported operation " + operation);
    }
  }

  <K> EntryColumnData getEntryColumnData(TableMetaDataView tableMetaData,
      RegionMapping regionMapping, K key, PdxInstance value, Operation operation) {
    String keyColumnName = tableMetaData.getKeyColumnName();
    ColumnData keyColumnData =
        new ColumnData(keyColumnName, key, tableMetaData.getColumnDataType(keyColumnName));
    List<ColumnData> valueColumnData = null;

    if (operation.isCreate() || operation.isUpdate()) {
      valueColumnData = createColumnDataList(tableMetaData, regionMapping, value);
    }

    return new EntryColumnData(keyColumnData, valueColumnData);
  }

  private List<ColumnData> createColumnDataList(TableMetaDataView tableMetaData,
      RegionMapping regionMapping, PdxInstance value) {
    List<ColumnData> result = new ArrayList<>();
    for (String fieldName : value.getFieldNames()) {
      String columnName = regionMapping.getColumnNameForField(fieldName, tableMetaData);
      if (tableMetaData.getKeyColumnName().equals(columnName)) {
        continue;
      }
      ColumnData columnData = new ColumnData(columnName, value.getField(fieldName),
          tableMetaData.getColumnDataType(columnName));
      result.add(columnData);
    }
    return result;
  }

  public interface DataSourceFactory {
    public DataSource getDataSource(String dataSourceName);
  }
}
