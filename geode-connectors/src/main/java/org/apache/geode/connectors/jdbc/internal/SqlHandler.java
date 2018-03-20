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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;

@Experimental
public class SqlHandler {
  private final JdbcConnectorService configService;
  private final DataSourceManager manager;
  private final TableMetaDataManager tableMetaDataManager;

  public SqlHandler(DataSourceManager manager, TableMetaDataManager tableMetaDataManager,
      JdbcConnectorService configService) {
    this.manager = manager;
    this.tableMetaDataManager = tableMetaDataManager;
    this.configService = configService;
  }

  public void close() {
    manager.close();
  }

  Connection getConnection(ConnectionConfiguration config) throws SQLException {
    return manager.getDataSource(config).getConnection();
  }

  public <K, V> PdxInstance read(Region<K, V> region, K key) throws SQLException {
    if (key == null) {
      throw new IllegalArgumentException("Key for query cannot be null");
    }

    RegionMapping regionMapping = getMappingForRegion(region.getName());
    ConnectionConfiguration connectionConfig =
        getConnectionConfig(regionMapping.getConnectionConfigName());
    String tableName = regionMapping.getRegionToTableName();
    PdxInstance result;
    try (Connection connection = getConnection(connectionConfig)) {
      List<ColumnValue> columnList =
          getColumnToValueList(connection, regionMapping, key, null, Operation.GET);
      try (PreparedStatement statement =
          getPreparedStatement(connection, columnList, tableName, Operation.GET)) {
        try (ResultSet resultSet = executeReadQuery(statement, columnList)) {
          String keyColumnName = getKeyColumnName(connection, tableName);
          InternalCache cache = (InternalCache) region.getRegionService();
          SqlToPdxInstanceCreator sqlToPdxInstanceCreator =
              new SqlToPdxInstanceCreator(cache, regionMapping, resultSet, keyColumnName);
          result = sqlToPdxInstanceCreator.create();
        }
      }
    }
    return result;
  }

  private ResultSet executeReadQuery(PreparedStatement statement, List<ColumnValue> columnList)
      throws SQLException {
    setValuesInStatement(statement, columnList);
    return statement.executeQuery();
  }


  private RegionMapping getMappingForRegion(String regionName) {
    RegionMapping regionMapping = this.configService.getMappingForRegion(regionName);
    if (regionMapping == null) {
      throw new JdbcConnectorException("JDBC mapping for region " + regionName
          + " not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    }
    return regionMapping;
  }

  private ConnectionConfiguration getConnectionConfig(String connectionConfigName) {
    ConnectionConfiguration connectionConfig =
        this.configService.getConnectionConfig(connectionConfigName);
    if (connectionConfig == null) {
      throw new JdbcConnectorException("JDBC connection with name " + connectionConfigName
          + " not found. Create the connection with the gfsh command 'create jdbc-connection'");
    }
    return connectionConfig;
  }

  private String getKeyColumnName(Connection connection, String tableName) {
    return this.tableMetaDataManager.getTableMetaDataView(connection, tableName).getKeyColumnName();
  }

  private void setValuesInStatement(PreparedStatement statement, List<ColumnValue> columnList)
      throws SQLException {
    int index = 0;
    for (ColumnValue columnValue : columnList) {
      index++;
      Object value = columnValue.getValue();
      if (value instanceof Character) {
        value = ((Character) value).toString();
      }
      if (value == null) {
        statement.setNull(index, columnValue.getDataType());
      } else {
        statement.setObject(index, value);
      }
    }
  }

  public <K, V> void write(Region<K, V> region, Operation operation, K key, PdxInstance value)
      throws SQLException {
    if (value == null && operation != Operation.DESTROY) {
      throw new IllegalArgumentException("PdxInstance cannot be null for non-destroy operations");
    }
    RegionMapping regionMapping = getMappingForRegion(region.getName());
    ConnectionConfiguration connectionConfig =
        getConnectionConfig(regionMapping.getConnectionConfigName());

    String tableName = regionMapping.getRegionToTableName();

    try (Connection connection = getConnection(connectionConfig)) {
      List<ColumnValue> columnList =
          getColumnToValueList(connection, regionMapping, key, value, operation);
      int updateCount = 0;
      try (PreparedStatement statement =
          getPreparedStatement(connection, columnList, tableName, operation)) {
        updateCount = executeWriteStatement(statement, columnList);
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
            getPreparedStatement(connection, columnList, tableName, upsertOp)) {
          updateCount = executeWriteStatement(upsertStatement, columnList);
        }
      }

      assert updateCount == 1;
    }
  }

  private Operation getOppositeOperation(Operation operation) {
    return operation.isUpdate() ? Operation.CREATE : Operation.UPDATE;
  }

  private int executeWriteStatement(PreparedStatement statement, List<ColumnValue> columnList)
      throws SQLException {
    setValuesInStatement(statement, columnList);
    return statement.executeUpdate();
  }

  private PreparedStatement getPreparedStatement(Connection connection,
      List<ColumnValue> columnList, String tableName, Operation operation) throws SQLException {
    String sqlStr = getSqlString(tableName, columnList, operation);
    return connection.prepareStatement(sqlStr);
  }

  private String getSqlString(String tableName, List<ColumnValue> columnList, Operation operation) {
    SqlStatementFactory statementFactory = new SqlStatementFactory();
    if (operation.isCreate()) {
      return statementFactory.createInsertSqlString(tableName, columnList);
    } else if (operation.isUpdate()) {
      return statementFactory.createUpdateSqlString(tableName, columnList);
    } else if (operation.isDestroy()) {
      return statementFactory.createDestroySqlString(tableName, columnList);
    } else if (operation.isGet()) {
      return statementFactory.createSelectQueryString(tableName, columnList);
    } else {
      throw new InternalGemFireException("unsupported operation " + operation);
    }
  }

  <K> List<ColumnValue> getColumnToValueList(Connection connection, RegionMapping regionMapping,
      K key, PdxInstance value, Operation operation) {
    String tableName = regionMapping.getRegionToTableName();
    TableMetaDataView tableMetaData =
        this.tableMetaDataManager.getTableMetaDataView(connection, tableName);
    String keyColumnName = tableMetaData.getKeyColumnName();
    ColumnValue keyColumnValue =
        new ColumnValue(true, keyColumnName, key, tableMetaData.getColumnDataType(keyColumnName));

    if (operation.isDestroy() || operation.isGet()) {
      return Collections.singletonList(keyColumnValue);
    }

    List<ColumnValue> result = createColumnValueList(tableMetaData, regionMapping, value);
    result.add(keyColumnValue);
    return result;
  }

  private List<ColumnValue> createColumnValueList(TableMetaDataView tableMetaData,
      RegionMapping regionMapping, PdxInstance value) {
    final String keyColumnName = tableMetaData.getKeyColumnName();
    List<ColumnValue> result = new ArrayList<>();
    for (String fieldName : value.getFieldNames()) {
      String columnName = regionMapping.getColumnNameForField(fieldName, tableMetaData);
      if (columnName.equalsIgnoreCase(keyColumnName)) {
        continue;
      }
      ColumnValue columnValue = new ColumnValue(false, columnName, value.getField(fieldName),
          tableMetaData.getColumnDataType(columnName));
      result.add(columnValue);
    }
    return result;
  }

}
