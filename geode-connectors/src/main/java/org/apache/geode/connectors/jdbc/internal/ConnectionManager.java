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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.cache.Operation;
import org.apache.geode.pdx.PdxInstance;

class ConnectionManager {

  private final InternalJdbcConnectorService configService;
  private final Map<String, Connection> connectionMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> tableToPrimaryKeyMap = new ConcurrentHashMap<>();
  private final ThreadLocal<PreparedStatementCache> preparedStatementCache = new ThreadLocal<>();

  ConnectionManager(InternalJdbcConnectorService configService) {
    this.configService = configService;
  }

  RegionMapping getMappingForRegion(String regionName) {
    return configService.getMappingForRegion(regionName);
  }

  Connection getConnection(ConnectionConfiguration config) {
    Connection connection = connectionMap.get(config.getName());
    try {
      if (connection != null && !connection.isClosed()) {
        return connection;
      }
    } catch (SQLException ignore) {
      // If isClosed throws fall through and connect again
    }
    return getNewConnection(config);
  }

  <K> List<ColumnValue> getColumnToValueList(ConnectionConfiguration config,
      RegionMapping regionMapping, K key, PdxInstance value, Operation operation) {
    String keyColumnName = getKeyColumnName(config, regionMapping.getTableName());
    ColumnValue keyColumnValue = new ColumnValue(true, keyColumnName, key);

    if (operation.isDestroy() || operation.isGet()) {
      return Collections.singletonList(keyColumnValue);
    }

    List<ColumnValue> result = createColumnValueList(regionMapping, value, keyColumnName);
    result.add(keyColumnValue);
    return result;
  }

  void close() {
    connectionMap.values().forEach(this::close);
  }

  String getKeyColumnName(ConnectionConfiguration connectionConfig, String tableName) {
    return tableToPrimaryKeyMap.computeIfAbsent(tableName,
        k -> computeKeyColumnName(connectionConfig, k));
  }

  ConnectionConfiguration getConnectionConfig(String connectionConfigName) {
    return configService.getConnectionConfig(connectionConfigName);
  }

  PreparedStatement getPreparedStatement(Connection connection, List<ColumnValue> columnList,
      String tableName, Operation operation, int pdxTypeId) {
    PreparedStatementCache statementCache = preparedStatementCache.get();

    if (statementCache == null) {
      statementCache = new PreparedStatementCache();
      preparedStatementCache.set(statementCache);
    }

    return statementCache.getPreparedStatement(connection, columnList, tableName, operation,
        pdxTypeId);
  }

  // package protected for testing purposes only
  Connection getSQLConnection(ConnectionConfiguration config) throws SQLException {
    return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
  }

  private synchronized Connection getNewConnection(ConnectionConfiguration config) {
    Connection connection;
    try {
      connection = getSQLConnection(config);
    } catch (SQLException e) {
      // TODO: consider a different exception
      throw new IllegalStateException("Could not connect to " + config.getUrl(), e);
    }
    connectionMap.put(config.getName(), connection);
    return connection;
  }

  private List<ColumnValue> createColumnValueList(RegionMapping regionMapping, PdxInstance value,
      String keyColumnName) {
    List<ColumnValue> result = new ArrayList<>();
    for (String fieldName : value.getFieldNames()) {
      String columnName = regionMapping.getColumnNameForField(fieldName);
      if (columnName.equalsIgnoreCase(keyColumnName)) {
        continue;
      }
      ColumnValue columnValue = new ColumnValue(false, columnName, value.getField(fieldName));
      result.add(columnValue);
    }
    return result;
  }

  private String computeKeyColumnName(ConnectionConfiguration connectionConfig, String tableName) {
    // TODO: check config for key column
    String key = null;
    try {
      Connection connection = getConnection(connectionConfig);
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet tables = metaData.getTables(null, null, "%", null);

      String realTableName = getTableNameFromMetaData(tableName, tables);
      key = getPrimaryKeyColumnNameFromMetaData(realTableName, metaData);

    } catch (SQLException e) {
      handleSQLException(e);
    }
    return key;
  }

  private String getTableNameFromMetaData(String tableName, ResultSet tables) throws SQLException {
    String realTableName = null;
    while (tables.next()) {
      String name = tables.getString("TABLE_NAME");
      if (name.equalsIgnoreCase(tableName)) {
        if (realTableName != null) {
          throw new IllegalStateException("Duplicate tables that match region name");
        }
        realTableName = name;
      }
    }

    if (realTableName == null) {
      throw new IllegalStateException("no table was found that matches " + tableName);
    }
    return realTableName;
  }

  private String getPrimaryKeyColumnNameFromMetaData(String tableName, DatabaseMetaData metaData)
      throws SQLException {
    ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);
    if (!primaryKeys.next()) {
      throw new IllegalStateException(
          "The table " + tableName + " does not have a primary key column.");
    }
    String key = primaryKeys.getString("COLUMN_NAME");
    if (primaryKeys.next()) {
      throw new IllegalStateException(
          "The table " + tableName + " has more than one primary key column.");
    }
    return key;
  }

  private void handleSQLException(SQLException e) {
    throw new IllegalStateException("NYI: handleSQLException", e);
  }

  private void close(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ignore) {
      }
    }
  }
}
