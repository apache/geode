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
  private final Map<String, JdbcDataSource> dataSourceMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> tableToPrimaryKeyMap = new ConcurrentHashMap<>();

  ConnectionManager(InternalJdbcConnectorService configService) {
    this.configService = configService;
  }

  RegionMapping getMappingForRegion(String regionName) {
    return configService.getMappingForRegion(regionName);
  }

  JdbcDataSource buildJdbcDataSource(ConnectionConfiguration config) {
    return new JdbcDataSourceBuilder(config).create();
  }

  private JdbcDataSource getDataSource(ConnectionConfiguration config) {
    return dataSourceMap.computeIfAbsent(config.getName(), k -> {
      return buildJdbcDataSource(config);
    });
  }

  Connection getConnection(ConnectionConfiguration config) {
    try {
      return getDataSource(config).getConnection();
    } catch (SQLException e) {
      throw new IllegalStateException("Could not connect to " + config.getUrl(), e);
    }
  }

  <K> List<ColumnValue> getColumnToValueList(ConnectionConfiguration config,
      RegionMapping regionMapping, K key, PdxInstance value, Operation operation) {
    String tableName = regionMapping.getRegionToTableName();
    String keyColumnName = getKeyColumnName(config, tableName);
    ColumnValue keyColumnValue = new ColumnValue(true, keyColumnName, key);

    if (operation.isDestroy() || operation.isGet()) {
      return Collections.singletonList(keyColumnValue);
    }

    List<ColumnValue> result = createColumnValueList(regionMapping, value, keyColumnName);
    result.add(keyColumnValue);
    return result;
  }

  void close() {
    dataSourceMap.values().forEach(this::close);
  }

  String getKeyColumnName(ConnectionConfiguration connectionConfig, String tableName) {
    return tableToPrimaryKeyMap.computeIfAbsent(tableName,
        k -> computeKeyColumnName(connectionConfig, k));
  }

  ConnectionConfiguration getConnectionConfig(String connectionConfigName) {
    return configService.getConnectionConfig(connectionConfigName);
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
    String key = null;
    try (Connection connection = getConnection(connectionConfig)) {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet tables = metaData.getTables(null, null, "%", null)) {
        String realTableName = getTableNameFromMetaData(tableName, tables);
        key = getPrimaryKeyColumnNameFromMetaData(realTableName, metaData);
      }
    } catch (SQLException e) {
      SqlHandler.handleSQLException(e);
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

  private void close(JdbcDataSource dataSource) {
    if (dataSource != null) {
      try {
        dataSource.close();
      } catch (Exception e) {
        // TODO ignored for now; should it be logged?
      }
    }
  }
}
