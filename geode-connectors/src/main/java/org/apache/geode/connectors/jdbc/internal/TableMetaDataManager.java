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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;

/**
 * Given a tableName this manager will determine which column should correspond to the Geode Region
 * key. The current implementation uses a connection to lookup the SQL metadata for the table and
 * find a single column that is a primary key on that table. If the table was configured with more
 * than one column as a primary key or no columns then an exception is thrown. The computation is
 * remembered so that it does not need to be recomputed for the same table name.
 */
public class TableMetaDataManager {
  private final ConcurrentMap<String, TableMetaData> tableToMetaDataMap = new ConcurrentHashMap<>();

  public TableMetaData getTableMetaData(Connection connection, String tableName) {
    return tableToMetaDataMap.computeIfAbsent(tableName, k -> computeTableMetaData(connection, k));
  }

  private TableMetaData computeTableMetaData(Connection connection, String tableName) {
    TableMetaDataImpl result;
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet tables = metaData.getTables(null, null, "%", null)) {
        String realTableName = getTableNameFromMetaData(tableName, tables);
        String key = getPrimaryKeyColumnNameFromMetaData(realTableName, metaData);
        result = new TableMetaDataImpl(key);
        getDataTypesFromMetaData(realTableName, metaData, result);
      }
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
    return result;
  }

  private String getTableNameFromMetaData(String tableName, ResultSet tables) throws SQLException {
    String realTableName = null;
    while (tables.next()) {
      String name = tables.getString("TABLE_NAME");
      if (name.equalsIgnoreCase(tableName)) {
        if (realTableName != null) {
          throw new JdbcConnectorException("Duplicate tables that match region name");
        }
        realTableName = name;
      }
    }

    if (realTableName == null) {
      throw new JdbcConnectorException("no table was found that matches " + tableName);
    }
    return realTableName;
  }

  private String getPrimaryKeyColumnNameFromMetaData(String tableName, DatabaseMetaData metaData)
      throws SQLException {
    ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);
    if (!primaryKeys.next()) {
      throw new JdbcConnectorException(
          "The table " + tableName + " does not have a primary key column.");
    }
    String key = primaryKeys.getString("COLUMN_NAME");
    if (primaryKeys.next()) {
      throw new JdbcConnectorException(
          "The table " + tableName + " has more than one primary key column.");
    }
    return key;
  }

  private void getDataTypesFromMetaData(String tableName, DatabaseMetaData metaData, TableMetaDataImpl result)
      throws SQLException {
    ResultSet columnData = metaData.getColumns(null, null, tableName, "%");
    while(columnData.next()) {
      String columnName = columnData.getString("COLUMN_NAME");
      int dataType = columnData.getInt("DATA_TYPE");
      result.addDataType(columnName, dataType);
    }
  }
}
