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
import java.util.Arrays;
import java.util.List;
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
  private final ConcurrentMap<String, TableMetaDataView> tableToMetaDataMap =
      new ConcurrentHashMap<>();

  public TableMetaDataView getTableMetaDataView(Connection connection, String tableName,
      String ids) {
    return tableToMetaDataMap.computeIfAbsent(tableName,
        k -> computeTableMetaDataView(connection, k, ids));
  }

  private TableMetaDataView computeTableMetaDataView(Connection connection, String tableName,
      String ids) {
    TableMetaData result;
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet tables = metaData.getTables(null, null, "%", null)) {
        String realTableName = getTableNameFromMetaData(tableName, tables);
        List<String> keys = getPrimaryKeyColumnNamesFromMetaData(realTableName, metaData, ids);
        String quoteString = metaData.getIdentifierQuoteString();
        if (quoteString == null) {
          quoteString = "";
        }
        result = new TableMetaData(realTableName, keys, quoteString);
        getDataTypesFromMetaData(realTableName, metaData, result);
      }
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
    return result;
  }

  private String getTableNameFromMetaData(String tableName, ResultSet tables) throws SQLException {
    String result = null;
    int inexactMatches = 0;

    while (tables.next()) {
      String name = tables.getString("TABLE_NAME");
      if (name.equals(tableName)) {
        return name;
      } else if (name.equalsIgnoreCase(tableName)) {
        inexactMatches++;
        result = name;
      }
    }

    if (inexactMatches > 1) {
      throw new JdbcConnectorException("Duplicate tables that match region name");
    }

    if (result == null) {
      throw new JdbcConnectorException("no table was found that matches " + tableName);
    }
    return result;
  }

  private List<String> getPrimaryKeyColumnNamesFromMetaData(String tableName,
      DatabaseMetaData metaData,
      String ids)
      throws SQLException {
    List<String> keys = new ArrayList<>();

    if (ids != null && !ids.isEmpty()) {
      keys.addAll(Arrays.asList(ids.split(",")));
      for (String key : keys) {
        checkColumnExistsInTable(tableName, metaData, key);
      }
    } else {
      try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName)) {
        while (primaryKeys.next()) {
          String key = primaryKeys.getString("COLUMN_NAME");
          keys.add(key);
        }
        if (keys.isEmpty()) {
          throw new JdbcConnectorException(
              "The table " + tableName + " does not have a primary key column.");
        }
      }
    }
    return keys;
  }

  private void getDataTypesFromMetaData(String tableName, DatabaseMetaData metaData,
      TableMetaData result) throws SQLException {
    try (ResultSet columnData = metaData.getColumns(null, null, tableName, "%")) {
      while (columnData.next()) {
        String columnName = columnData.getString("COLUMN_NAME");
        int dataType = columnData.getInt("DATA_TYPE");
        result.addDataType(columnName, dataType);
      }
    }
  }

  private void checkColumnExistsInTable(String tableName, DatabaseMetaData metaData,
      String columnName) throws SQLException {
    int caseInsensitiveMatches = 0;
    try (ResultSet columnData = metaData.getColumns(null, null, tableName, "%")) {
      while (columnData.next()) {
        String realColumnName = columnData.getString("COLUMN_NAME");
        if (columnName.equals(realColumnName)) {
          return;
        } else if (columnName.equalsIgnoreCase(realColumnName)) {
          caseInsensitiveMatches++;
        }
      }
    }
    if (caseInsensitiveMatches > 1) {
      throw new JdbcConnectorException(
          "The table " + tableName + " has more than one column that matches " + columnName);
    } else if (caseInsensitiveMatches == 0) {
      throw new JdbcConnectorException(
          "The table " + tableName + " does not have a column named " + columnName);
    }
  }
}
