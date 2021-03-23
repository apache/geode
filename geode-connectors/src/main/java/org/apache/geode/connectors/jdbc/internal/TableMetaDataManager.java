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
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.TableMetaData.ColumnMetaData;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.lang.utils.JavaWorkarounds;

/**
 * Given a tableName this manager will determine which column should correspond to the Geode Region
 * key. The current implementation uses a connection to lookup the SQL metadata for the table and
 * find a single column that is a primary key on that table. If the table was configured with more
 * than one column as a primary key or no columns then an exception is thrown. The computation is
 * remembered so that it does not need to be recomputed for the same table name.
 */
public class TableMetaDataManager {
  private static final String DEFAULT_CATALOG = "";
  private static final String DEFAULT_SCHEMA = "";
  private final ConcurrentMap<String, TableMetaDataView> tableToMetaDataMap =
      new ConcurrentHashMap<>();

  public TableMetaDataView getTableMetaDataView(Connection connection,
      RegionMapping regionMapping) {
    return JavaWorkarounds.computeIfAbsent(tableToMetaDataMap, computeTableName(regionMapping),
        k -> computeTableMetaDataView(connection, k, regionMapping));
  }

  /**
   * If the region mapping has been given a table name then return it.
   * Otherwise return the region mapping's region name as the table name.
   */
  String computeTableName(RegionMapping regionMapping) {
    String result = regionMapping.getTableName();
    if (result == null) {
      result = regionMapping.getRegionName();
    }
    return result;
  }

  private TableMetaDataView computeTableMetaDataView(Connection connection,
      String tableName, RegionMapping regionMapping) {
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      String realCatalogName = getCatalogNameFromMetaData(metaData, regionMapping);
      String realSchemaName = getSchemaNameFromMetaData(metaData, regionMapping, realCatalogName);
      String realTableName =
          getTableNameFromMetaData(metaData, realCatalogName, realSchemaName, tableName);
      List<String> keys = getPrimaryKeyColumnNamesFromMetaData(metaData, realCatalogName,
          realSchemaName, realTableName, regionMapping.getIds());
      String quoteString = metaData.getIdentifierQuoteString();
      Map<String, ColumnMetaData> columnMetaDataMap =
          createColumnMetaDataMap(metaData, realCatalogName, realSchemaName, realTableName);
      return new TableMetaData(realCatalogName, realSchemaName, realTableName, keys, quoteString,
          columnMetaDataMap);
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
  }

  String getCatalogNameFromMetaData(DatabaseMetaData metaData, RegionMapping regionMapping)
      throws SQLException {
    String catalogFilter = regionMapping.getCatalog();
    if (catalogFilter == null || catalogFilter.isEmpty()) {
      return DEFAULT_CATALOG;
    }
    try (ResultSet catalogs = metaData.getCatalogs()) {
      return findMatchInResultSet(catalogFilter, catalogs, "TABLE_CAT", "catalog");
    }
  }

  String getSchemaNameFromMetaData(DatabaseMetaData metaData, RegionMapping regionMapping,
      String catalogFilter) throws SQLException {
    String schemaFilter = regionMapping.getSchema();
    if (schemaFilter == null || schemaFilter.isEmpty()) {
      if ("PostgreSQL".equals(metaData.getDatabaseProductName())) {
        schemaFilter = "public";
      } else {
        return DEFAULT_SCHEMA;
      }
    }
    try (ResultSet schemas = metaData.getSchemas(catalogFilter, "%")) {
      return findMatchInResultSet(schemaFilter, schemas, "TABLE_SCHEM", "schema");
    }
  }

  private String getTableNameFromMetaData(DatabaseMetaData metaData, String catalogFilter,
      String schemaFilter, String tableName) throws SQLException {
    try (ResultSet tables = metaData.getTables(catalogFilter, schemaFilter, "%", null)) {
      return findMatchInResultSet(tableName, tables, "TABLE_NAME", "table");
    }
  }

  String findMatchInResultSet(String stringToFind, ResultSet resultSet, String column,
      String description)
      throws SQLException {
    int exactMatches = 0;
    String exactMatch = null;
    int inexactMatches = 0;
    String inexactMatch = null;
    if (resultSet != null) {
      while (resultSet.next()) {
        String name = resultSet.getString(column);
        if (name.equals(stringToFind)) {
          exactMatches++;
          exactMatch = name;
        } else if (name.equalsIgnoreCase(stringToFind)) {
          inexactMatches++;
          inexactMatch = name;
        }
      }
    }
    if (exactMatches == 1) {
      return exactMatch;
    }
    if (inexactMatches > 1 || exactMatches > 1) {
      throw new JdbcConnectorException(
          "Multiple " + description + "s were found that match \"" + stringToFind + '"');
    }
    if (inexactMatches == 1) {
      return inexactMatch;
    }
    throw new JdbcConnectorException(
        "No " + description + " was found that matches \"" + stringToFind + '"');
  }

  private List<String> getPrimaryKeyColumnNamesFromMetaData(DatabaseMetaData metaData,
      String catalogFilter, String schemaFilter, String tableName,
      String ids)
      throws SQLException {
    List<String> keys = new ArrayList<>();

    if (ids != null && !ids.isEmpty()) {
      keys.addAll(Arrays.asList(ids.split(",")));
      for (String key : keys) {
        checkColumnExistsInTable(tableName, metaData, catalogFilter, schemaFilter, key);
      }
    } else {
      try (
          ResultSet primaryKeys =
              metaData.getPrimaryKeys(catalogFilter, schemaFilter, tableName)) {
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

  private Map<String, ColumnMetaData> createColumnMetaDataMap(DatabaseMetaData metaData,
      String catalogFilter,
      String schemaFilter, String tableName) throws SQLException {
    Map<String, ColumnMetaData> result = new HashMap<>();
    try (ResultSet columnData =
        metaData.getColumns(catalogFilter, schemaFilter, tableName, "%")) {
      while (columnData.next()) {
        String columnName = columnData.getString("COLUMN_NAME");
        int dataType = columnData.getInt("DATA_TYPE");
        int nullableCode = columnData.getInt("NULLABLE");
        boolean nullable = nullableCode != DatabaseMetaData.columnNoNulls;
        result.put(columnName, new ColumnMetaData(JDBCType.valueOf(dataType), nullable));
      }
    }
    return result;
  }

  private void checkColumnExistsInTable(String tableName, DatabaseMetaData metaData,
      String catalogFilter, String schemaFilter, String columnName) throws SQLException {
    int caseInsensitiveMatches = 0;
    try (ResultSet columnData =
        metaData.getColumns(catalogFilter, schemaFilter, tableName, "%")) {
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
