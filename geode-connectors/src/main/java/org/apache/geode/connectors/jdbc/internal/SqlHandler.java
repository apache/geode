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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceImpl;

@Experimental
public class SqlHandler {
  private final ConnectionManager manager;
  private final TableKeyColumnManager tableKeyColumnManager;

  public SqlHandler(ConnectionManager manager) {
    this(manager, new TableKeyColumnManager());
  }

  SqlHandler(ConnectionManager manager, TableKeyColumnManager tableKeyColumnManager) {
    this.manager = manager;
    this.tableKeyColumnManager = tableKeyColumnManager;
  }

  public void close() {
    manager.close();
  }

  public <K, V> PdxInstance read(Region<K, V> region, K key) {
    if (key == null) {
      throw new IllegalArgumentException("Key for query cannot be null");
    }

    RegionMapping regionMapping = manager.getMappingForRegion(region.getName());
    ConnectionConfiguration connectionConfig =
        manager.getConnectionConfig(regionMapping.getConnectionConfigName());

    String tableName = regionMapping.getRegionToTableName();
    PdxInstance result = null;
    try (Connection connection = manager.getConnection(connectionConfig)) {
      List<ColumnValue> columnList =
          getColumnToValueList(connection, regionMapping, key, null, Operation.GET);
      try (PreparedStatement statement =
          getPreparedStatement(connection, columnList, tableName, Operation.GET, 0)) {
        PdxInstanceFactory factory = getPdxInstanceFactory(region, regionMapping);
        String keyColumnName = getKeyColumnName(connection, tableName);
        result = executeReadStatement(statement, columnList, factory, regionMapping, keyColumnName);
      }
    } catch (SQLException e) {
      handleSQLException(e);
    }
    return result;
  }

  private String getKeyColumnName(Connection connection, String tableName) {
    return this.tableKeyColumnManager.getKeyColumnName(connection, tableName);
  }

  private <K, V> PdxInstanceFactory getPdxInstanceFactory(Region<K, V> region,
      RegionMapping regionMapping) {
    InternalCache cache = (InternalCache) region.getRegionService();
    String valueClassName = regionMapping.getPdxClassName();
    PdxInstanceFactory factory;
    if (valueClassName != null) {
      factory = cache.createPdxInstanceFactory(valueClassName);
    } else {
      factory = cache.createPdxInstanceFactory("no class", false);
    }
    return factory;
  }

  private PdxInstance executeReadStatement(PreparedStatement statement,
      List<ColumnValue> columnList, PdxInstanceFactory factory, RegionMapping regionMapping,
      String keyColumnName) {
    PdxInstance pdxInstance = null;
    try {
      setValuesInStatement(statement, columnList);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          ResultSetMetaData metaData = resultSet.getMetaData();
          int ColumnsNumber = metaData.getColumnCount();
          for (int i = 1; i <= ColumnsNumber; i++) {
            Object columnValue = resultSet.getObject(i);
            String columnName = metaData.getColumnName(i);
            String fieldName = mapColumnNameToFieldName(columnName);
            if (regionMapping.isPrimaryKeyInValue()
                || !keyColumnName.equalsIgnoreCase(columnName)) {
              factory.writeField(fieldName, columnValue, Object.class);
            }
          }
          if (resultSet.next()) {
            throw new IllegalStateException(
                "Multiple rows returned for query: " + resultSet.getStatement().toString());
          }
          pdxInstance = factory.create();
        }
      }
    } catch (SQLException e) {
      handleSQLException(e);
    }
    return pdxInstance;
  }

  private void setValuesInStatement(PreparedStatement statement, List<ColumnValue> columnList)
      throws SQLException {
    int index = 0;
    for (ColumnValue columnValue : columnList) {
      index++;
      statement.setObject(index, columnValue.getValue());
    }
  }

  private String mapColumnNameToFieldName(String columnName) {
    return columnName.toLowerCase();
  }

  public <K, V> void write(Region<K, V> region, Operation operation, K key, PdxInstance value) {
    if (value == null && operation != Operation.DESTROY) {
      throw new IllegalArgumentException("PdxInstance cannot be null for non-destroy operations");
    }
    RegionMapping regionMapping = manager.getMappingForRegion(region.getName());

    if (regionMapping == null) {
      throw new IllegalStateException(
          "JDBC write failed. JDBC mapping for region " + region.getFullPath()
              + " not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    }
    ConnectionConfiguration connectionConfig =
        manager.getConnectionConfig(regionMapping.getConnectionConfigName());
    if (connectionConfig == null) {
      throw new IllegalStateException(
          "JDBC write failed. JDBC connection with name " + regionMapping.getConnectionConfigName()
              + " not found. Create the connection with the gfsh command 'create jdbc-connection'");
    }

    String tableName = regionMapping.getRegionToTableName();
    int pdxTypeId = value == null ? 0 : ((PdxInstanceImpl) value).getPdxType().getTypeId();

    try (Connection connection = manager.getConnection(connectionConfig)) {
      List<ColumnValue> columnList =
          getColumnToValueList(connection, regionMapping, key, value, operation);
      int updateCount = 0;
      try (PreparedStatement statement =
          getPreparedStatement(connection, columnList, tableName, operation, pdxTypeId)) {
        updateCount = executeWriteStatement(statement, columnList, operation, false);
      }

      // Destroy action not guaranteed to modify any database rows
      if (operation.isDestroy()) {
        return;
      }

      if (updateCount <= 0) {
        Operation upsertOp = getOppositeOperation(operation);
        try (PreparedStatement upsertStatement =
            getPreparedStatement(connection, columnList, tableName, upsertOp, pdxTypeId)) {
          updateCount = executeWriteStatement(upsertStatement, columnList, upsertOp, true);
        }
      }

      if (updateCount != 1) {
        throw new IllegalStateException("Unexpected updateCount " + updateCount);
      }
    } catch (SQLException e) {
      handleSQLException(e);
    }
  }

  private Operation getOppositeOperation(Operation operation) {
    return operation.isUpdate() ? Operation.CREATE : Operation.UPDATE;
  }

  private int executeWriteStatement(PreparedStatement statement, List<ColumnValue> columnList,
      Operation operation, boolean handleException) {
    int updateCount = 0;
    try {
      setValuesInStatement(statement, columnList);
      updateCount = statement.executeUpdate();
    } catch (SQLException e) {
      if (handleException || operation.isDestroy()) {
        handleSQLException(e);
      }
    }
    return updateCount;
  }

  private PreparedStatement getPreparedStatement(Connection connection,
      List<ColumnValue> columnList, String tableName, Operation operation, int pdxTypeId) {
    String sqlStr = getSqlString(tableName, columnList, operation);
    PreparedStatement statement = null;
    try {
      statement = connection.prepareStatement(sqlStr);
    } catch (SQLException e) {
      handleSQLException(e);
    }
    return statement;
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
      throw new IllegalArgumentException("unsupported operation " + operation);
    }
  }

  <K> List<ColumnValue> getColumnToValueList(Connection connection, RegionMapping regionMapping,
      K key, PdxInstance value, Operation operation) {
    String tableName = regionMapping.getRegionToTableName();
    String keyColumnName = getKeyColumnName(connection, tableName);
    ColumnValue keyColumnValue = new ColumnValue(true, keyColumnName, key);

    if (operation.isDestroy() || operation.isGet()) {
      return Collections.singletonList(keyColumnValue);
    }

    List<ColumnValue> result = createColumnValueList(regionMapping, value, keyColumnName);
    result.add(keyColumnValue);
    return result;
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

  static void handleSQLException(SQLException e) {
    throw new IllegalStateException("JDBC connector detected unexpected SQLException", e);
  }
}
