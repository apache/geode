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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceImpl;

public class SqlHandler {
  private ConnectionManager manager;

  public SqlHandler(ConnectionManager manager) {
    this.manager = manager;
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

    List<ColumnValue> columnList =
        manager.getColumnToValueList(connectionConfig, regionMapping, key, null, Operation.GET);
    String tableName = regionMapping.getTableName();
    PreparedStatement statement = manager.getPreparedStatement(
        manager.getConnection(connectionConfig), columnList, tableName, Operation.GET, 0);
    PdxInstanceFactory factory = getPdxInstanceFactory(region, regionMapping);
    String keyColumnName = manager.getKeyColumnName(connectionConfig, tableName);
    return executeReadStatement(statement, columnList, factory, regionMapping, keyColumnName);
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
    synchronized (statement) {
      try {
        setValuesInStatement(statement, columnList);
        ResultSet resultSet = statement.executeQuery();
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
      } catch (SQLException e) {
        handleSQLException(e);
      } finally {
        clearStatementParameters(statement);
      }
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
    final String tableName = regionMapping.getTableName();
    ConnectionConfiguration connectionConfig =
        manager.getConnectionConfig(regionMapping.getConnectionConfigName());
    List<ColumnValue> columnList =
        manager.getColumnToValueList(connectionConfig, regionMapping, key, value, operation);

    int pdxTypeId = value == null ? 0 : ((PdxInstanceImpl) value).getPdxType().getTypeId();
    PreparedStatement statement = manager.getPreparedStatement(
        manager.getConnection(connectionConfig), columnList, tableName, operation, pdxTypeId);
    int updateCount = executeWriteStatement(statement, columnList, operation, false);

    // Destroy action not guaranteed to modify any database rows
    if (operation.isDestroy()) {
      return;
    }

    if (updateCount <= 0) {
      Operation upsertOp = getOppositeOperation(operation);
      PreparedStatement upsertStatement = manager.getPreparedStatement(
          manager.getConnection(connectionConfig), columnList, tableName, upsertOp, pdxTypeId);
      updateCount = executeWriteStatement(upsertStatement, columnList, upsertOp, true);
    }

    if (updateCount != 1) {
      throw new IllegalStateException("Unexpected updateCount " + updateCount);
    }
  }

  private Operation getOppositeOperation(Operation operation) {
    return operation.isUpdate() ? Operation.CREATE : Operation.UPDATE;
  }

  private int executeWriteStatement(PreparedStatement statement, List<ColumnValue> columnList,
      Operation operation, boolean handleException) {
    int updateCount = 0;
    synchronized (statement) {
      try {
        setValuesInStatement(statement, columnList);
        updateCount = statement.executeUpdate();
      } catch (SQLException e) {
        if (handleException || operation.isDestroy()) {
          handleSQLException(e);
        }
      } finally {
        clearStatementParameters(statement);
      }
    }
    return updateCount;
  }

  private void clearStatementParameters(PreparedStatement statement) {
    try {
      statement.clearParameters();
    } catch (SQLException ignore) {
    }
  }

  private void handleSQLException(SQLException e) {
    throw new IllegalStateException("NYI: handleSQLException", e);
  }
}
