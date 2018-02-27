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
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;

@Experimental
public class SqlHandler {
  private final JdbcConnectorService configService;
  private final DataSourceManager manager;
  private final TableKeyColumnManager tableKeyColumnManager;

  public SqlHandler(DataSourceManager manager, JdbcConnectorService configService) {
    this(manager, new TableKeyColumnManager(), configService);
  }

  SqlHandler(DataSourceManager manager, TableKeyColumnManager tableKeyColumnManager,
      JdbcConnectorService configService) {
    this.manager = manager;
    this.tableKeyColumnManager = tableKeyColumnManager;
    this.configService = configService;
  }

  public void close() {
    manager.close();
  }

  Connection getConnection(ConnectionConfiguration config) {
    try {
      return manager.getDataSource(config).getConnection();
    } catch (SQLException e) {
      throw new IllegalStateException("Could not connect to " + config.getUrl(), e);
    }
  }

  public <K, V> PdxInstance read(Region<K, V> region, K key) {
    if (key == null) {
      throw new IllegalArgumentException("Key for query cannot be null");
    }

    RegionMapping regionMapping = getMappingForRegion(region.getName());
    ConnectionConfiguration connectionConfig =
        getConnectionConfig(regionMapping.getConnectionConfigName());
    String tableName = regionMapping.getRegionToTableName();
    PdxInstance result = null;
    try (Connection connection = getConnection(connectionConfig)) {
      List<ColumnValue> columnList =
          getColumnToValueList(connection, regionMapping, key, null, Operation.GET);
      try (PreparedStatement statement =
          getPreparedStatement(connection, columnList, tableName, Operation.GET, 0)) {
        String keyColumnName = getKeyColumnName(connection, tableName);
        result = executeReadStatement(region, statement, columnList, regionMapping, keyColumnName);
      }
    } catch (SQLException e) {
      handleSQLException(e);
    }
    return result;
  }

  private RegionMapping getMappingForRegion(String regionName) {
    RegionMapping regionMapping = this.configService.getMappingForRegion(regionName);
    if (regionMapping == null) {
      throw new IllegalStateException("JDBC mapping for region " + regionName
          + " not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    }
    return regionMapping;
  }

  private ConnectionConfiguration getConnectionConfig(String connectionConfigName) {
    ConnectionConfiguration connectionConfig =
        this.configService.getConnectionConfig(connectionConfigName);
    if (connectionConfig == null) {
      throw new IllegalStateException("JDBC connection with name " + connectionConfigName
          + " not found. Create the connection with the gfsh command 'create jdbc-connection'");
    }
    return connectionConfig;
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

  PdxInstance executeReadStatement(Region region, PreparedStatement statement,
      List<ColumnValue> columnList, RegionMapping regionMapping, String keyColumnName) {
    PdxInstanceFactory factory = getPdxInstanceFactory(region, regionMapping);
    PdxInstance pdxInstance = null;
    try {
      setValuesInStatement(statement, columnList);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          ResultSetMetaData metaData = resultSet.getMetaData();
          int ColumnsNumber = metaData.getColumnCount();
          for (int i = 1; i <= ColumnsNumber; i++) {
            String columnName = metaData.getColumnName(i);
            if (regionMapping.isPrimaryKeyInValue()
                || !keyColumnName.equalsIgnoreCase(columnName)) {
              String fieldName = mapColumnNameToFieldName(columnName, regionMapping);
              FieldType fieldType =
                  getFieldType(region, regionMapping.getPdxClassName(), fieldName, metaData, i);
              writeField(factory, resultSet, i, fieldName, fieldType);
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

  /**
   * @throws SQLException if the column value get fails
   */
  private void writeField(PdxInstanceFactory factory, ResultSet resultSet, int columnIndex,
      String fieldName, FieldType fieldType) throws SQLException {
    switch (fieldType) {
      case STRING:
        factory.writeString(fieldName, resultSet.getString(columnIndex));
        break;
      default:
        factory.writeObject(fieldName, resultSet.getObject(columnIndex));
    }
  }

  private FieldType getFieldType(Region region, String pdxClassName, String fieldName,
      ResultSetMetaData metaData, int columnIndex) {
    FieldType result = null;
    if (pdxClassName != null) {
      InternalCache cache = (InternalCache) region.getRegionService();
      PdxType pdxType = cache.getPdxRegistry().getPdxTypeForField(fieldName, pdxClassName);
      if (pdxType != null) {
        PdxField pdxField = pdxType.getPdxField(fieldName);
        if (pdxField != null) {
          result = pdxField.getFieldType();
        }
      }
    }
    if (result == null) {
      // TODO check metadata, for now make it object
      result = FieldType.OBJECT;
    }
    return result;
  }

  private void setValuesInStatement(PreparedStatement statement, List<ColumnValue> columnList)
      throws SQLException {
    int index = 0;
    for (ColumnValue columnValue : columnList) {
      index++;
      statement.setObject(index, columnValue.getValue());
    }
  }

  private String mapColumnNameToFieldName(String columnName, RegionMapping regionMapping) {
    return regionMapping.getFieldNameForColumn(columnName);
  }

  public <K, V> void write(Region<K, V> region, Operation operation, K key, PdxInstance value) {
    if (value == null && operation != Operation.DESTROY) {
      throw new IllegalArgumentException("PdxInstance cannot be null for non-destroy operations");
    }
    RegionMapping regionMapping = getMappingForRegion(region.getName());
    ConnectionConfiguration connectionConfig =
        getConnectionConfig(regionMapping.getConnectionConfigName());

    String tableName = regionMapping.getRegionToTableName();
    int pdxTypeId = value == null ? 0 : ((PdxInstanceImpl) value).getPdxType().getTypeId();

    try (Connection connection = getConnection(connectionConfig)) {
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
