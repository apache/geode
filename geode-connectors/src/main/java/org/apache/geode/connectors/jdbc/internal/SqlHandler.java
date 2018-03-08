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

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;

@Experimental
public class SqlHandler {
  private final JdbcConnectorService configService;
  private final DataSourceManager manager;
  private final TableKeyColumnManager tableKeyColumnManager;

  public SqlHandler(DataSourceManager manager, TableKeyColumnManager tableKeyColumnManager,
      JdbcConnectorService configService) {
    this.manager = manager;
    this.tableKeyColumnManager = tableKeyColumnManager;
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
          result = createPdxInstance(resultSet, region, regionMapping, keyColumnName);
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

  <K, V> PdxInstance createPdxInstance(ResultSet resultSet, Region<K, V> region,
      RegionMapping regionMapping, String keyColumnName) throws SQLException {
    PdxInstanceFactory factory = getPdxInstanceFactory(region, regionMapping);
    PdxInstance pdxInstance = null;
    if (resultSet.next()) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int ColumnsNumber = metaData.getColumnCount();
      for (int i = 1; i <= ColumnsNumber; i++) {
        String columnName = metaData.getColumnName(i);
        if (regionMapping.isPrimaryKeyInValue() || !keyColumnName.equalsIgnoreCase(columnName)) {
          String fieldName = mapColumnNameToFieldName(columnName, regionMapping);
          FieldType fieldType = getFieldType(region, regionMapping.getPdxClassName(), fieldName);
          writeField(factory, resultSet, i, fieldName, fieldType);
        }
      }
      if (resultSet.next()) {
        throw new JdbcConnectorException(
            "Multiple rows returned for query: " + resultSet.getStatement().toString());
      }
      pdxInstance = factory.create();
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
      case CHAR:
        char charValue = 0;
        String columnValue = resultSet.getString(columnIndex);
        if (columnValue != null && columnValue.length() > 0) {
          charValue = columnValue.toCharArray()[0];
        }
        factory.writeChar(fieldName, charValue);
        break;
      case SHORT:
        factory.writeShort(fieldName, resultSet.getShort(columnIndex));
        break;
      case INT:
        factory.writeInt(fieldName, resultSet.getInt(columnIndex));
        break;
      case LONG:
        factory.writeLong(fieldName, resultSet.getLong(columnIndex));
        break;
      case FLOAT:
        factory.writeFloat(fieldName, resultSet.getFloat(columnIndex));
        break;
      case DOUBLE:
        factory.writeDouble(fieldName, resultSet.getDouble(columnIndex));
        break;
      case BYTE:
        factory.writeByte(fieldName, resultSet.getByte(columnIndex));
        break;
      case BOOLEAN:
        factory.writeBoolean(fieldName, resultSet.getBoolean(columnIndex));
        break;
      case DATE:
        java.sql.Timestamp sqlDate = resultSet.getTimestamp(columnIndex);
        java.util.Date pdxDate = null;
        if (sqlDate != null) {
          pdxDate = new java.util.Date(sqlDate.getTime());
        }
        factory.writeDate(fieldName, pdxDate);
        break;
      case BYTE_ARRAY:
        factory.writeByteArray(fieldName, resultSet.getBytes(columnIndex));
        break;
      case BOOLEAN_ARRAY:
        factory.writeBooleanArray(fieldName,
            convertJdbcObjectToJavaType(boolean[].class, resultSet.getObject(columnIndex)));
        break;
      case CHAR_ARRAY:
        factory.writeCharArray(fieldName,
            convertJdbcObjectToJavaType(char[].class, resultSet.getObject(columnIndex)));
        break;
      case SHORT_ARRAY:
        factory.writeShortArray(fieldName,
            convertJdbcObjectToJavaType(short[].class, resultSet.getObject(columnIndex)));
        break;
      case INT_ARRAY:
        factory.writeIntArray(fieldName,
            convertJdbcObjectToJavaType(int[].class, resultSet.getObject(columnIndex)));
        break;
      case LONG_ARRAY:
        factory.writeLongArray(fieldName,
            convertJdbcObjectToJavaType(long[].class, resultSet.getObject(columnIndex)));
        break;
      case FLOAT_ARRAY:
        factory.writeFloatArray(fieldName,
            convertJdbcObjectToJavaType(float[].class, resultSet.getObject(columnIndex)));
        break;
      case DOUBLE_ARRAY:
        factory.writeDoubleArray(fieldName,
            convertJdbcObjectToJavaType(double[].class, resultSet.getObject(columnIndex)));
        break;
      case STRING_ARRAY:
        factory.writeStringArray(fieldName,
            convertJdbcObjectToJavaType(String[].class, resultSet.getObject(columnIndex)));
        break;
      case OBJECT_ARRAY:
        factory.writeObjectArray(fieldName,
            convertJdbcObjectToJavaType(Object[].class, resultSet.getObject(columnIndex)));
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        factory.writeArrayOfByteArrays(fieldName,
            convertJdbcObjectToJavaType(byte[][].class, resultSet.getObject(columnIndex)));
        break;
      case OBJECT:
        factory.writeObject(fieldName, resultSet.getObject(columnIndex));
        break;
    }
  }

  private <T> T convertJdbcObjectToJavaType(Class<T> javaType, Object jdbcObject) {
    try {
      return javaType.cast(jdbcObject);
    } catch (ClassCastException classCastException) {
      throw new JdbcConnectorException("Could not convert " + jdbcObject.getClass().getTypeName()
          + " to " + javaType.getTypeName(), classCastException);
    }
  }

  private <K, V> FieldType getFieldType(Region<K, V> region, String pdxClassName,
      String fieldName) {
    if (pdxClassName == null) {
      return FieldType.OBJECT;
    }

    InternalCache cache = (InternalCache) region.getRegionService();
    PdxType pdxType = cache.getPdxRegistry().getPdxTypeForField(fieldName, pdxClassName);
    if (pdxType != null) {
      PdxField pdxField = pdxType.getPdxField(fieldName);
      if (pdxField != null) {
        return pdxField.getFieldType();
      }
    }

    throw new JdbcConnectorException("Could not find PdxType for field " + fieldName
        + ". Add class " + pdxClassName + " with " + fieldName + " to pdx registry.");

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
      statement.setObject(index, value);
    }
  }

  private String mapColumnNameToFieldName(String columnName, RegionMapping regionMapping) {
    return regionMapping.getFieldNameForColumn(columnName);
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
}
