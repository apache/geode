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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxInstance;

@Experimental
public class SqlHandler {
  private static final Logger logger = LogService.getLogger();
  private final InternalCache cache;
  private final RegionMapping regionMapping;
  private final DataSource dataSource;
  private final TableMetaDataView tableMetaData;
  private final Map<String, FieldMapping> pdxToFieldMappings = new HashMap<>();
  private volatile SqlToPdxInstance sqlToPdxInstance;

  public SqlHandler(InternalCache cache, String regionName,
      TableMetaDataManager tableMetaDataManager, JdbcConnectorService configService,
      DataSourceFactory dataSourceFactory) {
    this.cache = cache;
    regionMapping = getMappingForRegion(configService, regionName);
    dataSource = getDataSource(dataSourceFactory, regionMapping.getDataSourceName());
    tableMetaData = getTableMetaDataView(tableMetaDataManager);
    cache.getService(JdbcConnectorService.class).validateMapping(regionMapping, dataSource);
    initializeFieldMappingMaps();
  }

  public SqlHandler(InternalCache cache, String regionName,
      TableMetaDataManager tableMetaDataManager, JdbcConnectorService configService) {
    this(cache, regionName, tableMetaDataManager, configService,
        JNDIInvoker::getDataSource);
  }

  private TableMetaDataView getTableMetaDataView(TableMetaDataManager tableMetaDataManager) {
    try (Connection connection = getConnection()) {
      return tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
    } catch (SQLException ex) {
      throw new JdbcConnectorException("Could not connect to datasource \""
          + regionMapping.getDataSourceName() + "\" because: " + ex);
    }
  }

  private static RegionMapping getMappingForRegion(JdbcConnectorService configService,
      String regionName) {
    RegionMapping regionMapping = configService.getMappingForRegion(regionName);
    if (regionMapping == null) {
      throw new JdbcConnectorException("JDBC mapping for region " + regionName
          + " not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    }
    return regionMapping;
  }

  private static DataSource getDataSource(DataSourceFactory dataSourceFactory,
      String dataSourceName) {
    DataSource dataSource = dataSourceFactory.getDataSource(dataSourceName);
    if (dataSource == null) {
      throw new JdbcConnectorException("JDBC data-source named \"" + dataSourceName
          + "\" not found. Create it with gfsh 'create data-source --pooled --name="
          + dataSourceName + "'.");
    }
    return dataSource;
  }

  private void initializeFieldMappingMaps() {
    for (FieldMapping fieldMapping : regionMapping.getFieldMappings()) {
      pdxToFieldMappings.put(fieldMapping.getPdxName(), fieldMapping);
    }
  }

  private String getColumnNameForField(String fieldName) {
    FieldMapping match = pdxToFieldMappings.get(fieldName);
    if (match != null) {
      return match.getJdbcName();
    }
    return null;
  }

  Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public <K, V> PdxInstance read(Region<K, V> region, K key) throws SQLException {
    if (key == null) {
      throw new IllegalArgumentException("Key for query cannot be null");
    }

    PdxInstance result;
    try (Connection connection = getConnection()) {
      EntryColumnData entryColumnData =
          getEntryColumnData(tableMetaData, key, null, Operation.GET);
      try (PreparedStatement statement =
          getPreparedStatement(connection, tableMetaData, entryColumnData, Operation.GET)) {
        try (ResultSet resultSet = executeReadQuery(statement, entryColumnData)) {
          result = getSqlToPdxInstance().create(resultSet);
        }
      }
    }
    return result;
  }

  private SqlToPdxInstance getSqlToPdxInstance() {
    SqlToPdxInstance result = sqlToPdxInstance;
    if (result == null) {
      result = initializeSqlToPdxInstance();
    }
    return result;
  }

  private synchronized SqlToPdxInstance initializeSqlToPdxInstance() {
    SqlToPdxInstanceCreator sqlToPdxInstanceCreator =
        new SqlToPdxInstanceCreator(cache, regionMapping);
    SqlToPdxInstance result = sqlToPdxInstanceCreator.create();
    sqlToPdxInstance = result;
    return result;
  }

  private ResultSet executeReadQuery(PreparedStatement statement, EntryColumnData entryColumnData)
      throws SQLException {
    setValuesInStatement(statement, entryColumnData, Operation.GET);
    return statement.executeQuery();
  }

  private void setValuesInStatement(PreparedStatement statement, EntryColumnData entryColumnData,
      Operation operation)
      throws SQLException {
    int index = 0;
    if (operation.isCreate() || operation.isUpdate()) {
      index = setValuesFromColumnData(statement, entryColumnData.getEntryValueColumnData(), index);
    }
    setValuesFromColumnData(statement, entryColumnData.getEntryKeyColumnData(), index);
  }

  private int setValuesFromColumnData(PreparedStatement statement, List<ColumnData> columnDataList,
      int index) throws SQLException {
    for (ColumnData columnData : columnDataList) {
      index++;
      setValueOnStatement(statement, index, columnData);
    }
    return index;
  }

  private void setValueOnStatement(PreparedStatement statement, int index, ColumnData columnData)
      throws SQLException {
    Object value = columnData.getValue();
    if (value instanceof Character) {
      Character character = ((Character) value);
      // if null character, set to null string instead of a string with the null character
      value = character.equals(Character.valueOf((char) 0)) ? null : character.toString();
    } else if (value instanceof Date) {
      Date jdkDate = (Date) value;
      switch (columnData.getDataType()) {
        case DATE:
          value = new java.sql.Date(jdkDate.getTime());
          break;
        case TIME:
        case TIME_WITH_TIMEZONE:
          value = new java.sql.Time(jdkDate.getTime());
          break;
        case TIMESTAMP:
        case TIMESTAMP_WITH_TIMEZONE:
          value = new java.sql.Timestamp(jdkDate.getTime());
          break;
        default:
          // no conversion needed
          break;
      }
    }
    if (value == null) {
      statement.setNull(index, columnData.getDataType().getVendorTypeNumber());
    } else {
      statement.setObject(index, value);
    }
  }

  public <K, V> void write(Region<K, V> region, Operation operation, K key, PdxInstance value)
      throws SQLException {
    if (value == null && !operation.isDestroy()) {
      throw new IllegalArgumentException("PdxInstance cannot be null for non-destroy operations");
    }

    try (Connection connection = getConnection()) {
      EntryColumnData entryColumnData =
          getEntryColumnData(tableMetaData, key, value, operation);
      int updateCount = 0;
      SQLException firstSqlEx = null;
      try (PreparedStatement statement =
          getPreparedStatement(connection, tableMetaData, entryColumnData, operation)) {
        updateCount = executeWriteStatement(statement, entryColumnData, operation);
      } catch (SQLException e) {
        if (operation.isDestroy()) {
          throw e;
        }
        firstSqlEx = e;
      }

      // Destroy action not guaranteed to modify any database rows
      if (operation.isDestroy()) {
        return;
      }

      if (updateCount <= 0) {
        Operation upsertOp = getOppositeOperation(operation);
        try (PreparedStatement upsertStatement =
            getPreparedStatement(connection, tableMetaData, entryColumnData, upsertOp)) {
          updateCount = executeWriteStatement(upsertStatement, entryColumnData, operation);
        }
      }

      if (updateCount <= 0 && firstSqlEx != null) {
        throw firstSqlEx;
      }

      assert updateCount == 1 : "expected 1 but updateCount was: " + updateCount;
    }
  }

  private Operation getOppositeOperation(Operation operation) {
    return operation.isUpdate() ? Operation.CREATE : Operation.UPDATE;
  }

  private int executeWriteStatement(PreparedStatement statement, EntryColumnData entryColumnData,
      Operation operation)
      throws SQLException {
    setValuesInStatement(statement, entryColumnData, operation);
    return statement.executeUpdate();
  }

  private PreparedStatement getPreparedStatement(Connection connection,
      TableMetaDataView tableMetaData, EntryColumnData entryColumnData, Operation operation)
      throws SQLException {
    String sqlStr = getSqlString(tableMetaData, entryColumnData, operation);
    if (logger.isDebugEnabled()) {
      logger.debug("Got SQL string:{} with key:{} value:{}", sqlStr,
          entryColumnData.getEntryKeyColumnData(),
          entryColumnData.getEntryValueColumnData());
    }
    return connection.prepareStatement(sqlStr);
  }

  private String getSqlString(TableMetaDataView tableMetaData, EntryColumnData entryColumnData,
      Operation operation) {
    SqlStatementFactory statementFactory =
        new SqlStatementFactory(tableMetaData.getIdentifierQuoteString());
    String tableName = tableMetaData.getQuotedTablePath();
    if (operation.isCreate()) {
      return statementFactory.createInsertSqlString(tableName, entryColumnData);
    } else if (operation.isUpdate()) {
      return statementFactory.createUpdateSqlString(tableName, entryColumnData);
    } else if (operation.isDestroy()) {
      return statementFactory.createDestroySqlString(tableName, entryColumnData);
    } else if (operation.isGet()) {
      return statementFactory.createSelectQueryString(tableName, entryColumnData);
    } else {
      throw new InternalGemFireException("unsupported operation " + operation);
    }
  }

  <K> EntryColumnData getEntryColumnData(TableMetaDataView tableMetaData,
      K key, PdxInstance value, Operation operation) {
    List<ColumnData> keyColumnData = createKeyColumnDataList(tableMetaData, key);
    List<ColumnData> valueColumnData = null;

    if (operation.isCreate() || operation.isUpdate()) {
      valueColumnData = createValueColumnDataList(tableMetaData, value);
    }

    return new EntryColumnData(keyColumnData, valueColumnData);
  }

  private <K> List<ColumnData> createKeyColumnDataList(TableMetaDataView tableMetaData, K key) {
    List<String> keyColumnNames = tableMetaData.getKeyColumnNames();
    List<ColumnData> result = new ArrayList<>();
    if (keyColumnNames.size() == 1) {
      String keyColumnName = keyColumnNames.get(0);
      ColumnData columnData =
          new ColumnData(keyColumnName, key, tableMetaData.getColumnDataType(keyColumnName));
      result.add(columnData);
    } else {
      if (!(key instanceof PdxInstance)) {
        throw new JdbcConnectorException(
            "The key \"" + key + "\" of class \"" + key.getClass().getName()
                + "\" must be a PdxInstance because multiple columns are configured as ids.");
      }
      PdxInstance compositeKey = (PdxInstance) key;
      if (compositeKey.isDeserializable()) {
        throw new JdbcConnectorException(
            "The key \"" + key
                + "\" must be a PdxInstance created with PdxInstanceFactory.neverDeserialize");
      }
      List<String> fieldNames = compositeKey.getFieldNames();
      if (fieldNames.size() != keyColumnNames.size()) {
        throw new JdbcConnectorException("The key \"" + key + "\" should have "
            + keyColumnNames.size() + " fields but has " + fieldNames.size() + " fields.");
      }
      for (String fieldName : fieldNames) {
        String columnName = getColumnNameForField(fieldName);
        if (columnName == null || !keyColumnNames.contains(columnName)) {
          throw new JdbcConnectorException("The key \"" + key + "\" has the field \"" + fieldName
              + "\" which does not match any of the key columns: " + keyColumnNames);
        }
        ColumnData columnData = new ColumnData(columnName, compositeKey.getField(fieldName),
            tableMetaData.getColumnDataType(columnName));
        result.add(columnData);
      }
    }
    return result;
  }

  private List<ColumnData> createValueColumnDataList(TableMetaDataView tableMetaData,
      PdxInstance value) {
    List<ColumnData> result = new ArrayList<>();
    for (String fieldName : value.getFieldNames()) {
      String columnName = getColumnNameForField(fieldName);
      if (columnName == null) {
        // The user must have added a new field to their pdx domain class.
        // To support PDX class versioning we will ignore this field.
        continue;
      }
      if (tableMetaData.getKeyColumnNames().contains(columnName)) {
        continue;
      }
      ColumnData columnData = new ColumnData(columnName, value.getField(fieldName),
          tableMetaData.getColumnDataType(columnName));
      result.add(columnData);
    }
    return result;
  }

  public interface DataSourceFactory {
    DataSource getDataSource(String dataSourceName);
  }
}
