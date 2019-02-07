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
package org.apache.geode.connectors.jdbc.internal.cli;

import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler.DataSourceFactory;
import org.apache.geode.connectors.jdbc.internal.SqlToPdxInstanceCreator;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.TypeRegistry;

@Experimental
public class CreateMappingPreconditionCheckFunction extends CliFunction<RegionMapping> {

  private transient DataSourceFactory dataSourceFactory;
  private transient TableMetaDataManager tableMetaDataManager;

  CreateMappingPreconditionCheckFunction(DataSourceFactory factory, TableMetaDataManager manager) {
    this.dataSourceFactory = factory;
    this.tableMetaDataManager = manager;
  }

  CreateMappingPreconditionCheckFunction() {
    this(dataSourceName -> JNDIInvoker.getDataSource(dataSourceName), new TableMetaDataManager());
  }

  // used by java during deserialization
  private void readObject(ObjectInputStream stream) {
    this.dataSourceFactory = dataSourceName -> JNDIInvoker.getDataSource(dataSourceName);
    this.tableMetaDataManager = new TableMetaDataManager();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<RegionMapping> context)
      throws Exception {
    RegionMapping regionMapping = context.getArguments();
    String dataSourceName = regionMapping.getDataSourceName();
    DataSource dataSource = dataSourceFactory.getDataSource(dataSourceName);
    if (dataSource == null) {
      throw new JdbcConnectorException("JDBC data-source named \"" + dataSourceName
          + "\" not found. Create it with gfsh 'create data-source --pooled --name="
          + dataSourceName + "'.");
    }
    InternalCache cache = (InternalCache) context.getCache();
    TypeRegistry typeRegistry = cache.getPdxRegistry();
    try (Connection connection = dataSource.getConnection()) {
      TableMetaDataView tableMetaData =
          tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
      Object[] output = new Object[2];
      ArrayList<FieldMapping> fieldMappings = new ArrayList<>();
      output[1] = fieldMappings;
      for (String jdbcName : tableMetaData.getColumnNames()) {
        boolean isNullable = tableMetaData.isColumnNullable(jdbcName);
        JDBCType jdbcType = tableMetaData.getColumnDataType(jdbcName);
        FieldMapping fieldMapping =
            new FieldMapping("", "", jdbcName, jdbcType.getName(), isNullable);
        updateFieldMappingFromExistingPdxType(fieldMapping, typeRegistry,
            regionMapping.getPdxName());
        fieldMappings.add(fieldMapping);
      }
      if (regionMapping.getIds() == null || regionMapping.getIds().isEmpty()) {
        List<String> keyColummnNames = tableMetaData.getKeyColumnNames();
        output[0] = String.join(",", keyColummnNames);
      }

      String member = context.getMemberName();
      return new CliFunctionResult(member, output);
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
  }

  private void updateFieldMappingFromExistingPdxType(FieldMapping fieldMapping,
      TypeRegistry typeRegistry, String pdxClassName) {
    String columnName = fieldMapping.getJdbcName();
    try {
      Set<PdxField> foundFields = typeRegistry.findFieldThatMatchesName(pdxClassName, columnName);
      if (!foundFields.isEmpty()) {
        fieldMapping.setPdxName(foundFields.iterator().next().getFieldName());
        JDBCType columnType = JDBCType.valueOf(fieldMapping.getJdbcType());
        FieldType fieldType = SqlToPdxInstanceCreator.findFieldType(foundFields,
            fieldMapping.isJdbcNullable(), columnType);
        fieldMapping.setPdxType(fieldType.name());
      }
    } catch (IllegalStateException ex) {
      throw new JdbcConnectorException(
          "Could not determine what pdx field to use for the column name " + columnName
              + " because " + ex.getMessage());
    }
  }
}
