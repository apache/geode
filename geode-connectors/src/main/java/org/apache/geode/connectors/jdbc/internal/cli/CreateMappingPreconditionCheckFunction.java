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

import javax.sql.DataSource;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler.DataSourceFactory;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

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
    try (Connection connection = dataSource.getConnection()) {
      TableMetaDataView tableMetaData =
          tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
      PdxInstanceFactory pdxInstanceFactory =
          context.getCache().createPdxInstanceFactory(regionMapping.getPdxName());
      Object[] output = new Object[2];
      ArrayList<FieldMapping> fieldMappings = new ArrayList<>();
      output[1] = fieldMappings;
      for (String jdbcName : tableMetaData.getColumnNames()) {
        boolean isNullable = tableMetaData.isColumnNullable(jdbcName);
        JDBCType jdbcType = tableMetaData.getColumnDataType(jdbcName);
        String pdxName = jdbcName;
        // TODO: look for existing pdx types to picked pdxName
        // It seems very unlikely that when a mapping is being
        // created that a pdx type will already exist. So I'm not
        // sure trying to look for an existing type is worth the effort.
        // But that does mean that the pdx field name will always be the
        // same as the column name. If we added a gfsh command that allowed
        // you to create a pdx type (pretty easy to implement using PdxInstanceFactory)
        // then it would be much more likely that a pdx type could exist
        // when the mapping is created. In that case we should then do
        // some extra work here to look at existing types.
        FieldType pdxType = computeFieldType(isNullable, jdbcType);
        pdxInstanceFactory.writeField(pdxName, null, pdxType.getFieldClass());
        fieldMappings.add(
            new FieldMapping(pdxName, pdxType.name(), jdbcName, jdbcType.getName(), isNullable));
      }
      PdxInstance pdxInstance = pdxInstanceFactory.create();
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

  static FieldType computeFieldType(boolean isNullable, JDBCType jdbcType) {
    switch (jdbcType) {
      case BIT: // 1 bit
        return computeType(isNullable, FieldType.BOOLEAN);
      case TINYINT: // unsigned 8 bits
        return computeType(isNullable, FieldType.SHORT);
      case SMALLINT: // signed 16 bits
        return computeType(isNullable, FieldType.SHORT);
      case INTEGER: // signed 32 bits
        return computeType(isNullable, FieldType.INT);
      case BIGINT: // signed 64 bits
        return computeType(isNullable, FieldType.LONG);
      case FLOAT:
        return computeType(isNullable, FieldType.DOUBLE);
      case REAL:
        return computeType(isNullable, FieldType.FLOAT);
      case DOUBLE:
        return computeType(isNullable, FieldType.DOUBLE);
      case CHAR:
        return FieldType.STRING;
      case VARCHAR:
        return FieldType.STRING;
      case LONGVARCHAR:
        return FieldType.STRING;
      case DATE:
        return computeDate(isNullable);
      case TIME:
        return computeDate(isNullable);
      case TIMESTAMP:
        return computeDate(isNullable);
      case BINARY:
        return FieldType.BYTE_ARRAY;
      case VARBINARY:
        return FieldType.BYTE_ARRAY;
      case LONGVARBINARY:
        return FieldType.BYTE_ARRAY;
      case NULL:
        throw new IllegalStateException("unexpected NULL jdbc column type");
      case BLOB:
        return FieldType.BYTE_ARRAY;
      case BOOLEAN:
        return computeType(isNullable, FieldType.BOOLEAN);
      case NCHAR:
        return FieldType.STRING;
      case NVARCHAR:
        return FieldType.STRING;
      case LONGNVARCHAR:
        return FieldType.STRING;
      case TIME_WITH_TIMEZONE:
        return computeDate(isNullable);
      case TIMESTAMP_WITH_TIMEZONE:
        return computeDate(isNullable);
      default:
        return FieldType.OBJECT;
    }
  }

  private static FieldType computeType(boolean isNullable, FieldType nonNullType) {
    if (isNullable) {
      return FieldType.OBJECT;
    }
    return nonNullType;

  }

  private static FieldType computeDate(boolean isNullable) {
    return computeType(isNullable, FieldType.DATE);
  }

  // Set<PdxType> pdxTypes = getPdxTypesForClassName(typeRegistry);
  // String fieldName = findExactMatch(columnName, pdxTypes);
  // if (fieldName == null) {
  // fieldName = findCaseInsensitiveMatch(columnName, pdxTypes);
  // }
  // return fieldName;

  // private Set<PdxType> getPdxTypesForClassName(TypeRegistry typeRegistry) {
  // Set<PdxType> pdxTypes = typeRegistry.getPdxTypesForClassName(getPdxName());
  // if (pdxTypes.isEmpty()) {
  // throw new JdbcConnectorException(
  // "The class " + getPdxName() + " has not been pdx serialized.");
  // }
  // return pdxTypes;
  // }

  // /**
  // * Given a column name and a set of pdx types, find the field name in those types that
  // match,
  // * ignoring case, the column name.
  // *
  // * @return the matching field name or null if no match
  // * @throws JdbcConnectorException if no fields match
  // * @throws JdbcConnectorException if more than one field matches
  // */
  // private String findCaseInsensitiveMatch(String columnName, Set<PdxType> pdxTypes) {
  // HashSet<String> matchingFieldNames = new HashSet<>();
  // for (PdxType pdxType : pdxTypes) {
  // for (String existingFieldName : pdxType.getFieldNames()) {
  // if (existingFieldName.equalsIgnoreCase(columnName)) {
  // matchingFieldNames.add(existingFieldName);
  // }
  // }
  // }
  // if (matchingFieldNames.isEmpty()) {
  // throw new JdbcConnectorException("The class " + getPdxName()
  // + " does not have a field that matches the column " + columnName);
  // } else if (matchingFieldNames.size() > 1) {
  // throw new JdbcConnectorException(
  // "Could not determine what pdx field to use for the column name " + columnName
  // + " because the pdx fields " + matchingFieldNames + " all match it.");
  // }
  // return matchingFieldNames.iterator().next();
  // }
  //
  // /**
  // * Given a column name, search the given pdxTypes for a field whose name exactly matches the
  // * column name.
  // *
  // * @return the matching field name or null if no match
  // */
  // private String findExactMatch(String columnName, Set<PdxType> pdxTypes) {
  // for (PdxType pdxType : pdxTypes) {
  // if (pdxType.getPdxField(columnName) != null) {
  // return columnName;
  // }
  // }
  // return null;
  // }


}
