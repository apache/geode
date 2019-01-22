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

import java.sql.Connection;
import java.sql.JDBCType;
import java.util.ArrayList;

import javax.sql.DataSource;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
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

  CreateMappingPreconditionCheckFunction() {
    super();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<RegionMapping> context)
      throws Exception {
    RegionMapping regionMapping = context.getArguments();
    String dataSourceName = regionMapping.getDataSourceName();
    DataSource dataSource = JNDIInvoker.getDataSource(dataSourceName);
    if (dataSource == null) {
      throw new JdbcConnectorException("JDBC data-source named \"" + dataSourceName
          + "\" not found. Create it with gfsh 'create data-source --pooled --name="
          + dataSourceName + "'.");
    }
    try (Connection connection = dataSource.getConnection()) {
      TableMetaDataView tableMetaData =
          new TableMetaDataManager().getTableMetaDataView(connection, regionMapping);
      PdxInstanceFactory pdxInstanceFactory =
          context.getCache().createPdxInstanceFactory(regionMapping.getPdxName());
      ArrayList<FieldMapping> fieldMappings = new ArrayList<>();
      for (String jdbcName : tableMetaData.getColumnNames()) {
        boolean isNullable = tableMetaData.isColumnNullable(jdbcName);
        JDBCType jdbcType = tableMetaData.getColumnDataType(jdbcName);
        String pdxName = jdbcName;
        FieldType pdxType = computeFieldType(isNullable, jdbcType);
        pdxInstanceFactory.writeField(pdxName, null, pdxType.getFieldClass());
        fieldMappings.add(new FieldMapping(pdxName, pdxType.name(), jdbcName, jdbcType.getName()));
      }
      PdxInstance pdxInstance = pdxInstanceFactory.create();
      // TODO look for existing PdxType in the registry whose names differ in case
      // TODO use this code when we create the field mapping
      // Set<String> columnNames = tableMetaDataView.getColumnNames();
      // if (columnNames.contains(fieldName)) {
      // return fieldName;
      // }
      //
      // List<String> ignoreCaseMatch = columnNames.stream().filter(c ->
      // c.equalsIgnoreCase(fieldName))
      // .collect(Collectors.toList());
      // if (ignoreCaseMatch.size() > 1) {
      // throw new JdbcConnectorException(
      // "The SQL table has at least two columns that match the PDX field: " + fieldName);
      // }
      //
      // if (ignoreCaseMatch.size() == 1) {
      // return ignoreCaseMatch.get(0);
      // }
      //
      // // there is no match either in the configured mapping or the table columns
      // return fieldName;

      // TODO use the following code when we create the mapping
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

      String member = context.getMemberName();
      return new CliFunctionResult(member, fieldMappings);
    }
  }

  private FieldType computeFieldType(boolean isNullable, JDBCType jdbcType) {
    // TODO Auto-generated method stub
    return null;
  }

}
