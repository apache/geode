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

      String member = context.getMemberName();
      return new CliFunctionResult(member, fieldMappings);
    }
  }

  private FieldType computeFieldType(boolean isNullable, JDBCType jdbcType) {
    // TODO Auto-generated method stub
    return null;
  }

}
