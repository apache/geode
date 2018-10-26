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

import java.util.List;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingNotFoundException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

@Experimental
public class AlterMappingFunction extends CliFunction<RegionMapping> {

  @Override
  public CliFunctionResult executeFunction(FunctionContext<RegionMapping> context)
      throws Exception {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    RegionMapping mapping = context.getArguments();
    RegionMapping existingMapping =
        service.getMappingForRegion(mapping.getRegionName());
    if (existingMapping == null) {
      throw new RegionMappingNotFoundException(
          "RegionMapping for region " + mapping.getRegionName() + " was not found");
    }

    // action
    RegionMapping alteredMapping = alterRegionMapping(mapping, existingMapping);
    service.replaceRegionMapping(alteredMapping);

    // output
    return new CliFunctionResult(context.getMemberName(), alteredMapping, null);
  }

  RegionMapping alterRegionMapping(RegionMapping regionMapping,
      RegionMapping existingMapping) {
    String connectionName = getValue(regionMapping.getConnectionConfigName(),
        existingMapping.getConnectionConfigName());
    String table = getValue(regionMapping.getTableName(), existingMapping.getTableName());
    String pdxClassName =
        getValue(regionMapping.getPdxClassName(), existingMapping.getPdxClassName());
    Boolean keyInValue = regionMapping.isPrimaryKeyInValue() == null
        ? existingMapping.isPrimaryKeyInValue() : regionMapping.isPrimaryKeyInValue();

    List<RegionMapping.FieldMapping> fieldMappings =
        regionMapping.getFieldMapping();
    if (!regionMapping.isFieldMappingModified()) {
      fieldMappings = existingMapping.getFieldMapping();
    }
    RegionMapping alteredMapping = new RegionMapping(
        existingMapping.getRegionName(), pdxClassName, table, connectionName, keyInValue);
    alteredMapping.getFieldMapping().addAll(fieldMappings);
    return alteredMapping;
  }

  private String getValue(String newValue, String existingValue) {
    // if newValue is null use the value already in the config
    // if newValue is the empty string, then "unset" it by returning null
    if (newValue == null) {
      return existingValue;
    }
    return newValue.isEmpty() ? null : newValue;
  }
}
