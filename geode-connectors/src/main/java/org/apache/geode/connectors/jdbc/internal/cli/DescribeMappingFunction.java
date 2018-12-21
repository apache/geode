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

import static org.apache.geode.connectors.util.internal.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingNotFoundException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.connectors.util.internal.DescribeMappingResult;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

public class DescribeMappingFunction extends CliFunction<String> {

  @Override
  public CliFunctionResult executeFunction(FunctionContext<String> context) {
    return new CliFunctionResult(context.getMemberName(), getResult(context));

  }

  private DescribeMappingResult getResult(FunctionContext<String> context) {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    RegionMapping mapping = service.getMappingForRegion(context.getArguments());
    if (mapping == null) {
      return null;
    }

    Map<String, String> attributes = new HashMap<>();
    attributes.put(REGION_NAME, mapping.getRegionName());
    attributes.put(DATA_SOURCE_NAME, mapping.getDataSourceName());
    attributes.put(TABLE_NAME, mapping.getTableName());
    attributes.put(PDX_NAME, mapping.getPdxName());

    try {
      attributes.put(SYNCHRONOUS_NAME,
          Boolean.toString(service.isMappingSynchronous(mapping, context.getCache())));
    } catch (RegionMappingNotFoundException e) {
      attributes.put(SYNCHRONOUS_NAME, "Not found.");
    }

    return new DescribeMappingResult(attributes);
  }
}
