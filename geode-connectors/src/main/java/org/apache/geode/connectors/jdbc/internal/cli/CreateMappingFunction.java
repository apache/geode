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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

@Experimental
public class CreateMappingFunction extends CliFunction<RegionMapping> {

  CreateMappingFunction() {
    super();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<RegionMapping> context)
      throws Exception {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    // input
    RegionMapping regionMapping = context.getArguments();

    verifyRegionExists(context, regionMapping);

    // action
    createRegionMapping(service, regionMapping);

    // output
    String member = context.getMemberName();
    String message =
        "Created JDBC mapping for region " + regionMapping.getRegionName() + " on " + member;
    return new CliFunctionResult(member, true, message);
  }

  private void verifyRegionExists(FunctionContext<RegionMapping> context,
      RegionMapping regionMapping) {
    Cache cache = context.getCache();
    String regionName = regionMapping.getRegionName();
    if (cache.getRegion(regionName) == null) {
      throw new IllegalStateException(
          "create jdbc-mapping requires that the region \"" + regionName + "\" exists.");
    }
  }

  /**
   * Creates the named connection configuration
   */
  void createRegionMapping(JdbcConnectorService service,
      RegionMapping regionMapping) throws RegionMappingExistsException {
    service.createRegionMapping(regionMapping);
  }
}
