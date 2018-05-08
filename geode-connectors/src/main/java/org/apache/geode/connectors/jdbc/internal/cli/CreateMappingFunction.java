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
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

@Experimental
public class CreateMappingFunction
    extends JdbcCliFunction<ConnectorService.RegionMapping, CliFunctionResult> {

  CreateMappingFunction() {
    super();
  }

  @Override
  CliFunctionResult getFunctionResult(JdbcConnectorService service,
      FunctionContext<ConnectorService.RegionMapping> context) throws Exception {
    // input
    ConnectorService.RegionMapping regionMapping = context.getArguments();

    // action
    createRegionMapping(service, regionMapping);

    // output
    String member = getMember(context);
    String message =
        "Created JDBC mapping for region " + regionMapping.getRegionName() + " on " + member;
    return new CliFunctionResult(member, true, message);
  }

  /**
   * Creates the named connection configuration
   */
  void createRegionMapping(JdbcConnectorService service,
      ConnectorService.RegionMapping regionMapping) throws RegionMappingExistsException {
    service.createRegionMapping(regionMapping);
  }
}
