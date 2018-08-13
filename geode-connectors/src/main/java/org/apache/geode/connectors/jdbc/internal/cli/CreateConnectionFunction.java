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

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.cli.CliFunctionResult;

public class CreateConnectionFunction extends CliFunction<ConnectorService.Connection> {

  @Override
  public CliFunctionResult executeFunction(FunctionContext<ConnectorService.Connection> context)
      throws Exception {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    ConnectorService.Connection connectionConfig = context.getArguments();
    createConnectionConfig(service, connectionConfig);
    String member = context.getMemberName();
    return createSuccessResult(connectionConfig.getName(), member);
  }

  /**
   * Creates the named connection configuration
   */
  void createConnectionConfig(JdbcConnectorService service,
      ConnectorService.Connection connectionConfig) throws ConnectionConfigExistsException {
    service.createConnectionConfig(connectionConfig);
  }

  private CliFunctionResult createSuccessResult(String connectionName, String member) {
    String message = "Created JDBC connection " + connectionName + " on " + member;
    return new CliFunctionResult(member, message);
  }
}
