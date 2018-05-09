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
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigNotFoundException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

@Experimental
public class AlterConnectionFunction extends CliFunction<ConnectorService.Connection> {

  @Override
  public CliFunctionResult executeFunction(FunctionContext<ConnectorService.Connection> context)
      throws Exception {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);

    ConnectorService.Connection connectionConfig = context.getArguments();
    ConnectorService.Connection existingConfig =
        service.getConnectionConfig(connectionConfig.getName());
    if (existingConfig == null) {
      throw new ConnectionConfigNotFoundException(
          "ConnectionConfiguration " + connectionConfig.getName() + " was not found");
    }

    // action
    ConnectorService.Connection alteredConfig =
        alterConnectionConfig(connectionConfig, existingConfig);
    service.replaceConnectionConfig(alteredConfig);

    return new CliFunctionResult(context.getMemberName(), alteredConfig, null);
  }

  /**
   * Creates the named connection configuration
   */
  ConnectorService.Connection alterConnectionConfig(ConnectorService.Connection connectionConfig,
      ConnectorService.Connection existingConfig) {
    String url = getValue(connectionConfig.getUrl(), existingConfig.getUrl());
    String user = getValue(connectionConfig.getUser(), existingConfig.getUser());
    String password = getValue(connectionConfig.getPassword(), existingConfig.getPassword());

    String parameters = connectionConfig.getParameters();
    if (parameters == null) {
      parameters = existingConfig.getParameters();
    }
    ConnectorService.Connection alteredConfig =
        new ConnectorService.Connection(existingConfig.getName(), url, user, password, parameters);
    return alteredConfig;
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
