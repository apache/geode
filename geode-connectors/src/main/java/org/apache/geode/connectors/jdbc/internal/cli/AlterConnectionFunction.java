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

import java.util.Map;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigNotFoundException;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

@Experimental
public class AlterConnectionFunction
    extends JdbcCliFunction<ConnectionConfiguration, CliFunctionResult> {

  AlterConnectionFunction() {
    super();
  }

  @Override
  CliFunctionResult getFunctionResult(JdbcConnectorService service,
      FunctionContext<ConnectionConfiguration> context) throws Exception {
    ConnectionConfiguration connectionConfig = context.getArguments();
    ConnectionConfiguration existingConfig =
        service.getConnectionConfig(connectionConfig.getName());
    if (existingConfig == null) {
      throw new ConnectionConfigNotFoundException(
          "ConnectionConfiguration " + connectionConfig.getName() + " was not found");
    }

    // action
    ConnectionConfiguration alteredConfig = alterConnectionConfig(connectionConfig, existingConfig);
    service.replaceConnectionConfig(alteredConfig);

    // output
    String member = getMember(context);
    XmlEntity xmlEntity = createXmlEntity(context);
    CliFunctionResult result = createSuccessResult(connectionConfig.getName(), member, xmlEntity);
    return result;
  }

  /**
   * Creates the named connection configuration
   */
  ConnectionConfiguration alterConnectionConfig(ConnectionConfiguration connectionConfig,
      ConnectionConfiguration existingConfig) {
    String url = getValue(connectionConfig.getUrl(), existingConfig.getUrl());
    String user = getValue(connectionConfig.getUser(), existingConfig.getUser());
    String password = getValue(connectionConfig.getPassword(), existingConfig.getPassword());

    Map<String, String> parameters = connectionConfig.getParameters();
    if (parameters == null) {
      parameters = existingConfig.getParameters();
    }
    ConnectionConfiguration alteredConfig =
        new ConnectionConfiguration(existingConfig.getName(), url, user, password, parameters);
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

  private CliFunctionResult createSuccessResult(String connectionName, String member,
      XmlEntity xmlEntity) {
    String message = "Altered JDBC connection " + connectionName + " on " + member;
    return new CliFunctionResult(member, xmlEntity, message);
  }
}
