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
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;


public class CreateConnectionFunction
    extends JdbcCliFunction<ConnectionConfiguration, CliFunctionResult> {

  CreateConnectionFunction() {
    super();
  }

  @Override
  CliFunctionResult getFunctionResult(JdbcConnectorService service,
      FunctionContext<ConnectionConfiguration> context) throws Exception {
    ConnectionConfiguration connectionConfig = context.getArguments();
    createConnectionConfig(service, connectionConfig);
    String member = getMember(context);
    XmlEntity xmlEntity = createXmlEntity(context);
    return createSuccessResult(connectionConfig.getName(), member, xmlEntity);
  }

  /**
   * Creates the named connection configuration
   */
  void createConnectionConfig(JdbcConnectorService service,
      ConnectionConfiguration connectionConfig) throws ConnectionConfigExistsException {
    service.createConnectionConfig(connectionConfig);
  }

  private CliFunctionResult createSuccessResult(String connectionName, String member,
      XmlEntity xmlEntity) {
    String message = "Created JDBC connection " + connectionName + " on " + member;
    return new CliFunctionResult(member, xmlEntity, message);
  }
}
