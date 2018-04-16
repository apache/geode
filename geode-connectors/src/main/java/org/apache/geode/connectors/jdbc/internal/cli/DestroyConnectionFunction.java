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
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;


public class DestroyConnectionFunction extends JdbcCliFunction<String, CliFunctionResult> {

  DestroyConnectionFunction() {
    super();
  }

  @Override
  CliFunctionResult getFunctionResult(JdbcConnectorService service,
      FunctionContext<String> context) {
    String connectionName = context.getArguments();
    boolean success = destroyConnectionConfig(service, connectionName);

    // output
    String member = getMember(context);
    return createResult(success, context, member, connectionName);
  }

  /**
   * Destroys the named connection configuration
   *
   * @return true if the connection was found and destroyed
   */
  boolean destroyConnectionConfig(JdbcConnectorService service, String connectionName) {
    ConnectorService.Connection connectionConfig = service.getConnectionConfig(connectionName);
    if (connectionConfig != null) {
      service.destroyConnectionConfig(connectionName);
      return true;
    }
    return false;
  }

  private CliFunctionResult createResult(boolean success, FunctionContext<String> context,
      String member, String connectionName) {
    CliFunctionResult result;
    if (success) {
      XmlEntity xmlEntity = createXmlEntity(context);
      result = createSuccessResult(member, connectionName, xmlEntity);

    } else {
      result = createNotFoundResult(member, connectionName);
    }
    return result;
  }

  private CliFunctionResult createSuccessResult(String member, String connectionName,
      XmlEntity xmlEntity) {
    String message = "Destroyed JDBC connection \"" + connectionName + "\" on " + member;
    return new CliFunctionResult(member, xmlEntity, message);
  }

  private CliFunctionResult createNotFoundResult(String member, String connectionName) {
    String message = "Connection named \"" + connectionName + "\" not found";
    return new CliFunctionResult(member, false, message);
  }
}
