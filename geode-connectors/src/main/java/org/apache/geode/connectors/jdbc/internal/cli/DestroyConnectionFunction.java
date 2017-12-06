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

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class DestroyConnectionFunction implements Function<String>, InternalEntity {

  private static final String ID = DestroyConnectionFunction.class.getName();

  private final FunctionContextArgumentProvider argumentProvider;
  private final ExceptionHandler exceptionHandler;

  public DestroyConnectionFunction() {
    this(new FunctionContextArgumentProvider(), new ExceptionHandler());
  }

  private DestroyConnectionFunction(FunctionContextArgumentProvider jdbcCommandFunctionContext,
      ExceptionHandler exceptionHandler) {
    this.argumentProvider = jdbcCommandFunctionContext;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<String> context) {
    try {
      // input
      InternalJdbcConnectorService service = argumentProvider.getJdbcConnectorService(context);
      String connectionName = context.getArguments();

      // action
      boolean success = destroyConnectionConfig(service, connectionName);

      // output
      String member = argumentProvider.getMember(context);
      CliFunctionResult result = createResult(success, context, member, connectionName);
      context.getResultSender().lastResult(result);

    } catch (Exception e) {
      exceptionHandler.handleException(context, e);
    }
  }

  /**
   * Destroys the named connection configuration
   *
   * @return true if the connection was found and destroyed
   */
  boolean destroyConnectionConfig(InternalJdbcConnectorService service, String connectionName) {
    ConnectionConfiguration connectionConfig = service.getConnectionConfig(connectionName);
    if (connectionConfig != null) {
      service.destroyConnectionConfig(connectionName);
      return true;
    }
    return false;
  }

  private CliFunctionResult createResult(boolean success, FunctionContext<?> context, String member,
      String connectionName) {
    CliFunctionResult result;
    if (success) {
      XmlEntity xmlEntity = argumentProvider.createXmlEntity(context);
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
