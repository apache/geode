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

import java.util.Set;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.internal.InternalEntity;

public class ListConnectionFunction implements Function<Void>, InternalEntity {

  private static final String ID = ListConnectionFunction.class.getName();

  private final FunctionContextArgumentProvider argumentProvider;
  private final ExceptionHandler exceptionHandler;

  public ListConnectionFunction() {
    this(new FunctionContextArgumentProvider(), new ExceptionHandler());
  }

  private ListConnectionFunction(FunctionContextArgumentProvider jdbcCommandFunctionContext,
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
  public void execute(FunctionContext<Void> context) {
    try {
      // input
      InternalJdbcConnectorService service = argumentProvider.getJdbcConnectorService(context);

      // action
      ConnectionConfiguration[] connectionConfigs = getConnectionConfigAsArray(service);

      // output
      context.getResultSender().lastResult(connectionConfigs);

    } catch (Exception e) {
      exceptionHandler.handleException(context, e);
    }
  }

  ConnectionConfiguration[] getConnectionConfigAsArray(InternalJdbcConnectorService service) {
    Set<ConnectionConfiguration> connectionConfigs = getConnectionConfigs(service);
    return connectionConfigs.toArray(new ConnectionConfiguration[connectionConfigs.size()]);
  }

  private Set<ConnectionConfiguration> getConnectionConfigs(InternalJdbcConnectorService service) {
    return service.getConnectionConfigs();
  }
}
