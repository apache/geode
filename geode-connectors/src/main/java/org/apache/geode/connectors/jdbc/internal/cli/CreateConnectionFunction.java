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

import static org.apache.geode.connectors.jdbc.internal.cli.FunctionContextArgumentProvider.getMemberFromContext;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.xml.ElementType;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlGenerator;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class CreateConnectionFunction implements Function<ConnectionConfiguration>, InternalEntity {

  private static final String ID = CreateConnectionFunction.class.getName();

  private final FunctionContextArgumentProvider argumentProvider;
  private final ExceptionHandler exceptionHandler;

  public CreateConnectionFunction() {
    this(new FunctionContextArgumentProvider(), new ExceptionHandler());
  }

  private CreateConnectionFunction(FunctionContextArgumentProvider argumentProvider,
      ExceptionHandler exceptionHandler) {
    this.argumentProvider = argumentProvider;
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
  public void execute(FunctionContext<ConnectionConfiguration> context) {
    try {
      // input
      ConnectionConfiguration connectionConfig = context.getArguments();
      InternalJdbcConnectorService service = argumentProvider.getJdbcConnectorService(context);

      // action
      createConnectionConfig(service, connectionConfig);

      // output
      String member = argumentProvider.getMember(context);
      XmlEntity xmlEntity = argumentProvider.createXmlEntity(context);
      CliFunctionResult result = createSuccessResult(connectionConfig.getName(), member, xmlEntity);
      context.getResultSender().lastResult(result);

    } catch (Exception e) {
      exceptionHandler.handleException(context, e);
    }
  }

  /**
   * Creates the named connection configuration
   */
  void createConnectionConfig(InternalJdbcConnectorService service,
      ConnectionConfiguration connectionConfig) throws ConnectionConfigExistsException {
    service.createConnectionConfig(connectionConfig);
  }

  private CliFunctionResult createSuccessResult(String connectionName, String member,
      XmlEntity xmlEntity) {
    String message = "Created JDBC connection " + connectionName + " on " + member;
    return new CliFunctionResult(member, xmlEntity, message);
  }
}
