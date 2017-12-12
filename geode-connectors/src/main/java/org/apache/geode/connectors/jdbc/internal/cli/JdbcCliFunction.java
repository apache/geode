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
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

@Experimental
public abstract class JdbcCliFunction<T1, T2> implements Function<T1>, InternalEntity {

  private final FunctionContextArgumentProvider argumentProvider;
  private final ExceptionHandler exceptionHandler;

  JdbcCliFunction(FunctionContextArgumentProvider argumentProvider,
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
    return this.getClass().getName();
  }

  @Override
  public void execute(FunctionContext<T1> context) {
    try {
      InternalJdbcConnectorService service = argumentProvider.getJdbcConnectorService(context);
      T2 result = getFunctionResult(service, context);
      context.getResultSender().lastResult(result);
    } catch (Exception e) {
      exceptionHandler.handleException(context, e);
    }
  }

  String getMember(FunctionContext<T1> context) {
    return argumentProvider.getMember(context);
  }

  XmlEntity createXmlEntity(FunctionContext<T1> context) {
    return argumentProvider.createXmlEntity(context);
  }

  abstract T2 getFunctionResult(InternalJdbcConnectorService service, FunctionContext<T1> context)
      throws Exception;
}
