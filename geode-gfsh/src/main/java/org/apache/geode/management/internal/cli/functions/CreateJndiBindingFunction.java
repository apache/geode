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
package org.apache.geode.management.internal.cli.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.datasource.ConfigProperty;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.util.DriverJarUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.functions.CliFunctionResult.StatusState;

public class CreateJndiBindingFunction extends CliFunction<Object[]> {

  private static final Logger logger = LogService.getLogger();

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    Object[] arguments = context.getArguments();
    JndiBinding configuration = (JndiBinding) arguments[0];
    boolean creatingDataSource = (Boolean) arguments[1];

    final String TYPE_NAME;
    if (creatingDataSource) {
      TYPE_NAME = "data-source";
    } else {
      TYPE_NAME = "jndi-binding";
    }

    try {
      JNDIInvoker.mapDatasource(getParamsAsMap(configuration),
          convert(configuration.getConfigProperties()));
    } catch (Exception ex) {
      if (logger.isErrorEnabled()) {
        logger.error("create " + TYPE_NAME + " failed", ex);
      }
      return new CliFunctionResult(context.getMemberName(), StatusState.ERROR, ex.getMessage());
    }

    return new CliFunctionResult(context.getMemberName(), StatusState.OK,
        String.format("Created %s \"%s\" on \"%s\".", TYPE_NAME, configuration.getJndiName(),
            context.getMemberName()));
  }

  static Map<String, String> getParamsAsMap(JndiBindingsType.JndiBinding binding) {
    Map<String, String> params = new HashMap<>();
    params.put("blocking-timeout-seconds", binding.getBlockingTimeoutSeconds());
    params.put("conn-pooled-datasource-class", binding.getConnPooledDatasourceClass());
    params.put("connection-url", binding.getConnectionUrl());
    params.put("idle-timeout-seconds", binding.getIdleTimeoutSeconds());
    params.put("init-pool-size", binding.getInitPoolSize());
    params.put("jdbc-driver-class", binding.getJdbcDriverClass());
    params.put("jndi-name", binding.getJndiName());
    params.put("login-timeout-seconds", binding.getLoginTimeoutSeconds());
    params.put("managed-conn-factory-class", binding.getManagedConnFactoryClass());
    params.put("max-pool-size", binding.getMaxPoolSize());
    params.put("password", binding.getPassword());
    params.put("transaction-type", binding.getTransactionType());
    params.put("type", binding.getType());
    params.put("user-name", binding.getUserName());
    params.put("xa-datasource-class", binding.getXaDatasourceClass());
    return params;
  }

  DriverJarUtil getDriverJarUtil() {
    return new DriverJarUtil();
  }

  static List<ConfigProperty> convert(
      List<JndiBindingsType.JndiBinding.ConfigProperty> properties) {
    return properties.stream().map(p -> {
      ConfigProperty prop = new ConfigProperty();
      prop.setName(p.getName());
      prop.setType(p.getType());
      prop.setValue(p.getValue());
      return prop;
    }).collect(Collectors.toList());
  }
}
