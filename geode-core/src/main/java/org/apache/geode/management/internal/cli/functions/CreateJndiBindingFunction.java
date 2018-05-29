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

import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.datasource.ConfigProperty;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class CreateJndiBindingFunction extends CliFunction<JndiBindingsType.JndiBinding> {

  static final String RESULT_MESSAGE =
      "Initiated jndi binding \"{0}\" on \"{1}\". See server logs to verify.";

  @Override
  public CliFunctionResult executeFunction(FunctionContext<JndiBindingsType.JndiBinding> context) {
    ResultSender<Object> resultSender = context.getResultSender();
    JndiBindingsType.JndiBinding configuration = context.getArguments();
    JNDIInvoker.mapDatasource(getParamsAsMap(configuration),
        convert(configuration.getConfigProperties()));

    return new CliFunctionResult(context.getMemberName(), true,
        CliStrings.format(RESULT_MESSAGE, configuration.getJndiName(), context.getMemberName()));
  }

  static Map getParamsAsMap(JndiBindingsType.JndiBinding binding) {
    Map params = new HashMap();
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
