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

package org.apache.geode.management.internal.cli.commands;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.ListJndiBindingFunction;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeJndiBindingCommand extends GfshCommand {
  private static final Logger logger = LogService.getLogger();

  static final String DESCRIBE_JNDI_BINDING = "describe jndi-binding";
  private static final String DESCRIBE_JNDIBINDING__HELP =
      "Describe the configuration of the given jndi binding.";
  private static final Function LIST_BINDING_FUNCTION = new ListJndiBindingFunction();

  @CliCommand(value = DESCRIBE_JNDI_BINDING, help = DESCRIBE_JNDIBINDING__HELP)
  @CliMetaData
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result describeJndiBinding(@CliOption(key = "name", mandatory = true,
      help = "Name of the binding to describe") String bindingName) {
    Result result = null;
    TabularResultData tabularData = ResultBuilder.createTabularResultData();

    InternalClusterConfigurationService ccService = getSharedConfiguration();
    if (ccService != null) {
      CacheConfig cacheConfig = ccService.getCacheConfig("cluster");
      List<JndiBindingsType.JndiBinding> jndiBindings = cacheConfig.getJndiBindings();

      if (jndiBindings.size() == 0) {
        return ResultBuilder
            .createUserErrorResult(String.format("JNDI binding : %s not found", bindingName));
      }

      for (JndiBindingsType.JndiBinding binding : jndiBindings) {
        if (binding.getJndiName().equals(bindingName)
            || binding.getJndiName().equals("java:" + bindingName)) {
          addTableRow(tabularData, "type", binding.getType());
          addTableRow(tabularData, "jndi-name", binding.getJndiName());
          addTableRow(tabularData, "jdbc-driver-class", binding.getJdbcDriverClass());
          addTableRow(tabularData, "user-name", binding.getUserName());
          addTableRow(tabularData, "connection-url", binding.getConnectionUrl());

          if (!"SimpleDataSource".equals(binding.getType())) {
            if ("ManagedDataSource".equals(binding.getType())) {
              addTableRow(tabularData, "managed-conn-factory-class",
                  binding.getManagedConnFactoryClass());
            } else if ("PooledDataSource".equals(binding.getType())) {
              addTableRow(tabularData, "conn-pooled-datasource-class",
                  binding.getConnPooledDatasourceClass());
            } else if ("XAPooledDataSource".equals(binding.getType())) {
              addTableRow(tabularData, "xa-datasource-class", binding.getXaDatasourceClass());
            }

            addTableRow(tabularData, "init-pool-size", binding.getInitPoolSize());
            addTableRow(tabularData, "max-pool-size", binding.getMaxPoolSize());
            addTableRow(tabularData, "idle-timeout-seconds", binding.getIdleTimeoutSeconds());
            addTableRow(tabularData, "blocking-timeout-seconds",
                binding.getBlockingTimeoutSeconds());
            addTableRow(tabularData, "login-timeout-seconds", binding.getLoginTimeoutSeconds());
          }

          for (JndiBindingsType.JndiBinding.ConfigProperty confProp : binding.getConfigProperty()) {
            addTableRow(tabularData, confProp.getName(), confProp.getValue());
          }

          break;
        }
      }
    }

    result = ResultBuilder.buildResult(tabularData);

    return result;
  }

  private void addTableRow(TabularResultData table, String property, String value) {
    table.accumulate("Property", property);
    table.accumulate("Value", value != null ? value : "");
  }
}
