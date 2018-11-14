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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class DescribeDataSourceCommand extends InternalGfshCommand {
  public static final String DATA_SOURCE_PROPERTIES_SECTION = "data-source-properties";
  private static final Logger logger = LogService.getLogger();
  static final String DESCRIBE_DATA_SOURCE = "describe data-source";
  private static final String DESCRIBE_DATA_SOURCE__HELP =
      "Describe the configuration of the given data source.";

  @CliCommand(value = DESCRIBE_DATA_SOURCE, help = DESCRIBE_DATA_SOURCE__HELP)
  @CliMetaData
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel describeDataSource(@CliOption(key = "name", mandatory = true,
      help = "Name of the data source to describe") String dataSourceName) {

    ResultModel resultModel = new ResultModel();
    TabularResultModel tabularData = resultModel.addTable(DATA_SOURCE_PROPERTIES_SECTION);

    InternalConfigurationPersistenceService ccService =
        (InternalConfigurationPersistenceService) getConfigurationPersistenceService();
    if (ccService == null) {
      return ResultModel.createError("Cluster configuration service must be enabled.");
    }
    CacheConfig cacheConfig = ccService.getCacheConfig("cluster");
    if (cacheConfig == null) {
      return ResultModel.createError("Cluster configuration is not available.");
    }

    List<JndiBindingsType.JndiBinding> jndiBindings = cacheConfig.getJndiBindings();
    JndiBindingsType.JndiBinding binding = jndiBindings.stream()
        .filter(b -> b.getJndiName().equals(dataSourceName)).findFirst().orElse(null);
    if (binding == null) {
      return ResultModel.createError(String.format("Data source: %s not found", dataSourceName));
    }
    boolean pooled;
    String type = binding.getType();
    if ("SimpleDataSource".equals(type)) {
      pooled = false;
    } else if ("PooledDataSource".equals(type)) {
      pooled = true;
    } else {
      return ResultModel.createError(String.format("Unknown data source type: %s", type));
    }

    addTableRow(tabularData, CreateDataSourceCommand.POOLED, Boolean.toString(pooled));
    addTableRow(tabularData, CreateDataSourceCommand.NAME, binding.getJndiName());
    addTableRow(tabularData, CreateDataSourceCommand.USERNAME, binding.getUserName());
    addTableRow(tabularData, CreateDataSourceCommand.URL, binding.getConnectionUrl());
    if (pooled) {
      addTableRow(tabularData, CreateDataSourceCommand.POOLED_DATA_SOURCE_FACTORY_CLASS,
          binding.getConnPooledDatasourceClass());
      for (JndiBindingsType.JndiBinding.ConfigProperty confProp : binding.getConfigProperties()) {
        addTableRow(tabularData, confProp.getName(), confProp.getValue());
      }
    }

    return resultModel;
  }

  private void addTableRow(TabularResultModel table, String property, String value) {
    table.accumulate("Property", property);
    table.accumulate("Value", value != null ? value : "");
  }
}
