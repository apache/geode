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

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand.DATASOURCE_TYPE;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class DescribeDataSourceCommand extends GfshCommand {
  static final String DESCRIBE_DATA_SOURCE = "describe data-source";
  private static final String DESCRIBE_DATA_SOURCE__HELP = EXPERIMENTAL +
      "Describe the configuration of the given data source.";

  static final String DATA_SOURCE_PROPERTIES_SECTION = "data-source-properties";
  static final String REGIONS_USING_DATA_SOURCE_SECTION = "regions-using-data-source";

  @CliCommand(value = DESCRIBE_DATA_SOURCE, help = DESCRIBE_DATA_SOURCE__HELP)
  @CliMetaData
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel describeDataSource(@CliOption(key = "name", mandatory = true,
      help = "Name of the data source to describe") String dataSourceName) {

    ResultModel resultModel = new ResultModel();
    resultModel.setHeader(EXPERIMENTAL);
    TabularResultModel tabularData = resultModel.addTable(DATA_SOURCE_PROPERTIES_SECTION);

    InternalConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    if (ccService == null) {
      return ResultModel.createError("Cluster configuration service must be enabled.");
    }
    CacheConfig cacheConfig = ccService.getCacheConfig(null);
    if (cacheConfig == null) {
      return ResultModel.createError(String.format("Data source: %s not found", dataSourceName));
    }

    List<JndiBindingsType.JndiBinding> jndiBindings = cacheConfig.getJndiBindings();
    JndiBindingsType.JndiBinding binding = jndiBindings.stream()
        .filter(b -> b.getJndiName().equals(dataSourceName)).findFirst().orElse(null);
    if (binding == null) {
      return ResultModel.createError(String.format("Data source: %s not found", dataSourceName));
    }
    boolean pooled;
    String type = binding.getType();
    if (DATASOURCE_TYPE.SIMPLE.getType().equals(type)) {
      pooled = false;
    } else if (DATASOURCE_TYPE.POOLED.getType().equals(type)) {
      pooled = true;
    } else {
      return ResultModel.createError(String.format("Unknown data source type: %s", type));
    }

    addTableRow(tabularData, CreateDataSourceCommand.NAME, binding.getJndiName());
    addTableRow(tabularData, CreateDataSourceCommand.URL, binding.getConnectionUrl());
    addTableRow(tabularData, CreateDataSourceCommand.USERNAME, binding.getUserName());
    addTableRow(tabularData, CreateDataSourceCommand.POOLED, Boolean.toString(pooled));
    if (pooled) {
      addTableRow(tabularData, CreateDataSourceCommand.POOLED_DATA_SOURCE_FACTORY_CLASS,
          binding.getConnPooledDatasourceClass());
      for (JndiBindingsType.JndiBinding.ConfigProperty confProp : binding.getConfigProperties()) {
        addTableRow(tabularData, confProp.getName(), confProp.getValue());
      }
    }

    InfoResultModel regionsUsingSection = resultModel.addInfo(REGIONS_USING_DATA_SOURCE_SECTION);
    List<String> regionsUsing = getRegionsThatUseDataSource(cacheConfig, dataSourceName);
    regionsUsingSection.setHeader("Regions Using Data Source:");
    if (regionsUsing.isEmpty()) {
      regionsUsingSection.addLine("no regions are using " + dataSourceName);
    } else {
      regionsUsingSection.setContent(regionsUsing);
    }

    return resultModel;
  }

  List<String> getRegionsThatUseDataSource(CacheConfig cacheConfig, String dataSourceName) {
    return cacheConfig.getRegions()
        .stream()
        .filter(regionConfig -> hasJdbcMappingThatUsesDataSource(regionConfig, dataSourceName))
        .map(RegionConfig::getName)
        .collect(Collectors.toList());
  }

  private boolean hasJdbcMappingThatUsesDataSource(RegionConfig regionConfig,
      String dataSourceName) {
    return regionConfig.getCustomRegionElements()
        .stream()
        .anyMatch(cacheElement -> isRegionMappingUsingDataSource(cacheElement, dataSourceName));
  }

  private boolean isRegionMappingUsingDataSource(CacheElement cacheElement, String dataSourceName) {
    if (!(cacheElement instanceof RegionMapping)) {
      return false;
    }
    RegionMapping regionMapping = (RegionMapping) cacheElement;
    return dataSourceName.equals(regionMapping.getDataSourceName());
  }

  private void addTableRow(TabularResultModel table, String property, String value) {
    table.accumulate("Property", property);
    table.accumulate("Value", value != null ? value : "");
  }

  @CliAvailabilityIndicator({DESCRIBE_DATA_SOURCE})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }
}
