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

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand.DATASOURCE_TYPE;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ListDataSourceCommand extends GfshCommand {
  static final String LIST_DATA_SOURCE = "list data-source";
  private static final String LIST_DATA_SOURCE__HELP = EXPERIMENTAL +
      "List each existing data source.";

  static final String DATA_SOURCE_PROPERTIES_SECTION = "data-source-properties";

  @CliCommand(value = LIST_DATA_SOURCE, help = LIST_DATA_SOURCE__HELP)
  @CliMetaData
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listDataSources() {

    ResultModel resultModel = new ResultModel();
    resultModel.setHeader(EXPERIMENTAL);
    TabularResultModel tabularData = resultModel.addTable(DATA_SOURCE_PROPERTIES_SECTION);
    tabularData.setColumnHeader("name", "pooled", "in use", "url");

    ConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    if (ccService == null) {
      return ResultModel.createError("Cluster configuration service must be enabled.");
    }
    CacheConfig cacheConfig = ccService.getCacheConfig(null);
    if (cacheConfig == null) {
      return ResultModel.createInfo("No data sources found");
    }

    cacheConfig.getJndiBindings().stream()
        .forEach(dataSource -> addDataSourceToResult(dataSource, cacheConfig, tabularData));

    return resultModel;
  }

  private void addDataSourceToResult(JndiBindingsType.JndiBinding binding, CacheConfig cacheConfig,
      TabularResultModel tabularData) {
    boolean pooled;
    String type = binding.getType();
    if (DATASOURCE_TYPE.SIMPLE.getType().equals(type)) {
      pooled = false;
    } else if (DATASOURCE_TYPE.POOLED.getType().equals(type)) {
      pooled = true;
    } else {
      // skip this binding since it was not created as a data-source
      return;
    }

    String dataSourceName = binding.getJndiName();
    tabularData.addRow(dataSourceName, Boolean.toString(pooled),
        Boolean.toString(isDataSourceUsedByRegion(cacheConfig, dataSourceName)),
        binding.getConnectionUrl());

  }

  boolean isDataSourceUsedByRegion(CacheConfig cacheConfig, String dataSourceName) {
    return cacheConfig.getRegions().stream()
        .anyMatch(regionConfig -> hasJdbcMappingThatUsesDataSource(regionConfig, dataSourceName));
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

  @CliAvailabilityIndicator({LIST_DATA_SOURCE})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }
}
