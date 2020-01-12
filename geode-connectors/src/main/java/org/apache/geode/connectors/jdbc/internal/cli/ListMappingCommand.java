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


import java.util.ArrayList;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ListMappingCommand extends GfshCommand {
  public static final String JDBC_MAPPINGS_SECTION = "jdbc-mappings";
  static final String LIST_MAPPING = "list jdbc-mappings";
  static final String LIST_MAPPING__HELP = EXPERIMENTAL + "Display JDBC mappings for all members.";
  static final String LIST_OF_MAPPINGS = "List of JDBC mappings";
  static final String NO_MAPPINGS_FOUND = "No JDBC mappings found";

  private static final String LIST_MAPPING__GROUPS_NAME__HELP =
      "Server Group(s) of the JDBC mappings to list.";

  @CliCommand(value = LIST_MAPPING, help = LIST_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel listMapping(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = LIST_MAPPING__GROUPS_NAME__HELP) String[] groups) {
    ArrayList<RegionMapping> mappings = new ArrayList<>();

    try {
      ConfigurationPersistenceService configService = checkForClusterConfiguration();
      if (groups == null) {
        groups = new String[] {ConfigurationPersistenceService.CLUSTER_CONFIG};
      }
      for (String group : groups) {
        CacheConfig cacheConfig = getCacheConfig(configService, group);
        for (RegionConfig regionConfig : cacheConfig.getRegions()) {
          mappings.addAll(
              MappingCommandUtils.getMappingsFromRegionConfig(cacheConfig, regionConfig, group));
        }
      }
    } catch (PreconditionException ex) {
      return ResultModel.createError(ex.getMessage());
    }

    // output
    ResultModel resultModel = new ResultModel();
    boolean mappingsExist =
        fillTabularResultData(mappings, resultModel.addTable(JDBC_MAPPINGS_SECTION));
    if (mappingsExist) {
      resultModel.setHeader(EXPERIMENTAL);
      return resultModel;
    } else {
      return ResultModel.createInfo(EXPERIMENTAL + "\n" + NO_MAPPINGS_FOUND);
    }
  }

  /**
   * Returns true if any connections exist
   */
  private boolean fillTabularResultData(ArrayList<RegionMapping> mappings,
      TabularResultModel tableModel) {
    if (mappings == null) {
      return false;
    }
    for (RegionMapping mapping : mappings) {
      tableModel.accumulate(LIST_OF_MAPPINGS, mapping.getRegionName());
    }
    return !mappings.isEmpty();
  }

  private CacheConfig getCacheConfig(ConfigurationPersistenceService configService, String group)
      throws PreconditionException {
    CacheConfig result = configService.getCacheConfig(group);
    if (result == null) {
      throw new PreconditionException(
          "Cache Configuration not found"
              + ((group.equals(ConfigurationPersistenceService.CLUSTER_CONFIG)) ? "."
                  : " for group " + group + "."));
    }
    return result;
  }

  private ConfigurationPersistenceService checkForClusterConfiguration()
      throws PreconditionException {
    ConfigurationPersistenceService result = getConfigurationPersistenceService();
    if (result == null) {
      throw new PreconditionException("Cluster Configuration must be enabled.");
    }
    return result;
  }

  @CliAvailabilityIndicator({LIST_MAPPING})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }
}
