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
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class CreateMappingCommand extends SingleGfshCommand {
  static final String CREATE_MAPPING = "create jdbc-mapping";
  static final String CREATE_MAPPING__HELP =
      EXPERIMENTAL + "Create a mapping for a region for use with a JDBC database connection.";
  static final String CREATE_MAPPING__REGION_NAME = "region";
  static final String CREATE_MAPPING__REGION_NAME__HELP =
      "Name of the region the mapping is being created for.";
  static final String CREATE_MAPPING__PDX_NAME = "pdx-name";
  static final String CREATE_MAPPING__PDX_NAME__HELP =
      "Name of pdx class for which values will be written to the database.";
  static final String CREATE_MAPPING__TABLE_NAME = "table";
  static final String CREATE_MAPPING__TABLE_NAME__HELP =
      "Name of database table for values to be written to.";
  static final String CREATE_MAPPING__CONNECTION_NAME = "connection";
  static final String CREATE_MAPPING__CONNECTION_NAME__HELP = "Name of JDBC connection to use.";

  @CliCommand(value = CREATE_MAPPING, help = CREATE_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel createMapping(
      @CliOption(key = CREATE_MAPPING__REGION_NAME, mandatory = true,
          help = CREATE_MAPPING__REGION_NAME__HELP) String regionName,
      @CliOption(key = CREATE_MAPPING__CONNECTION_NAME, mandatory = true,
          help = CREATE_MAPPING__CONNECTION_NAME__HELP) String connectionName,
      @CliOption(key = CREATE_MAPPING__TABLE_NAME,
          help = CREATE_MAPPING__TABLE_NAME__HELP) String table,
      @CliOption(key = CREATE_MAPPING__PDX_NAME,
          help = CREATE_MAPPING__PDX_NAME__HELP) String pdxName) {
    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);
    RegionMapping mapping = new RegionMapping(regionName,
        pdxName, table, connectionName);

    // action
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new CreateMappingFunction(), mapping, targetMembers);

    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);
    result.setConfigObject(mapping);
    return result;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig cacheConfig, Object element) {
    RegionMapping newCacheElement = (RegionMapping) element;
    RegionMapping existingCacheElement = cacheConfig.findCustomRegionElement(
        newCacheElement.getRegionName(), newCacheElement.getId(), RegionMapping.class);

    if (existingCacheElement != null) {
      cacheConfig
          .getRegions()
          .stream()
          .filter(regionConfig -> regionConfig.getName().equals(newCacheElement.getRegionName()))
          .forEach(
              regionConfig -> regionConfig.getCustomRegionElements().remove(existingCacheElement));
    }

    cacheConfig
        .getRegions()
        .stream()
        .filter(regionConfig -> regionConfig.getName().equals(newCacheElement.getRegionName()))
        .forEach(regionConfig -> regionConfig.getCustomRegionElements().add(newCacheElement));
  }
}
