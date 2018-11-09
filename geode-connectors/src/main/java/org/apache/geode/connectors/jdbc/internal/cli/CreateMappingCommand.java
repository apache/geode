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
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcAsyncWriter;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
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
  static final String CREATE_MAPPING__DATA_SOURCE_NAME = "data-source";
  static final String CREATE_MAPPING__DATA_SOURCE_NAME__HELP = "Name of JDBC data source to use.";

  @CliCommand(value = CREATE_MAPPING, help = CREATE_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel createMapping(
      @CliOption(key = CREATE_MAPPING__REGION_NAME, mandatory = true,
          help = CREATE_MAPPING__REGION_NAME__HELP) String regionName,
      @CliOption(key = CREATE_MAPPING__DATA_SOURCE_NAME, mandatory = true,
          help = CREATE_MAPPING__DATA_SOURCE_NAME__HELP) String dataSourceName,
      @CliOption(key = CREATE_MAPPING__TABLE_NAME,
          help = CREATE_MAPPING__TABLE_NAME__HELP) String table,
      @CliOption(key = CREATE_MAPPING__PDX_NAME, mandatory = true,
          help = CREATE_MAPPING__PDX_NAME__HELP) String pdxName) {
    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);
    RegionMapping mapping = new RegionMapping(regionName,
        pdxName, table, dataSourceName);

    // action
    ConfigurationPersistenceService configurationPersistenceService =
        getConfigurationPersistenceService();
    if (configurationPersistenceService == null) {
      return ResultModel.createError("Cluster Configuration must be enabled.");
    }

    CacheConfig cacheConfig = configurationPersistenceService.getCacheConfig(null);

    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      return ResultModel
          .createError("A region named " + regionName + " must already exist.");
    }

    if (regionConfig.getCustomRegionElements().stream()
        .anyMatch(element -> element instanceof RegionMapping)) {
      return ResultModel
          .createError("A jdbc-mapping for " + regionName + " already exists.");
    }

    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes().stream()
        .filter(attributes -> attributes.getCacheLoader() != null).findFirst().orElse(null);
    if (regionAttributes != null) {
      DeclarableType loaderDeclarable = regionAttributes.getCacheLoader();
      if (loaderDeclarable != null) {
        return ResultModel
            .createError("The existing region " + regionName
                + " must not already have a cache-loader, but it has "
                + loaderDeclarable.getClassName());
      }
    }
    String queueName = getAsyncEventQueueName(regionName);
    AsyncEventQueue asyncEventQueue = cacheConfig.getAsyncEventQueues().stream()
        .filter(queue -> queue.getId().equals(queueName)).findFirst().orElse(null);
    if (asyncEventQueue != null) {
      return ResultModel
          .createError("An async-event-queue named " + queueName + " must not already exist.");
    }


    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new CreateMappingFunction(), mapping, targetMembers);

    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);
    result.setConfigObject(mapping);
    return result;
  }

  static String getAsyncEventQueueName(String regionName) {
    return "JDBC-" + regionName;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig cacheConfig, Object element) {
    RegionMapping newCacheElement = (RegionMapping) element;
    String regionName = newCacheElement.getRegionName();
    String queueName = getAsyncEventQueueName(regionName);
    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      return;
    }
    RegionAttributesType attributes = getRegionAttributes(regionConfig);
    addMappingToRegion(newCacheElement, regionConfig);
    createAsyncQueue(cacheConfig, attributes, queueName);
    alterRegion(queueName, attributes);
  }

  private void alterRegion(String queueName, RegionAttributesType attributes) {
    setCacheLoader(attributes);
    addAsyncEventQueueId(queueName, attributes);
  }

  private void addMappingToRegion(RegionMapping newCacheElement, RegionConfig regionConfig) {
    regionConfig.getCustomRegionElements().add(newCacheElement);
  }

  private RegionConfig findRegionConfig(CacheConfig cacheConfig, String regionName) {
    return cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(regionName)).findFirst().orElse(null);
  }

  private void createAsyncQueue(CacheConfig cacheConfig, RegionAttributesType attributes,
      String queueName) {
    AsyncEventQueue asyncEventQueue = new AsyncEventQueue();
    asyncEventQueue.setId(queueName);
    boolean isPartitioned = attributes.getPartitionAttributes() != null;
    asyncEventQueue.setParallel(isPartitioned);
    DeclarableType listener = new DeclarableType();
    listener.setClassName(JdbcAsyncWriter.class.getName());
    asyncEventQueue.setAsyncEventListener(listener);
    cacheConfig.getAsyncEventQueues().add(asyncEventQueue);
  }

  private void addAsyncEventQueueId(String queueName, RegionAttributesType attributes) {
    String asyncEventQueueList = attributes.getAsyncEventQueueIds();
    if (asyncEventQueueList == null) {
      asyncEventQueueList = "";
    }
    if (!asyncEventQueueList.contains(queueName)) {
      if (asyncEventQueueList.length() > 0) {
        asyncEventQueueList += ',';
      }
      asyncEventQueueList += queueName;
      attributes.setAsyncEventQueueIds(asyncEventQueueList);
    }
  }

  private void setCacheLoader(RegionAttributesType attributes) {
    DeclarableType loader = new DeclarableType();
    loader.setClassName(JdbcLoader.class.getName());
    attributes.setCacheLoader(loader);
  }

  private RegionAttributesType getRegionAttributes(RegionConfig regionConfig) {
    RegionAttributesType attributes;
    List<RegionAttributesType> attributesList = regionConfig.getRegionAttributes();
    if (attributesList.isEmpty()) {
      attributes = new RegionAttributesType();
      attributesList.add(attributes);
    } else {
      attributes = attributesList.get(0);
    }
    return attributes;
  }
}
