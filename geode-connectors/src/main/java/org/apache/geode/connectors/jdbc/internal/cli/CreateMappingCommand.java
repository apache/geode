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

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcAsyncWriter;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.JdbcWriter;
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
      EXPERIMENTAL + "Create a JDBC mapping for a region for use with a JDBC database.";
  static final String CREATE_MAPPING__REGION_NAME = "region";
  static final String CREATE_MAPPING__REGION_NAME__HELP =
      "Name of the region the JDBC mapping is being created for.";
  static final String CREATE_MAPPING__PDX_NAME = "pdx-name";
  static final String CREATE_MAPPING__PDX_NAME__HELP =
      "Name of pdx class for which values will be written to the database.";
  static final String CREATE_MAPPING__TABLE_NAME = "table";
  static final String CREATE_MAPPING__TABLE_NAME__HELP =
      "Name of database table for values to be written to.";
  static final String CREATE_MAPPING__DATA_SOURCE_NAME = "data-source";
  static final String CREATE_MAPPING__DATA_SOURCE_NAME__HELP = "Name of JDBC data source to use.";
  static final String CREATE_MAPPING__SYNCHRONOUS_NAME = "synchronous";
  static final String CREATE_MAPPING__SYNCHRONOUS_NAME__HELP =
      "By default, writes will be asynchronous. If true, writes will be synchronous.";
  static final String CREATE_MAPPING__ID_NAME = "id";
  static final String CREATE_MAPPING__ID_NAME__HELP =
      "The table column names to use as the region key for this JDBC mapping. If more than one column name is given then they must be separated by commas.";

  public static String createAsyncEventQueueName(String regionPath) {
    if (regionPath.startsWith("/")) {
      regionPath = regionPath.substring(1);
    }
    return "JDBC#" + regionPath.replace('/', '_');
  }

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
          help = CREATE_MAPPING__PDX_NAME__HELP) String pdxName,
      @CliOption(key = CREATE_MAPPING__SYNCHRONOUS_NAME,
          help = CREATE_MAPPING__SYNCHRONOUS_NAME__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean synchronous,
      @CliOption(key = CREATE_MAPPING__ID_NAME,
          help = CREATE_MAPPING__ID_NAME__HELP) String id) {
    if (regionName.startsWith("/")) {
      regionName = regionName.substring(1);
    }

    // input
    Set<DistributedMember> targetMembers = findMembersForRegion(regionName);
    RegionMapping mapping = new RegionMapping(regionName, pdxName, table, dataSourceName, id);

    try {
      ConfigurationPersistenceService configurationPersistenceService =
          checkForClusterConfiguration();
      CacheConfig cacheConfig = configurationPersistenceService.getCacheConfig(null);
      RegionConfig regionConfig = checkForRegion(regionName, cacheConfig);
      checkForExistingMapping(regionName, regionConfig);
      checkForCacheLoader(regionName, regionConfig);
      checkForCacheWriter(regionName, synchronous, regionConfig);
      checkForAsyncQueue(regionName, synchronous, cacheConfig);
    } catch (PreconditionException ex) {
      return ResultModel.createError(ex.getMessage());
    }

    // action
    Object[] arguments = new Object[] {mapping, synchronous};
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new CreateMappingFunction(), arguments, targetMembers);

    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);
    result.setConfigObject(arguments);
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

  private RegionConfig checkForRegion(String regionName, CacheConfig cacheConfig)
      throws PreconditionException {
    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      throw new PreconditionException("A region named " + regionName + " must already exist.");
    }
    return regionConfig;
  }

  private void checkForExistingMapping(String regionName, RegionConfig regionConfig)
      throws PreconditionException {
    if (regionConfig.getCustomRegionElements().stream()
        .anyMatch(element -> element instanceof RegionMapping)) {
      throw new PreconditionException("A JDBC mapping for " + regionName + " already exists.");
    }
  }

  private void checkForCacheLoader(String regionName, RegionConfig regionConfig)
      throws PreconditionException {
    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
    if (regionAttributes != null) {
      DeclarableType loaderDeclarable = regionAttributes.getCacheLoader();
      if (loaderDeclarable != null) {
        throw new PreconditionException("The existing region " + regionName
            + " must not already have a cache-loader, but it has "
            + loaderDeclarable.getClassName());
      }
    }
  }

  private void checkForCacheWriter(String regionName, boolean synchronous,
      RegionConfig regionConfig) throws PreconditionException {
    if (synchronous) {
      RegionAttributesType writerAttributes = regionConfig.getRegionAttributes();
      if (writerAttributes != null) {
        DeclarableType writerDeclarable = writerAttributes.getCacheWriter();
        if (writerDeclarable != null) {
          throw new PreconditionException("The existing region " + regionName
              + " must not already have a cache-writer, but it has "
              + writerDeclarable.getClassName());
        }
      }
    }
  }

  private void checkForAsyncQueue(String regionName, boolean synchronous, CacheConfig cacheConfig)
      throws PreconditionException {
    if (!synchronous) {
      String queueName = createAsyncEventQueueName(regionName);
      AsyncEventQueue asyncEventQueue = cacheConfig.getAsyncEventQueues().stream()
          .filter(queue -> queue.getId().equals(queueName)).findFirst().orElse(null);
      if (asyncEventQueue != null) {
        throw new PreconditionException(
            "An async-event-queue named " + queueName + " must not already exist.");
      }
    }
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig cacheConfig, Object element) {
    Object[] arguments = (Object[]) element;
    RegionMapping regionMapping = (RegionMapping) arguments[0];
    boolean synchronous = (Boolean) arguments[1];
    String regionName = regionMapping.getRegionName();
    String queueName = createAsyncEventQueueName(regionName);
    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      return false;
    }

    RegionAttributesType attributes = getRegionAttribute(regionConfig);
    addMappingToRegion(regionMapping, regionConfig);
    if (!synchronous) {
      createAsyncQueue(cacheConfig, attributes, queueName);
    }
    alterRegion(queueName, attributes, synchronous);

    return true;
  }

  private RegionAttributesType getRegionAttribute(RegionConfig config) {
    if (config.getRegionAttributes() == null) {
      config.setRegionAttributes(new RegionAttributesType());
    }

    return config.getRegionAttributes();
  }

  @CliAvailabilityIndicator({CREATE_MAPPING})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }

  private void alterRegion(String queueName, RegionAttributesType attributes, boolean synchronous) {
    setCacheLoader(attributes);
    if (synchronous) {
      setCacheWriter(attributes);
    } else {
      addAsyncEventQueueId(queueName, attributes);
    }
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
    boolean isPartitioned = attributes.getDataPolicy().equals(RegionAttributesDataPolicy.PARTITION)
        || attributes.getDataPolicy().equals(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
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

  private void setCacheWriter(RegionAttributesType attributes) {
    DeclarableType writer = new DeclarableType();
    writer.setClassName(JdbcWriter.class.getName());
    attributes.setCacheWriter(writer);
  }
}
