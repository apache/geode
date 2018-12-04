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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.JdbcWriter;
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
public class DestroyMappingCommand extends SingleGfshCommand {
  static final String DESTROY_MAPPING = "destroy jdbc-mapping";
  static final String DESTROY_MAPPING__HELP = EXPERIMENTAL + "Destroy the specified mapping.";
  static final String DESTROY_MAPPING__REGION_NAME = "region";
  static final String DESTROY_MAPPING__REGION_NAME__HELP = "Name of the region mapping to destroy.";

  @CliCommand(value = DESTROY_MAPPING, help = DESTROY_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel destroyMapping(@CliOption(key = DESTROY_MAPPING__REGION_NAME, mandatory = true,
      help = DESTROY_MAPPING__REGION_NAME__HELP) String regionName) {
    if (regionName.startsWith("/")) {
      regionName = regionName.substring(1);
    }

    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);

    // action
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new DestroyMappingFunction(), regionName, targetMembers);

    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);
    result.setConfigObject(regionName);
    return result;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig cacheConfig, Object configObject) {
    String regionName = (String) configObject;
    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      return false;
    }
    boolean modified = false;
    modified |= removeJdbcMappingFromRegion(regionConfig);
    modified |= removeJdbcQueueFromCache(cacheConfig, regionName);
    RegionAttributesType attributes = getRegionAttribute(regionConfig);
    modified |= removeJdbcLoader(attributes);
    modified |= removeJdbcWriter(attributes);
    modified |= removeJdbcAsyncEventQueueId(attributes, regionName);
    return modified;
  }

  private RegionAttributesType getRegionAttribute(RegionConfig config) {
    if (config.getRegionAttributes() == null) {
      config.setRegionAttributes(new RegionAttributesType());
    }

    return config.getRegionAttributes();
  }

  private boolean removeJdbcLoader(RegionAttributesType attributes) {
    DeclarableType cacheLoader = attributes.getCacheLoader();
    if (cacheLoader != null) {
      if (JdbcLoader.class.getName().equals(cacheLoader.getClassName())) {
        attributes.setCacheLoader(null);
        return true;
      }
    }
    return false;
  }

  private boolean removeJdbcWriter(RegionAttributesType attributes) {
    DeclarableType cacheWriter = attributes.getCacheWriter();
    if (cacheWriter != null) {
      if (JdbcWriter.class.getName().equals(cacheWriter.getClassName())) {
        attributes.setCacheWriter(null);
        return true;
      }
    }
    return false;
  }

  private boolean removeJdbcAsyncEventQueueId(RegionAttributesType attributes, String regionName) {
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    String queueIds = attributes.getAsyncEventQueueIds();
    if (queueIds == null) {
      return false;
    }
    List<String> queues = new ArrayList<>(Arrays.asList(queueIds.split(",")));
    if (queues.contains(queueName)) {
      queues.remove(queueName);
      String newQueueIds = String.join(",", queues);
      attributes.setAsyncEventQueueIds(newQueueIds);
      return true;
    }
    return false;
  }

  private boolean removeJdbcQueueFromCache(CacheConfig cacheConfig, String regionName) {
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    Iterator<AsyncEventQueue> iterator = cacheConfig.getAsyncEventQueues().iterator();
    while (iterator.hasNext()) {
      AsyncEventQueue queue = iterator.next();
      if (queueName.equals(queue.getId())) {
        iterator.remove();
        return true;
      }
    }
    return false;
  }

  private boolean removeJdbcMappingFromRegion(RegionConfig regionConfig) {
    Iterator<CacheElement> iterator = regionConfig.getCustomRegionElements().iterator();
    while (iterator.hasNext()) {
      CacheElement element = iterator.next();
      if (element instanceof RegionMapping) {
        iterator.remove();
        return true;
      }
    }
    return false;
  }

  private RegionConfig findRegionConfig(CacheConfig cacheConfig, String regionName) {
    return cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(regionName)).findFirst().orElse(null);
  }

  @CliAvailabilityIndicator({DESTROY_MAPPING})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }
}
