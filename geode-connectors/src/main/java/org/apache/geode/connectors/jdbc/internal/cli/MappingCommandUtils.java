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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.Region.SEPARATOR_CHAR;

import java.util.ArrayList;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;

public class MappingCommandUtils {

  public static CacheConfig getCacheConfig(ConfigurationPersistenceService configService,
      String group)
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

  public static RegionConfig checkForRegion(String regionName, CacheConfig cacheConfig,
      String groupName)
      throws PreconditionException {
    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      String groupClause = "A region named " + regionName + " must already exist"
          + (!groupName.equals(ConfigurationPersistenceService.CLUSTER_CONFIG)
              ? " for group " + groupName + "." : ".");
      throw new PreconditionException(groupClause);
    }
    return regionConfig;
  }

  private static RegionConfig findRegionConfig(CacheConfig cacheConfig, String regionName) {
    return cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(regionName)).findFirst().orElse(null);
  }

  public static ArrayList<RegionMapping> getMappingsFromRegionConfig(CacheConfig cacheConfig,
      RegionConfig regionConfig,
      String group) {
    ArrayList<RegionMapping> results = new ArrayList<>();
    for (CacheElement element : regionConfig.getCustomRegionElements()) {
      if (element instanceof RegionMapping) {
        ((RegionMapping) element).setRegionName(regionConfig.getName());
        results.add((RegionMapping) element);
      }
    }
    return results;
  }

  public static boolean isMappingSynchronous(CacheConfig cacheConfig, RegionConfig regionConfig) {
    return findAsyncEventQueue(cacheConfig, regionConfig) == null;
  }

  public static CacheConfig.AsyncEventQueue findAsyncEventQueue(CacheConfig cacheConfig,
      RegionConfig regionConfig) {
    for (CacheConfig.AsyncEventQueue queue : cacheConfig.getAsyncEventQueues()) {
      if (queue.getId()
          .equals(createAsyncEventQueueName(regionConfig.getName()))) {
        return queue;
      }
    }
    return null;
  }

  public static boolean isAccessor(RegionAttributesType attributesType) {
    return attributesType.getDataPolicy() == RegionAttributesDataPolicy.EMPTY
        || (attributesType.getPartitionAttributes() != null
            && attributesType.getPartitionAttributes().getLocalMaxMemory() != null
            && attributesType.getPartitionAttributes().getLocalMaxMemory().equals("0"));
  }

  public static boolean isPartition(RegionAttributesType attributesType) {
    boolean isPartitioned = false;
    if (attributesType.getDataPolicy() != null) {
      isPartitioned = attributesType.getDataPolicy().isPartition();
    } else if (attributesType.getRefid() != null) {
      isPartitioned = RegionShortcut.valueOf(attributesType.getRefid()).isPartition();
    }
    return isPartitioned;
  }

  public static String createAsyncEventQueueName(String regionPath) {
    if (regionPath.startsWith(SEPARATOR)) {
      regionPath = regionPath.substring(1);
    }
    return "JDBC#" + regionPath.replace(SEPARATOR_CHAR, '_');
  }

}
