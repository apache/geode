/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.configuration.mutators;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.configuration.RuntimeRegionConfig;

public class RegionConfigManager
    implements ConfigurationManager<RegionConfig, RuntimeRegionConfig> {
  private InternalCache cache;

  @VisibleForTesting
  RegionConfigManager() {}

  public RegionConfigManager(InternalCache cache) {
    this.cache = cache;
  }

  ManagementService getManagementService() {
    return ManagementService.getExistingManagementService(cache);
  }

  @Override
  public void add(RegionConfig configElement, CacheConfig existingConfig) {
    existingConfig.getRegions().add(configElement);
  }

  @Override
  public void update(RegionConfig config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented yet");
  }

  @Override
  public void delete(RegionConfig config, CacheConfig existing) {
    existing.getRegions().removeIf(i -> i.getId().equals(config.getId()));
  }

  @Override
  public List<RuntimeRegionConfig> list(RegionConfig filter, CacheConfig existing) {
    List<RegionConfig> staticRegionConfigs;
    if (StringUtils.isBlank(filter.getName())) {
      staticRegionConfigs = existing.getRegions();
    } else {
      staticRegionConfigs =
          existing.getRegions().stream().filter(r -> filter.getName().equals(r.getName())).collect(
              Collectors.toList());
    }

    List<RuntimeRegionConfig> results = new ArrayList<>();
    for (RegionConfig config : staticRegionConfigs) {
      DistributedRegionMXBean distributedRegionMXBean =
          getManagementService().getDistributedRegionMXBean("/" + config.getName());
      long entryCount = -1;
      if (distributedRegionMXBean != null) {
        entryCount = distributedRegionMXBean.getSystemRegionEntryCount();
      }
      RuntimeRegionConfig runtimeConfig = new RuntimeRegionConfig(config);
      runtimeConfig.setEntryCount(entryCount);
      results.add(runtimeConfig);
    }
    return results;
  }

  @Override
  public RegionConfig get(String id, CacheConfig existing) {
    return CacheElement.findElement(existing.getRegions(), id);
  }

  @Override
  public void checkCompatibility(RegionConfig incoming, String group, RegionConfig existing) {
    // if their types are the same, then they are compatible
    if (incoming.getType().equals(existing.getType())) {
      return;
    }

    // one has to be the proxy of the other's main type
    if (!incoming.getType().contains("PROXY") && !existing.getType().contains("PROXY")) {
      throw new IllegalArgumentException(getDescription(incoming) + " is not compatible with "
          + group + "'s existing regionConfig "
          + getDescription(existing));
    }

    // the beginning part of the type has to be the same
    String incomingType = incoming.getType().split("_")[0];
    String existingType = existing.getType().split("_")[0];
    if (!incomingType.equals(existingType)) {
      throw new IllegalArgumentException(
          getDescription(incoming) + " is not compatible with " + group + "'s existing "
              + getDescription(existing));
    }
  }

  private String getDescription(RegionConfig regionConfig) {
    return "Region " + regionConfig.getName() + " of type " + regionConfig.getType();
  }
}
