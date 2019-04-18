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

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.configuration.RuntimeRegionConfig;

public class RegionConfigManager
    implements ConfigurationManager<RegionConfig> {
  private InternalCache cache;
  private ManagementService managementService;

  public RegionConfigManager(InternalCache cache) {
    this.cache = cache;
    this.managementService = ManagementService.getExistingManagementService(cache);
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
    throw new NotImplementedException("Not implemented yet");
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
          managementService.getDistributedRegionMXBean("/" + config.getName());
      if (distributedRegionMXBean == null) {
        throw new IllegalStateException("Can't get the region mbean info for " + config.getName());
      }
      RuntimeRegionConfig runtimeConfig = new RuntimeRegionConfig(config);
      runtimeConfig.setEntryCount(distributedRegionMXBean.getSystemRegionEntryCount());
      results.add(runtimeConfig);
    }
    return results;
  }
}
