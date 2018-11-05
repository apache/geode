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
package org.apache.geode.connectors.jdbc.internal;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

@Experimental
public class JdbcConnectorServiceImpl implements JdbcConnectorService {

  private final Map<String, RegionMapping> mappingsByRegion =
      new ConcurrentHashMap<>();

  @Override
  public Set<RegionMapping> getRegionMappings() {
    Set<RegionMapping> regionMappings = new HashSet<>();
    regionMappings.addAll(mappingsByRegion.values());
    return regionMappings;
  }

  @Override
  public void createRegionMapping(RegionMapping mapping)
      throws RegionMappingExistsException {
    RegionMapping existing =
        mappingsByRegion.putIfAbsent(mapping.getRegionName(), mapping);
    if (existing != null) {
      throw new RegionMappingExistsException(
          "RegionMapping for region " + mapping.getRegionName() + " exists");
    }
  }

  @Override
  public void replaceRegionMapping(RegionMapping alteredMapping)
      throws RegionMappingNotFoundException {
    RegionMapping existingMapping =
        mappingsByRegion.get(alteredMapping.getRegionName());
    if (existingMapping == null) {
      throw new RegionMappingNotFoundException(
          "RegionMapping for the region " + alteredMapping.getRegionName() + " was not found");
    }

    mappingsByRegion.put(existingMapping.getRegionName(), alteredMapping);
  }

  @Override
  public RegionMapping getMappingForRegion(String regionName) {
    return mappingsByRegion.get(regionName);
  }

  @Override
  public void destroyRegionMapping(String regionName) {
    mappingsByRegion.remove(regionName);
  }

  @Override
  public void init(Cache cache) {}


  @Override
  public Class<? extends CacheService> getInterface() {
    return JdbcConnectorService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }
}
