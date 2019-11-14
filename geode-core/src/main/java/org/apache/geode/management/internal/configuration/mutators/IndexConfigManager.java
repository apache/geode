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

package org.apache.geode.management.internal.configuration.mutators;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.internal.configuration.converters.IndexConverter;

public class IndexConfigManager extends CacheConfigurationManager<Index> {
  private final IndexConverter converter = new IndexConverter();

  @Override
  public void add(Index config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented yet");
  }

  @Override
  public void update(Index config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented yet");
  }

  @Override
  public void delete(Index config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented yet");
  }

  @Override
  public List<Index> list(Index filterConfig, CacheConfig existing) {
    List<Index> result = new ArrayList<>();
    for (RegionConfig region : existing.getRegions()) {
      if (filterConfig.getRegionName() != null
          && !region.getName().equals(filterConfig.getRegionName())) {
        continue;
      }
      for (RegionConfig.Index index : region.getIndexes()) {
        if (filterConfig.getName() != null && !index.getName().equals(filterConfig.getName())) {
          continue;
        }
        Index matchingIndex = converter.fromXmlObject(index);
        result.add(matchingIndex);
      }
    }
    return result;
  }

  @Override
  public Index get(String id, CacheConfig existing) {
    throw new NotImplementedException("Not implemented yet");
  }
}
