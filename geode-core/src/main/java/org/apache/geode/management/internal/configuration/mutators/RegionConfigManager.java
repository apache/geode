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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;

public class RegionConfigManager implements ConfigurationManager<RegionConfig> {

  public RegionConfigManager() {}

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
  public List<RegionConfig> list(RegionConfig filter, CacheConfig existing) {
    if (StringUtils.isBlank(filter.getName())) {
      return existing.getRegions();
    }
    return existing.getRegions().stream().filter(r -> filter.getName().equals(r.getName())).collect(
        Collectors.toList());
  }
}
