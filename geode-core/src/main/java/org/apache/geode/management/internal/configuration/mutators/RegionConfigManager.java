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
import org.apache.geode.cache.configuration.CacheElement;
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
    existing.getRegions().removeIf(i -> i.getId().equals(config.getId()));
  }

  @Override
  public List<RegionConfig> list(RegionConfig filter, CacheConfig existing) {
    List<RegionConfig> regionConfigs;
    if (StringUtils.isBlank(filter.getName())) {
      regionConfigs = existing.getRegions();
    } else {
      regionConfigs =
          existing.getRegions().stream().filter(r -> filter.getName().equals(r.getName())).collect(
              Collectors.toList());
    }

    return regionConfigs;
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
      raiseIncompatibilityError(incoming, group, existing);
    }

    // the beginning part of the type has to be the same
    String incomingType = incoming.getType().split("_")[0];
    String existingType = existing.getType().split("_")[0];
    if (!incomingType.equals(existingType)) {
      raiseIncompatibilityError(incoming, group, existing);
    }
  }

  private void raiseIncompatibilityError(RegionConfig incoming, String group,
      RegionConfig existing) {
    throw new IllegalArgumentException(getDescription(incoming) + " is not compatible with " + group
        + "'s existing " + getDescription(existing) + ".");
  }

  private String getDescription(RegionConfig regionConfig) {
    return "Region '" + regionConfig.getName() + "' of type '" + regionConfig.getType() + "'";
  }
}
