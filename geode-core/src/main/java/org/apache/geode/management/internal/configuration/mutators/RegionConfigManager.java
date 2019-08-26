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
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.configuration.converters.RegionConverter;

public class RegionConfigManager implements ConfigurationManager<Region> {

  private final RegionConverter converter = new RegionConverter();

  @Override
  public void add(Region configElement, CacheConfig existingConfig) {
    existingConfig.getRegions().add(converter.fromConfigObject(configElement));
  }

  @Override
  public void update(Region config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented yet");
  }

  @Override
  public void delete(Region config, CacheConfig existing) {
    existing.getRegions().removeIf(i -> i.getId().equals(config.getId()));
  }

  @Override
  public List<Region> list(Region filter, CacheConfig existing) {
    Stream<RegionConfig> stream = existing.getRegions().stream();
    if (StringUtils.isNotBlank(filter.getName())) {
      stream = stream.filter(r -> filter.getName().equals(r.getName()));
    }
    return stream.map(converter::fromXmlObject).filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public Region get(String id, CacheConfig existing) {
    return converter.fromXmlObject(Identifiable.find(existing.getRegions(), id));
  }

  @Override
  public void checkCompatibility(Region incoming, String group, Region existing) {
    // if their types are the same, then they are compatible
    if (incoming.getType().equals(existing.getType())) {
      return;
    }

    // one has to be the proxy of the other's main type
    if (!incoming.getType().name().contains("PROXY")
        && !existing.getType().name().contains("PROXY")) {
      raiseIncompatibilityError(incoming, group, existing);
    }

    // the beginning part of the type has to be the same
    String incomingType = incoming.getType().name().split("_")[0];
    String existingType = existing.getType().name().split("_")[0];
    if (!incomingType.equals(existingType)) {
      raiseIncompatibilityError(incoming, group, existing);
    }
  }

  private void raiseIncompatibilityError(Region incoming, String group,
      Region existing) {
    throw new IllegalArgumentException(getDescription(incoming) + " is not compatible with " + group
        + "'s existing " + getDescription(existing) + ".");
  }

  private String getDescription(Region regionConfig) {
    return "Region '" + regionConfig.getName() + "' of type '" + regionConfig.getType() + "'";
  }
}
