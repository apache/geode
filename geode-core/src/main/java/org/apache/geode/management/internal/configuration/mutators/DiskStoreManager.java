/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.management.internal.configuration.mutators;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.internal.configuration.converters.DiskStoreConverter;

public class DiskStoreManager extends CacheConfigurationManager<DiskStore> {
  private final DiskStoreConverter diskStoreConverter = new DiskStoreConverter();

  public DiskStoreManager(ConfigurationPersistenceService service) {
    super(service);
  }

  @Override
  public void add(DiskStore config, CacheConfig existing) {
    List<DiskStoreType> diskStoreTypes = existing.getDiskStores();
    if (diskStoreTypes.stream().noneMatch(diskStoreType -> diskStoreType.getName()
        .equals(config.getName()))) {
      diskStoreTypes.add(diskStoreConverter.fromConfigObject(config));
    }
  }

  @Override
  public void update(DiskStore config, CacheConfig existing) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void delete(DiskStore config, CacheConfig existing) {
    existing.getDiskStores().stream()
        .filter(diskStoreType -> config.getName().equals(diskStoreType.getName())).findFirst()
        .ifPresent(diskStore -> existing.getDiskStores().remove(diskStore));
  }

  @Override
  public List<DiskStore> list(DiskStore filterConfig, CacheConfig existing) {
    return existing.getDiskStores().stream()
        .filter(diskStoreType -> StringUtils.isEmpty(filterConfig.getName())
            || filterConfig.getName().equals(diskStoreType.getName()))
        .map(diskStoreConverter::fromXmlObject).collect(Collectors.toList());
  }

  @Override
  public DiskStore get(DiskStore config, CacheConfig existing) {
    return existing.getDiskStores().stream()
        .filter(diskStoreType -> diskStoreType.getName().equals(config.getName())).map(
            diskStoreConverter::fromXmlObject)
        .findFirst().orElse(null);
  }
}
