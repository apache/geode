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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.management.configuration.DiskStore;

public class DiskStoreManagerTest {

  private CacheConfig cacheConfig;
  private DiskStore diskStore;
  private DiskStoreManager manager;

  @Before
  public void before() throws Exception {
    cacheConfig = new CacheConfig();
    diskStore = new DiskStore();
    diskStore.setName("diskStoreName");
    diskStore.setDirectories(new ArrayList());
    manager = new DiskStoreManager(null);
  }

  @Test
  public void addResultsInDiskStoreTypeAddedToCacheConfig() {
    manager.add(diskStore, cacheConfig);
    assertThat(cacheConfig.getDiskStores().size()).isEqualTo(1);
  }

  @Test
  public void deleteWhenDiskStoreTypeDoesNotExistShouldNotFail() {
    manager.delete(diskStore, cacheConfig);
    assertThat(cacheConfig.getDiskStores().size()).isEqualTo(0);
  }

  @Test
  public void deleteWhenDiskStoreExistsShouldSucceed() {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setName(diskStore.getName());
    cacheConfig.getDiskStores().add(diskStoreType);
    manager.delete(diskStore, cacheConfig);
    assertThat(cacheConfig.getDiskStores().size()).isEqualTo(0);
  }

  @Test
  public void unableToAddDuplicatesToCacheConfig() {
    manager.add(diskStore, cacheConfig);
    manager.add(diskStore, cacheConfig);
    assertThat(cacheConfig.getDiskStores().size()).isEqualTo(1);
  }

  @Test
  public void listShouldIncludeAllKnownDiskStores() {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setName(diskStore.getName());
    cacheConfig.getDiskStores().add(diskStoreType);
    List<DiskStore> diskStores = manager.list(diskStore, cacheConfig);
    assertThat(diskStores.size()).isEqualTo(1);
    assertThat(diskStores.contains(diskStoreType));
  }

  @Test
  public void getShouldReturnDiskStoreMatchingByName() {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setName(diskStore.getName());
    cacheConfig.getDiskStores().add(diskStoreType);
    DiskStore foundDiskStore = manager.get(diskStore, cacheConfig);
    assertThat(foundDiskStore).isNotNull();
    assertThat(foundDiskStore.getName()).isEqualTo(diskStoreType.getName());
  }

  @Test
  public void getShouldReturnNullIfNoDiskStoresExist() {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setName(diskStore.getName());
    DiskStore foundDiskStore = manager.get(diskStore, cacheConfig);
    assertThat(foundDiskStore).isNull();
  }

  @Test
  public void getShouldReturnNullIfDiskStoreDoesNotMatch() {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setName("notTheDiskStoreYouAreLookingFor");
    cacheConfig.getDiskStores().add(diskStoreType);
    DiskStore foundDiskStore = manager.get(diskStore, cacheConfig);
    assertThat(foundDiskStore).isNull();
  }
}
