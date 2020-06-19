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

package org.apache.geode.management.internal.configuration.realizers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.DiskStore;

public class DiskStoreRealizerTest {
  InternalCache cache;
  DiskStoreRealizer diskStoreRealizer;
  DiskStoreFactory diskStoreFactory;

  @Before
  public void init() {
    cache = mock(InternalCache.class);
    diskStoreFactory = mock(DiskStoreFactory.class);
    when(cache.createDiskStoreFactory(any())).thenReturn(diskStoreFactory);
    diskStoreRealizer = new DiskStoreRealizer();
  }


  @Test
  public void creatingDiskStoreWithNameShouldSucceed() throws Exception {
    DiskStore config = new DiskStore();
    String name = "diskStoreName";
    config.setName(name);
    config.setDirectories(new ArrayList());
    RealizationResult result = diskStoreRealizer.create(config, cache);
    verify(diskStoreFactory, times(1)).create(name);
    assertThat(result.getMessage())
        .isEqualTo("DiskStore " + config.getName() + " created successfully.");
    assertThat(result.isSuccess()).isTrue();
  }

  @Test
  public void existsReturnsTrueIfADiskStoreMatchesName() {
    DiskStore config = new DiskStore();
    String name = "diskStoreName";
    Collection<org.apache.geode.cache.DiskStore> diskStores = new ArrayList();
    org.apache.geode.cache.DiskStore existingDiskStore =
        mock(org.apache.geode.cache.DiskStore.class);
    when(existingDiskStore.getName()).thenReturn(name);
    diskStores.add(existingDiskStore);
    when(cache.listDiskStores()).thenReturn(diskStores);

    config.setName(name);
    assertThat(diskStoreRealizer.exists(config, cache)).isTrue();
  }

  @Test
  public void existsReturnsFalseIfADiskStoreMatchesName() {
    DiskStore config = new DiskStore();
    String name = "diskStoreName";
    Collection<org.apache.geode.cache.DiskStore> diskStores = new ArrayList();
    org.apache.geode.cache.DiskStore existingDiskStore =
        mock(org.apache.geode.cache.DiskStore.class);
    when(existingDiskStore.getName()).thenReturn("notTheDiskStoreYouAreLookingFor");
    diskStores.add(existingDiskStore);
    when(cache.listDiskStores()).thenReturn(diskStores);

    config.setName(name);
    assertThat(diskStoreRealizer.exists(config, cache)).isFalse();
  }

  @Test
  public void deletingDiskStoreShouldFailIfDiskStoreNotFound() throws Exception {
    DiskStore config = new DiskStore();
    String name = "diskStoreName";
    config.setName(name);
    config.setDirectories(new ArrayList());
    RealizationResult result = diskStoreRealizer.delete(config, cache);
    assertThat(result.getMessage()).isEqualTo("DiskStore " + config.getName() + " not found.");
    assertThat(result.isSuccess()).isFalse();
  }

  @Test
  public void deletingDiskStoreShouldSucceedIfDiskStoreFound() throws Exception {
    DiskStore config = new DiskStore();
    config.setDirectories(new ArrayList());
    String name = "diskStoreName";
    config.setName(name);
    when(cache.findDiskStore(name)).thenReturn(mock(org.apache.geode.cache.DiskStore.class));
    RealizationResult result = diskStoreRealizer.delete(config, cache);
    assertThat(result.getMessage())
        .isEqualTo("DiskStore " + config.getName() + " deleted successfully.");
    assertThat(result.isSuccess()).isTrue();
  }

}
