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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class PartitionedRegionOverflowClearDUnitTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  VM server1;

  VM server2;

  VM accessor;

  File disk1;

  File disk2;

  private static final String OVERFLOW_REGION_NAME = "testOverflowRegion";

  private static final String DISK_STORE_NAME = "testDiskStore";

  public static final int NUM_ENTRIES = 1000;

  protected RegionShortcut getRegionShortCut() {
    return RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW;
  }

  @Before
  public void setUp() throws Exception {
    server1 = getVM(1);
    server2 = getVM(2);
    accessor = getVM(3);
    disk1 = temporaryFolder.newFolder();
    disk2 = temporaryFolder.newFolder();
  }

  @Test
  public void testClearWithOverflow() {
    initializeDataStores();
    accessor.invoke(this::populateRegion);
    accessor.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(OVERFLOW_REGION_NAME);
      region.clear();
    });
    server1.bounce();
//    server2.bounce();
    server1.invoke(() -> {
      Region region = createOverflowRegion(1);
      assertThat(region.size()).isEqualTo(0);
    });
//    server2.invoke(() -> {
//      Region region = createOverflowRegion(2);
//      assertThat(region.size()).isEqualTo(0);
//    });
  }

  private void initializeDataStores() {
    server1.invoke(() -> {
      createOverflowRegion(1);
    });
//    server2.invoke(() -> {
//      createOverflowRegion(2);
//    });
  }

  private Region createOverflowRegion(int server) {
    File disk;
    if (server == 1) {
      disk = new File(temporaryFolder.getRoot(), "disk1");
    }
    else {
      disk = new File(temporaryFolder.getRoot(), "disk2");
    }
    cacheRule.createCache();
    cacheRule.getCache().createDiskStoreFactory(new DiskStoreAttributes()).setDiskDirs(new File[] {disk}).create(DISK_STORE_NAME);
    return cacheRule.getCache().createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(1).create())
        .setDiskStoreName(DISK_STORE_NAME)
        .setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
        .create(OVERFLOW_REGION_NAME);
  }

  private void populateRegion() {
    cacheRule.createCache();
    cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(
        new PartitionAttributesFactory().setLocalMaxMemory(0).setRedundantCopies(1).create()).create(OVERFLOW_REGION_NAME);
    Region region = cacheRule.getCache().getRegion(OVERFLOW_REGION_NAME);
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put("key" + i, "value" + i));
  }

}
