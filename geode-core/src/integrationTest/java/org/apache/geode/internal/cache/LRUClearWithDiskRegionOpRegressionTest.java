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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Regression test to confirm fix for TRAC #37244.
 *
 * <p>
 * TRAC #37244: deadlock between a concurrent LRUList clear and a region operation where diskRegions
 * are involved
 *
 * @see org.apache.geode.internal.cache.eviction.TestLRUListWithAsyncSorting
 */
@Category(IntegrationTest.class)
public class LRUClearWithDiskRegionOpRegressionTest {

  private InternalCache cache;
  private Region<Integer, Integer> region;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    String regionName = testName.getMethodName();
    File dir = temporaryFolder.newFolder(testName.getMethodName());

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0").create();

    AttributesFactory<Integer, Integer> factory = new AttributesFactory();

    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirsAndSizes(new File[] {dir}, new int[] {Integer.MAX_VALUE});
    diskStoreFactory.setAutoCompact(false);

    DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = true;
    try {
      factory.setDiskStoreName(diskStoreFactory.create(regionName).getName());
    } finally {
      DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = false;
    }

    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDiskSynchronous(true);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));

    RegionAttributes<Integer, Integer> regionAttributes = factory.create();

    InternalRegionArguments args = new InternalRegionArguments().setDestroyLockFlag(true)
        .setRecreateFlag(false).setSnapshotInputStream(null).setImageTarget(null);

    DistributedRegion distributedRegion =
        new DistributedRegion(regionName, regionAttributes, null, cache, args);

    region = cache.createVMRegion(regionName, regionAttributes,
        new InternalRegionArguments().setInternalMetaRegion(distributedRegion)
            .setDestroyLockFlag(true).setSnapshotInputStream(null).setImageTarget(null));
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  public void testPutWhileClear() throws Exception {
    // put two entries into the region
    for (int i = 0; i < 2; i++) {
      region.put(i, i);
    }

    // check for entry value
    assertThat(region.get(0)).isEqualTo(0);
  }

}
