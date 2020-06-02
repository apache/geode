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
package org.apache.geode.internal.cache.partitioned.colocation;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.cache.GemFireCacheImpl.addCacheLifecycleListener;
import static org.apache.geode.internal.cache.GemFireCacheImpl.removeCacheLifecycleListener;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.CacheLifecycleListener;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.RegionsTest;

@Category(RegionsTest.class)
public class ModifyColocationIntegrationTest {

  private final AtomicBoolean cacheExists = new AtomicBoolean();

  private CacheLifecycleListener cacheLifecycleListener;
  private InternalCache cache;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    cacheLifecycleListener = new CacheLifecycleListener() {
      @Override
      public void cacheCreated(InternalCache cache) {
        cacheExists.set(true);
      }

      @Override
      public void cacheClosed(InternalCache cache) {
        cacheExists.set(false);
      }
    };
    addCacheLifecycleListener(cacheLifecycleListener);
  }

  @After
  public void tearDown() {
    removeCacheLifecycleListener(cacheLifecycleListener);
    cache.close();
  }

  /**
   * Test that a user is not allowed to change the colocation of a PR with persistent data.
   */
  @Test
  public void testModifyColocation() {
    // Create PRs where region3 is colocated with region1.
    createCacheAndColocatedPRs("region1");

    // Close everything
    cache.close();

    // Restart colocated with "region2"
    Throwable thrown = catchThrowable(() -> createCacheAndColocatedPRs("region2"));
    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("For partition region " + SEPARATOR + "region3")
        .hasMessageContaining("cannot change colocated-with to \"" + SEPARATOR + "region2\"")
        .hasMessageContaining("because there is persistent data with different colocation.")
        .hasMessageContaining("Previous configured value is \"" + SEPARATOR + "region1\"");

    // await cache close is complete
    awaitCacheClose();

    // Restart colocated with region1. Make sure we didn't screw anything up.
    createCacheAndColocatedPRs("region1");

    // Close everything
    cache.close();

    // Restart uncolocated. We don't allow changing from colocated to uncolocated.
    thrown = catchThrowable(() -> createCacheAndColocatedPRs(null));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("For partition region " + SEPARATOR + "region3")
        .hasMessageContaining("cannot change colocated-with to \"\"")
        .hasMessageContaining("because there is persistent data with different colocation.")
        .hasMessageContaining("Previous configured value is \"" + SEPARATOR + "region1\"");

    awaitCacheClose();
  }

  /**
   * Create three PRs on a VM, named region1, region2, and region3. The colocated with attribute
   * describes which region region3 should be colocated with.
   */
  private void createCacheAndColocatedPRs(String colocatedWith) {
    cache = (InternalCache) new CacheFactory()
        .set(LOCATORS, "")
        .create();

    cache.createDiskStoreFactory()
        .setDiskDirs(new File[] {temporaryFolder.getRoot()})
        .create("disk");

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName("disk");
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create("region1");
    regionFactory.create("region2");

    partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);
    if (colocatedWith != null) {
      partitionAttributesFactory.setColocatedWith(colocatedWith);
    }

    regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName("disk");
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create("region3");
  }

  private void awaitCacheClose() {
    await().until(() -> !cacheExists.get());
  }
}
