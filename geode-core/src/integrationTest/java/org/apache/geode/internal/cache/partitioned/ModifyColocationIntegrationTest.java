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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.RegionsTest;

@Category(RegionsTest.class)
public class ModifyColocationIntegrationTest {

  private InternalCache cache;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    closeCache();
  }

  /**
   * Test that a user is not allowed to change the colocation of a PR with persistent data.
   */
  @Test
  public void testModifyColocation() throws Exception {
    // Create PRs where region3 is colocated with region1.
    createColocatedPRs("region1");

    pause();

    // Close everything
    closeCache();

    // Restart colocated with "region2"
    Throwable thrown = catchThrowable(() -> createColocatedPRs("region2"));
    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot change colocated-with to")
        .hasMessageContaining(
            "because there is persistent data with different colocation. Previous configured value is");

    pause();

    // Close everything
    closeCache();

    // Restart colocated with region1.
    // Make sure we didn't screw anything up.
    createColocatedPRs("/region1");

    pause();

    // Close everything
    closeCache();

    // Restart uncolocated. We don't allow changing
    // from uncolocated to colocated.
    try {
      createColocatedPRs(null);
      fail("Should have received an illegal state exception");
    } catch (IllegalStateException expected) {
      // do nothing
    }

    pause();

    // Close everything
    closeCache();
  }

  /**
   * Create three PRs on a VM, named region1, region2, and region3. The colocated with attribute
   * describes which region region3 should be colocated with.
   */
  private void createColocatedPRs(String colocatedWith) {
    Cache cache = getCache();

    cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");

    {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setRedundantCopies(0);
      partitionAttributesFactory.setRecoveryDelay(-1);
      partitionAttributesFactory.setStartupRecoveryDelay(-1);

      RegionFactory regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
      regionFactory.setDiskStoreName("disk");
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      regionFactory.create("region1");
      regionFactory.create("region2");
    }

    {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setRedundantCopies(0);
      partitionAttributesFactory.setRecoveryDelay(-1);
      partitionAttributesFactory.setStartupRecoveryDelay(-1);
      if (colocatedWith != null) {
        partitionAttributesFactory.setColocatedWith(colocatedWith);
      }

      RegionFactory regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
      regionFactory.setDiskStoreName("disk");
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      regionFactory.create("region3");
    }
  }

  private InternalCache getCache() {
    if (cache == null) {
      cache = (InternalCache) new CacheFactory().set(LOCATORS, "").create();
    }
    return cache;
  }

  private void closeCache() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

  private File[] getDiskDirs() {
    return new File[] {temporaryFolder.getRoot()};
  }

  private void pause() throws InterruptedException {
    Thread.sleep(2_000);
  }
}
