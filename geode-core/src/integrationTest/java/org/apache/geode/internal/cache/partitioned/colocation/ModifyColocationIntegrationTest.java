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

import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.RegionsTest;

@Category(RegionsTest.class)
public class ModifyColocationIntegrationTest {

  private InternalCache cache;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
    waitUntilCloseActuallyCompletes();
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
    waitUntilCloseActuallyCompletes();

    // Restart colocated with "region2"
    Throwable thrown = catchThrowable(() -> createCacheAndColocatedPRs("region2"));
    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("For partition region /region3")
        .hasMessageContaining("cannot change colocated-with to \"/region2\"")
        .hasMessageContaining("because there is persistent data with different colocation.")
        .hasMessageContaining("Previous configured value is \"/region1\"");

    // Close everything
    cache.close();
    waitUntilCloseActuallyCompletes();

    // Restart colocated with region1. Make sure we didn't screw anything up.
    createCacheAndColocatedPRs("region1");

    // Close everything
    cache.close();
    waitUntilCloseActuallyCompletes();

    // Restart uncolocated. We don't allow changing from colocated to uncolocated.
    thrown = catchThrowable(() -> createCacheAndColocatedPRs(null));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("For partition region /region3")
        .hasMessageContaining("cannot change colocated-with to \"\"")
        .hasMessageContaining("because there is persistent data with different colocation.")
        .hasMessageContaining("Previous configured value is \"/region1\"");
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

  private void waitUntilCloseActuallyCompletes() {
    await().until(() -> InternalDistributedSystem.getConnectedInstance() == null);
  }
}
