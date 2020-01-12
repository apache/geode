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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * Test to verify the meta-data cleanUp done at the time of cache close Op.
 */

public class PartitionedRegionCacheCloseDUnitTest extends CacheTestCase {

  private static final int TOTAL_NUM_BUCKETS = 5;
  private static final String REGION_NAME = "REGION";

  @Test
  public void partitionRegionConfigNodesMatchesRegionMembership() throws Exception {
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);

    // Create PRs on only 2 VMs
    vm0.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createPartitionedRegion());

    // Close all the PRs on vm2
    vm0.invoke(() -> getCache().close());

    vm1.invoke(() -> {
      Region prRootRegion = PartitionedRegionHelper.getPRRoot(getCache());

      await().untilAsserted(() -> {
        PartitionRegionConfig partitionRegionConfig =
            (PartitionRegionConfig) prRootRegion.get("#" + REGION_NAME);
        assertThat(partitionRegionConfig.getNodes()).hasSize(1);
      });
    });
  }

  private void createPartitionedRegion() {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setLocalMaxMemory(20);
    partitionAttributesFactory.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(REGION_NAME);
  }
}
