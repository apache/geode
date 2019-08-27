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

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

public class PersistentPartitionedRegionLargeVersionDUnitTest extends CacheTestCase {

  /**
   * Test that we can recover from disk files with versions larger than Integer.MAX_VALUE
   */
  @Test
  public void canRecoverWithLargeGCVersionInFiles() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    final DiskStoreID vm0_memberId = vm0.invoke(() -> {
      createRegion();
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region");
      region.put(0, 0);

      // Manually set the version in bucket 0 to be greater than Integer.MAX_VALUE
      RegionVersionVector vectorVector = region.getBucketRegion(0).getVersionVector();
      vectorVector.recordVersion(vectorVector.getOwnerId(), ((long) Integer.MAX_VALUE) + 10L);

      // Do an update, which will pick up the large version
      region.put(1, 0);

      // Do a destroy, and a tombstone gc, which will set the gc version to the large version
      region.destroy(1);
      getCache().getTombstoneService().forceBatchExpirationForTests(1);
      vectorVector = region.getBucketRegion(0).getVersionVector();
      assertThat(vectorVector.getCurrentVersion()).isEqualTo(((long) Integer.MAX_VALUE) + 12L);
      return (DiskStoreID) vectorVector.getOwnerId();
    });

    // Start a second member to copy the bucket
    vm1.invoke(() -> {
      createRegion();
      getCache().getResourceManager().createRebalanceFactory().start().getResults();
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region");
      RegionVersionVector vectorVector = region.getBucketRegion(0).getVersionVector();
      assertThat(vectorVector.getGCVersion(vm0_memberId))
          .isEqualTo(((long) Integer.MAX_VALUE) + 12L);
    });

    // Shutdown both members
    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    // Restart both members
    vm1.invoke(() -> {
      createRegion();
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region");
      RegionVersionVector vectorVector = region.getBucketRegion(0).getVersionVector();
      assertThat(vectorVector.getGCVersion(vm0_memberId))
          .isEqualTo(((long) Integer.MAX_VALUE) + 12L);
    });
    vm0.invoke(() -> {
      createRegion();
      getCache().getResourceManager().createRebalanceFactory().start().getResults();
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region");
      RegionVersionVector vectorVector = region.getBucketRegion(0).getVersionVector();
      assertThat(vectorVector.getCurrentVersion()).isEqualTo(((long) Integer.MAX_VALUE) + 12L);
    });
  }

  private void createRegion() {
    Cache cache = getCache();
    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(1).create())
        .create("region");
  }
}
