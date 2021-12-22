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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

public class PartitionedRegionAttributesMutatorDUnitTest extends CacheTestCase {

  public static final int NUM_BUCKETS = 13;
  public static final int NUM_VMS = 3;
  private static final String COLOCATED = "colocated";
  private static final String BASE = "base";

  /**
   * This is a regression test for GEODE-6767 that we can still rebalance after altering certain
   * partitioned region attributes.
   *
   * Altering the region attributes changes metadata in the __PR region. If that metadata is
   * corrupted, rebalance may end up reducing redundancy due to the fact that
   * isColocationComplete=false
   * in the metadata.
   */
  @Test
  public void canStillRebalanceAfterAlteringEntryTimeToLive() {
    VM vm0 = VM.getVM(0);
    IntStream.range(0, NUM_VMS).forEach(this::createRegionsOnVM);

    vm0.invoke(this::createAllBuckets);

    // Alter the region
    vm0.invoke(this::alterRegion);

    // Move data.
    vm0.invoke(this::moveBuckets);

    // Check redundancy of all regions
    vm0.invoke(this::checkRedundancyLevel);
  }

  private void alterRegion() {
    Cache cache = getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(COLOCATED);
    region.getAttributesMutator().setEntryTimeToLive(new ExpirationAttributes(1000));
  }

  private void checkRedundancyLevel() {
    Cache cache = getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(COLOCATED);
    assertThat(region.getPrStats().getLowRedundancyBucketCount()).isEqualTo(0);
  }

  private void moveBuckets() throws InterruptedException {
    // Move some data, to force a rebalance
    Region<Object, Object> base = getCache().getRegion(BASE);
    Set<DistributedMember> allMembersForKey =
        PartitionRegionHelper.getAllMembersForKey(base, 0);

    Iterator<DistributedMember> memberIterator = allMembersForKey.iterator();
    DistributedMember member1 = memberIterator.next();
    DistributedMember member2 = memberIterator.next();

    RebalanceResults results = PartitionRegionHelper.moveData(base, member1, member2, 100);
    assertThat(results.getTotalBucketTransfersCompleted()).isGreaterThan(1);
  }

  private void createAllBuckets() {
    Cache cache = getCache();
    PartitionRegionHelper.assignBucketsToPartitions(cache.getRegion(BASE));
  }

  private void createRegionsOnVM(int i) {
    VM.getVM(i).invoke(this::createRegions);
  }

  private void createRegions() {
    Cache cache = getCache();
    {
      PartitionAttributes attributes = new PartitionAttributesFactory<>()
          .setRedundantCopies(1)
          .setTotalNumBuckets(NUM_BUCKETS)
          .create();

      cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
          .setStatisticsEnabled(true)
          .setPartitionAttributes(attributes)
          .create(BASE);
    }

    PartitionAttributes attributes = new PartitionAttributesFactory<>()
        .setColocatedWith(BASE)
        .setRedundantCopies(1)
        .setTotalNumBuckets(NUM_BUCKETS)
        .create();

    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setStatisticsEnabled(true)
        .setPartitionAttributes(attributes)
        .create(COLOCATED);
  }

}
