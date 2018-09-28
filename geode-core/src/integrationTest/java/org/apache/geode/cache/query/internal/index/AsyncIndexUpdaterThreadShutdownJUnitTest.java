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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.index.IndexManager.IndexUpdaterThread;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.OQLIndexTest;

/**
 * Test create a region (Replicated OR Partitioned) and sets index maintenance Asynchronous so that
 * {@link IndexManager} starts a new thread for index maintenance when region is populated. This
 * test verifies that after cache close {@link IndexUpdaterThread} is shutdown for each region
 * (Replicated/Bucket).
 *
 *
 */
@Category({OQLIndexTest.class})
public class AsyncIndexUpdaterThreadShutdownJUnitTest {

  String name = "PR_with_Async_Index";

  @Test
  public void testAsyncIndexUpdaterThreadShutdownForRR() {
    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setIndexMaintenanceSynchronous(false);
    Region localRegion = rf.create(name);

    assertNotNull("Region ref null", localRegion);

    try {
      cache.getQueryService().createIndex("idIndex", "ID", "/" + name);
    } catch (Exception e) {
      cache.close();
      e.printStackTrace();
      fail("Index creation failed");
    }

    for (int i = 0; i < 500; i++) {
      localRegion.put(i, new Portfolio(i));
    }

    InternalRegion internalRegion = (InternalRegion) localRegion;
    assertTrue(internalRegion.getIndexManager().getUpdaterThread().isAlive());

    localRegion.close();

    assertFalse(internalRegion.getIndexManager().getUpdaterThread().isAlive());

    cache.close();
  }

  @Test
  public void testAsyncIndexUpdaterThreadShutdownForPR() {
    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    rf.setIndexMaintenanceSynchronous(false);
    Region localRegion = rf.create(name);

    assertNotNull("Region ref null", localRegion);

    try {
      cache.getQueryService().createIndex("idIndex", "ID", "/" + name);
    } catch (Exception e) {
      cache.close();
      e.printStackTrace();
      fail("Index creation failed");
    }

    for (int i = 0; i < 500; i++) {
      localRegion.put(i, new Portfolio(i));
    }

    InternalRegion internalRegion = (InternalRegion) localRegion;
    assertTrue(internalRegion.getIndexManager().getUpdaterThread().isAlive());

    PartitionedRegion pr = (PartitionedRegion) localRegion;
    HashSet<BucketRegion> buckets = new HashSet<>(pr.getDataStore().getAllLocalBucketRegions());
    for (BucketRegion br : buckets) {
      assertTrue(br.getIndexManager().getUpdaterThread().isAlive());
    }

    localRegion.close();

    assertFalse(internalRegion.getIndexManager().getUpdaterThread().isAlive());
    for (BucketRegion br : buckets) {
      assertFalse(br.getIndexManager().getUpdaterThread().isAlive());
    }

    cache.close();
  }
}
