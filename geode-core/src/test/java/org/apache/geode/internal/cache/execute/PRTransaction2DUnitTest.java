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
package org.apache.geode.internal.cache.execute;

import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.*;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class PRTransaction2DUnitTest extends JUnit4CacheTestCase {
  Host host = Host.getHost(0);
  VM dataStore1 = host.getVM(0);
  VM dataStore2 = host.getVM(1);

  @Test
  public void testSizeOpOnLocalRegionInTransaction() {
    String regionName = "region";
    String region2Name = "region2";
    int totalBuckets = 2;
    boolean isSecondRegionLocal = true;
    setupRegions(totalBuckets, regionName, isSecondRegionLocal, region2Name);

    dataStore1.invoke(() -> verifySizeOpTransaction(2, regionName, totalBuckets, region2Name,
        isSecondRegionLocal));
  }

  @Test
  public void testSizeOpOnReplicateRegionInTransaction() {
    String regionName = "region";
    String region2Name = "region2";
    int totalBuckets = 2;
    boolean isSecondRegionLocal = false;
    setupRegions(totalBuckets, regionName, isSecondRegionLocal, region2Name);

    dataStore1.invoke(() -> verifySizeOpTransaction(2, regionName, totalBuckets, region2Name,
        isSecondRegionLocal));
  }

  private void setupRegions(int totalBuckets, String regionName, boolean isLocal,
      String region2Name) {
    createPRAndInitABucketOnDataStore1(totalBuckets, regionName);

    createPRAndInitOtherBucketsOnDataStore2(totalBuckets, regionName);

    initSecondRegion(totalBuckets, region2Name, isLocal);
  }

  private void createPRAndInitABucketOnDataStore1(int totalBuckets, String regionName) {
    dataStore1.invoke(() -> {
      createPartitionedRegion(regionName, 0, totalBuckets);
      Region<Integer, String> region = getCache().getRegion(regionName);
      // should create first bucket on server1
      region.put(1, "VALUE-1");
    });
  }

  @SuppressWarnings("rawtypes")
  private void createPartitionedRegion(String regionName, int copies, int totalBuckets) {
    RegionFactory<Integer, String> factory = getCache().createRegionFactory();
    PartitionAttributes pa = new PartitionAttributesFactory().setTotalNumBuckets(totalBuckets)
        .setRedundantCopies(copies).create();
    factory.setPartitionAttributes(pa).create(regionName);
  }

  private void createPRAndInitOtherBucketsOnDataStore2(int totalBuckets, String regionName) {
    dataStore2.invoke(() -> {
      createPartitionedRegion(regionName, 0, totalBuckets);
      Region<Integer, String> region = getCache().getRegion(regionName);
      for (int i = totalBuckets; i > 1; i--) {
        region.put(i, "VALUE-" + i);
      }
    });
  }

  private void initSecondRegion(int totalBuckets, String region2Name, boolean isLocal) {
    dataStore2.invoke(() -> createSecondRegion(region2Name, isLocal));
    dataStore1.invoke(() -> {
      createSecondRegion(region2Name, isLocal);
      Region<Integer, String> region = getCache().getRegion(region2Name);
      for (int i = totalBuckets; i > 0; i--) {
        region.put(i, "" + i);
      }
    });
  }

  private void createSecondRegion(String regionName, boolean isLocal) {
    RegionFactory<Integer, String> rf =
        getCache().createRegionFactory(isLocal ? RegionShortcut.LOCAL : RegionShortcut.REPLICATE);
    rf.create(regionName);
  }

  private void verifySizeOpTransaction(int key, String regionName, int totalBuckets,
      String region2Name, boolean isLocal) {
    if (isLocal) {
      assertThatThrownBy(
          () -> doSizeOpTransaction(2, regionName, totalBuckets, region2Name, isLocal))
              .isInstanceOf(TransactionException.class);
    } else {
      doSizeOpTransaction(2, regionName, totalBuckets, region2Name, isLocal);
    }
  }

  private void doSizeOpTransaction(int key, String regionName, int totalBuckets, String region2Name,
      boolean isLocal) {
    TXManagerImpl txMgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    Region<Integer, String> region = getCache().getRegion(regionName);
    Region<Integer, String> region2 = getCache().getRegion(region2Name);
    try {
      txMgr.begin();
      region.get(key);
      assertEquals(totalBuckets, region.size());
      int num = totalBuckets + 1;
      region2.put(num, "" + num);
      assertEquals(num, region2.size());
    } finally {
      txMgr.rollback();
    }
  }

}
