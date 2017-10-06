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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.*;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class PRTransaction2DUnitTest extends JUnit4CacheTestCase {

  private static final Logger logger = LogService.getLogger();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  VM accessor = null;
  VM dataStore1 = null;
  VM dataStore2 = null;
  VM dataStore3 = null;
  String regionName = "region";
  String region2Name = "region2";

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    accessor = host.getVM(3);
  }

  @Test
  public void testSizeOpOnLocalRegionInTransaction() {
    int totalBuckets = 2;
    boolean isSecondRegionLocal = true;
    setupRegions(totalBuckets, regionName, isSecondRegionLocal, region2Name);

    dataStore1.invoke(() -> verifySizeOpTransaction(2, regionName, totalBuckets, region2Name,
        isSecondRegionLocal));
  }

  @Test
  public void testSizeOpOnReplicateRegionInTransaction() {
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

  long k1 = 1;
  long k2 = 2;
  long k3 = 3;
  long k4 = 4;
  String v1 = "value1";
  String v2 = "value2";
  String v3 = "value3";

  enum SetOp {
    KEYSET, VALUES, ENTRYSET;
  }

  protected void createRegion(boolean preventSetOpToStartTx) {
    accessor.invoke(() -> createCache(preventSetOpToStartTx));
    dataStore1.invoke(() -> createCache(preventSetOpToStartTx));
    dataStore2.invoke(() -> createCache(preventSetOpToStartTx));
    dataStore3.invoke(() -> createCache(preventSetOpToStartTx));

    int totalBuckets = 3;
    accessor.invoke(() -> createPartitionedRegion(regionName, 0, totalBuckets));
    dataStore1.invoke(() -> createPartitionedRegion(regionName, 0, totalBuckets));
    dataStore2.invoke(() -> createPartitionedRegion(regionName, 0, totalBuckets));
    dataStore3.invoke(() -> createPartitionedRegion(regionName, 0, totalBuckets));
  }

  @Test
  public void testValuesCallStartsTx() {
    verifySetOp(SetOp.VALUES, false);
  }

  @Test
  public void testKeySetCallStartsTx() {
    verifySetOp(SetOp.KEYSET, false);
  }

  @Test
  public void testEntrySetCallStartsTx() {
    verifySetOp(SetOp.ENTRYSET, false);
  }

  @Test
  public void testValuesCallNotStartTxIfDisabled() {
    verifySetOp(SetOp.VALUES, true);
  }

  @Test
  public void testKeySetCallNotStartTxIfDisabled() {
    verifySetOp(SetOp.KEYSET, true);
  }

  @Test
  public void testEntrySetCallNotStartTxIfDisabled() {
    verifySetOp(SetOp.ENTRYSET, true);
  }

  @SuppressWarnings("rawtypes")
  private void basicVerifySetOp(SetOp op, boolean preventSetOpToStartTx) {
    Cache cache = basicGetCache();
    Region<Long, String> region = cache.getRegion(Region.SEPARATOR + regionName);

    Collection<Long> keys = new ArrayList<Long>();
    keys.add(k1);
    keys.add(k2);
    keys.add(k3);
    keys.add(k4);
    Collection<String> values = new ArrayList<String>();
    values.add(v1);
    values.add(v2);
    values.add(v3);
    CacheTransactionManager txMgr = basicGetCache().getCacheTransactionManager();

    try {
      txMgr.begin();

      Collection set = getSetOp(region, op);

      verifySetOp(op, region, keys, values, set);
    } finally {
      assertNotNull(TXManagerImpl.getCurrentTXState());
      if (preventSetOpToStartTx) {
        assertFalse(((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).hasRealDeal());
      } else {
        assertTrue(((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).hasRealDeal());
      }
      txMgr.rollback();
    }
  }

  private void verifySetOp(SetOp op, boolean preventSetOpToStartTx) {
    createRegion(preventSetOpToStartTx);
    dataStore1.invoke(() -> initRegion());

    accessor.invoke(() -> basicVerifySetOp(op, preventSetOpToStartTx));
    dataStore1.invoke(() -> basicVerifySetOp(op, preventSetOpToStartTx));
    dataStore2.invoke(() -> basicVerifySetOp(op, preventSetOpToStartTx));
    dataStore3.invoke(() -> basicVerifySetOp(op, preventSetOpToStartTx));
  }

  @SuppressWarnings("rawtypes")
  private void verifySetOp(SetOp op, Region<Long, String> region, Collection<Long> keys,
      Collection<String> values, Collection set) {
    Iterator it = set.iterator();
    while (it.hasNext()) {
      Object o = it.next();
      switch (op) {
        case VALUES:
          assertTrue(values.contains(o));
          break;
        case KEYSET:
          assertTrue(keys.contains(o));
          break;
        case ENTRYSET:
          assertTrue(keys.contains(((Region.Entry) o).getKey()));
          assertTrue(values.contains(((Region.Entry) o).getValue()));
          break;
        default:
          fail("Unexpected op: " + op);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private Collection getSetOp(Region<Long, String> region, SetOp op) {
    Collection set = null;
    switch (op) {
      case VALUES:
        set = region.values();
        break;
      case KEYSET:
        set = region.keySet();
        break;
      case ENTRYSET:
        set = region.entrySet();
        break;
      default:
        fail("Unexpected op: " + op);
    }
    return set;
  }

  private void initRegion() {
    Region<Long, String> currRegion;
    currRegion = basicGetCache().getRegion(Region.SEPARATOR + regionName);
    currRegion.put(k1, v1);
    currRegion.put(k2, v2);
    currRegion.put(k3, v3);
  }

  final String preventSetOpBootstrapTransaction = "preventSetOpBootstrapTransaction";
  final String PREVENT_SET_OP_BOOTSTRAP_TRANSACTION =
      (System.currentTimeMillis() % 2 == 0 ? DistributionConfig.GEMFIRE_PREFIX : "geode.")
          + preventSetOpBootstrapTransaction;

  private void createCache(boolean preventSetOpToStartTx) {
    if (preventSetOpToStartTx) {
      logger.info("setting system property {} to true ", PREVENT_SET_OP_BOOTSTRAP_TRANSACTION);
      System.setProperty(PREVENT_SET_OP_BOOTSTRAP_TRANSACTION, "true");
    }
    getCache();
  }

}
