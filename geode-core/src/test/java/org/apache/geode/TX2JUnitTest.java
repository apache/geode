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
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class TX2JUnitTest {

  private static final Logger logger = LogService.getLogger();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  long k1 = 1;
  long k2 = 2;
  long k3 = 3;
  long k4 = 4;
  String v1 = "value1";
  String v2 = "value2";
  String v3 = "value3";
  String regionName = "region1";
  Cache cache;

  enum SetOp {
    KEYSET, VALUES, ENTRYSET;
  }

  @After
  public void tearDownTXJUnitTest() throws Exception {
    closeCache();
  }

  @Test
  @Parameters
  public void testRegionSetOpWithTx(SetOp op, boolean preventSetOpToStartTx) {
    verifySetOp(SetOp.VALUES, false);
  }

  private Object[] parametersForTestRegionSetOpWithTx() {
    return new Object[] {new Object[] {SetOp.VALUES, false}, new Object[] {SetOp.VALUES, true},
        new Object[] {SetOp.KEYSET, false}, new Object[] {SetOp.KEYSET, true},
        new Object[] {SetOp.ENTRYSET, false}, new Object[] {SetOp.ENTRYSET, true},};
  }

  private void verifySetOp(SetOp op, boolean preventSetOpToStartTx) {
    this.cache = createCache(preventSetOpToStartTx);
    Region<Long, String> region = createRegion(cache);
    initRegion(region);

    basicVerifySetOp(op, preventSetOpToStartTx, cache);
  }

  private void basicVerifySetOp(SetOp op, boolean preventSetOpToStartTx, Cache cache) {
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
    CacheTransactionManager txMgr = cache.getCacheTransactionManager();

    try {
      txMgr.begin();

      @SuppressWarnings("rawtypes")
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

  protected Region<Long, String> createRegion(Cache cache) {
    RegionFactory<Long, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<Long, String> r = rf.create(regionName);
    return r;
  }

  final String preventSetOpBootstrapTransaction = "preventSetOpBootstrapTransaction";
  final String PREVENT_SET_OP_BOOTSTRAP_TRANSACTION =
      (System.currentTimeMillis() % 2 == 0 ? DistributionConfig.GEMFIRE_PREFIX : "geode.")
          + preventSetOpBootstrapTransaction;

  private Cache createCache(boolean preventSetOpToStartTx) {
    if (preventSetOpToStartTx) {
      logger.info("setting system property {} to true ", PREVENT_SET_OP_BOOTSTRAP_TRANSACTION);
      System.setProperty(PREVENT_SET_OP_BOOTSTRAP_TRANSACTION, "true");
    }
    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0");
    this.cache = (GemFireCacheImpl) cf.create();
    return this.cache;
  }

  protected void closeCache() {
    if (this.cache != null) {
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }

  private void initRegion(Region<Long, String> region) {
    region.put(k1, v1);
    region.put(k2, v2);
    region.put(k3, v3);
    region.put(k4, v3);
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
}
