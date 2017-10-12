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
package org.apache.geode.internal.jta;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.TXManagerImpl;
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

import javax.naming.Context;
import javax.naming.NamingException;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Moved some non-DUnit tests over from org/apache/geode/internal/jta/dunit/JTADUnitTest
 * 
 */
@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class JtaIntegrationJUnitTest {

  private static final Logger logger = LogService.getLogger();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @After
  public void tearDown() {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  @Test
  public void testBug43987() {
    // InternalDistributedSystem ds = getSystem(); // ties us in to the DS owned by
    // DistributedTestCase.
    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0");// (ds.getProperties());
    Cache cache = cf.create(); // should just reuse the singleton DS owned by DistributedTestCase.
    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<String, String> r = rf.create("JTA_reg");
    r.put("key", "value");
    cache.close();
    cache = cf.create();
    RegionFactory<String, String> rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<String, String> r1 = rf1.create("JTA_reg");
    r1.put("key1", "value");
  }

  @Test
  public void testBug46169() throws Exception {
    String tableName = CacheUtils.init("CacheTest");
    assertFalse(tableName == null || tableName.equals(""));
    logger.debug("Table name: " + tableName);

    logger.debug("init for bug46169 Successful!");
    Cache cache = CacheUtils.getCache();

    TransactionManager xmanager =
        (TransactionManager) cache.getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(xmanager);

    Transaction trans = xmanager.suspend();
    assertNull(trans);

    try {
      logger.debug("Destroying table: " + tableName);
      CacheUtils.destroyTable(tableName);
      logger.debug("Closing cache...");
      logger.debug("destroyTable for bug46169 Successful!");
    } finally {
      CacheUtils.closeCache();
    }
  }

  @Test
  public void testBug46192() throws Exception {
    String tableName = CacheUtils.init("CacheTest");
    assertFalse(tableName == null || tableName.equals(""));
    logger.debug("Table name: " + tableName);

    logger.debug("init for bug46192 Successful!");
    Cache cache = CacheUtils.getCache();

    TransactionManager xmanager =
        (TransactionManager) cache.getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(xmanager);

    try {
      xmanager.rollback();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // passed
    }

    try {
      xmanager.commit();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // passed
    }

    try {
      logger.debug("Destroying table: " + tableName);
      CacheUtils.destroyTable(tableName);
      logger.debug("Closing cache...");
      logger.debug("destroyTable for bug46192 Successful!");
    } finally {
      CacheUtils.closeCache();
    }
  }

  @Test
  @Parameters
  public void testRegionSetOpWithJTA(SetOp op, boolean preventSetOpToStartJTA)
      throws NotSupportedException, SystemException, NamingException {
    verifyJTASetOp(op, preventSetOpToStartJTA);
  }

  private Object[] parametersForTestRegionSetOpWithJTA() {
    return new Object[] {new Object[] {SetOp.VALUES, false}, new Object[] {SetOp.VALUES, true},
        new Object[] {SetOp.KEYSET, false}, new Object[] {SetOp.KEYSET, true},
        new Object[] {SetOp.ENTRYSET, false}, new Object[] {SetOp.ENTRYSET, true},};
  }

  long k1 = 1;
  long k2 = 2;
  long k3 = 3;
  long k4 = 4;
  String v1 = "value1";
  String v2 = "value2";
  String v3 = "value3";
  String regionName = "region1";

  enum SetOp {
    KEYSET, VALUES, ENTRYSET;
  }

  private void verifyJTASetOp(SetOp op, boolean preventSetOpToStartJTA)
      throws NotSupportedException, SystemException, NamingException {
    Cache cache = createCache(preventSetOpToStartJTA);
    Region<Long, String> region = createRegion(cache);
    initRegion(region);

    basicVerifySetOp(op, preventSetOpToStartJTA, cache);
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

  private Cache createCache(boolean preventSetOpToStartJTA) {
    if (preventSetOpToStartJTA) {
      logger.info("setting system property {} to true ", PREVENT_SET_OP_BOOTSTRAP_TRANSACTION);
      System.setProperty(PREVENT_SET_OP_BOOTSTRAP_TRANSACTION, "true");
    }
    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0");
    Cache cache = cf.create();
    return cache;
  }

  private void initRegion(Region<Long, String> region) {
    region.put(k1, v1);
    region.put(k2, v2);
    region.put(k3, v3);
    region.put(k4, v3);
  }

  private void basicVerifySetOp(SetOp op, boolean preventSetOpToStartJTA, Cache cache)
      throws NotSupportedException, SystemException, NamingException {
    Region<Long, String> region = cache.getRegion(Region.SEPARATOR + regionName);

    Context ctx = cache.getJNDIContext();
    UserTransaction ta = startUserTransaction(ctx);

    Collection<Long> keys = new ArrayList<Long>();
    keys.add(k1);
    keys.add(k2);
    keys.add(k3);
    keys.add(k4);
    Collection<String> values = new ArrayList<String>();
    values.add(v1);
    values.add(v2);
    values.add(v3);

    // Begin the user transaction
    try {
      ta.begin();

      @SuppressWarnings("rawtypes")
      Collection set = getSetOp(region, op);

      verifyJTASetOp(op, region, keys, values, set);
    } finally {
      if (preventSetOpToStartJTA) {
        assertNull(TXManagerImpl.getCurrentTXState());
      } else {
        assertNotNull(TXManagerImpl.getCurrentTXState());
        ta.rollback();
      }
    }
  }

  private UserTransaction startUserTransaction(Context ctx) throws NamingException {
    return (UserTransaction) ctx.lookup("java:/UserTransaction");
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
  private void verifyJTASetOp(SetOp op, Region<Long, String> region, Collection<Long> keys,
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
