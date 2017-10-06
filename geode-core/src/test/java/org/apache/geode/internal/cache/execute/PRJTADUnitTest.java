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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for JTA with PR.
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class PRJTADUnitTest extends JUnit4CacheTestCase {

  private static final Logger logger = LogService.getLogger();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  public PRJTADUnitTest() {
    super();
  }

  VM accessor = null;
  VM dataStore1 = null;
  VM dataStore2 = null;
  VM dataStore3 = null;


  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS(); // isolate this test from others to avoid periodic CacheExistsExceptions
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    accessor = host.getVM(3);
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

  protected void createRegion(boolean preventSetOpToStartJTA) {
    accessor.invoke(() -> createCache(preventSetOpToStartJTA));
    dataStore1.invoke(() -> createCache(preventSetOpToStartJTA));
    dataStore2.invoke(() -> createCache(preventSetOpToStartJTA));
    dataStore3.invoke(() -> createCache(preventSetOpToStartJTA));

    accessor.invoke(() -> createPR());
    dataStore1.invoke(() -> createPR());
    dataStore2.invoke(() -> createPR());
    dataStore3.invoke(() -> createPR());
  }

  @Test
  public void testValuesCallStartsJTA() {
    verifyJTASetOp(SetOp.VALUES, false);
  }

  @Test
  public void testKeySetCallStartsJTA() {
    verifyJTASetOp(SetOp.KEYSET, false);
  }

  @Test
  public void testEntrySetCallStartsJTA() {
    verifyJTASetOp(SetOp.ENTRYSET, false);
  }

  @Test
  public void testValuesCallNotStartJTAIfDisabled() {
    verifyJTASetOp(SetOp.VALUES, true);
  }

  @Test
  public void testKeySetCallNotStartJTAIfDisabled() {
    verifyJTASetOp(SetOp.KEYSET, true);
  }

  @Test
  public void testEntrySetCallNotStartJTAIfDisabled() {
    verifyJTASetOp(SetOp.ENTRYSET, true);
  }

  String regionName = "region1";

  private void basicVerifySetOp(SetOp op, boolean preventSetOpToStartJTA)
      throws NotSupportedException, SystemException, NamingException {
    Cache cache = basicGetCache();
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

  private void verifyJTASetOp(SetOp op, boolean preventSetOpToStartJTA) {
    createRegion(preventSetOpToStartJTA);
    dataStore1.invoke(() -> initRegion());

    accessor.invoke(() -> basicVerifySetOp(op, preventSetOpToStartJTA));
    dataStore1.invoke(() -> basicVerifySetOp(op, preventSetOpToStartJTA));
    dataStore2.invoke(() -> basicVerifySetOp(op, preventSetOpToStartJTA));
    dataStore3.invoke(() -> basicVerifySetOp(op, preventSetOpToStartJTA));
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
    currRegion.put(k4, v3);
  }

  private UserTransaction startUserTransaction(Context ctx) throws NamingException {
    return (UserTransaction) ctx.lookup("java:/UserTransaction");
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(() -> verifyNoTxState());
  }

  private void verifyNoTxState() {
    TXManagerImpl mgr = getCache().getTxManager();
    assertEquals(0, mgr.hostedTransactionsInProgressForTest());
  }

  final String preventSetOpBootstrapTransaction = "preventSetOpBootstrapTransaction";
  final String PREVENT_SET_OP_BOOTSTRAP_TRANSACTION =
      (System.currentTimeMillis() % 2 == 0 ? DistributionConfig.GEMFIRE_PREFIX : "geode.")
          + preventSetOpBootstrapTransaction;

  private void createCache(boolean preventSetOpToStartJTA) {
    if (preventSetOpToStartJTA) {
      logger.info("setting system property {} to true ", PREVENT_SET_OP_BOOTSTRAP_TRANSACTION);
      System.setProperty(PREVENT_SET_OP_BOOTSTRAP_TRANSACTION, "true");
    }
    getCache();
  }

  private void createPR() {
    basicGetCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(
            new PartitionAttributesFactory<Long, String>().setTotalNumBuckets(3).create())
        .create(regionName);
  }
}
