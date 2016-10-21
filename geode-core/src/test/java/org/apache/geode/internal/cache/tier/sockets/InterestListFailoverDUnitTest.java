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
package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Test Scenario :
 *
 * one server two clients create Entries in all vms c1 : register (k1,k2,k3,k4,k5) c2 : put (k1 ->
 * vm2-key-1) and (k6 -> vm2-key-6) c1 : validate (r.getEntry("key-1").getValue() == "vm2-key-1")
 * (r.getEntry("key-6").getValue() == "key-6") // as it is not registered s1 : stop server c2 : put
 * (k1 -> vm2-key-1) and (k6 -> vm2-key-6) c1 : validate (r.getEntry("key-1").getValue() ==
 * "vm2-key-1") (r.getEntry("key-6").getValue() == "key-6") // as it is not registered *
 */
@Category(DistributedTest.class)
public class InterestListFailoverDUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME =
      InterestListFailoverDUnitTest.class.getSimpleName() + "_region";

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;

  private int PORT1;
  private int PORT2;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);

  }

  public void createServersAndClients(int redundancyLevel) {
    final Host host = Host.getHost(0);
    // start servers first
    PORT1 = ((Integer) vm0
        .invoke(() -> CacheServerTestUtil.createCacheServer(REGION_NAME, new Boolean(true))))
            .intValue();

    PORT2 = ((Integer) vm3
        .invoke(() -> CacheServerTestUtil.createCacheServer(REGION_NAME, new Boolean(true))))
            .intValue();

    vm1.invoke(() -> CacheServerTestUtil.disableShufflingOfEndpoints());
    vm2.invoke(() -> CacheServerTestUtil.disableShufflingOfEndpoints());
    vm1.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(host), redundancyLevel), REGION_NAME));
    vm2.invoke(() -> CacheServerTestUtil
        .createCacheClient(getClientPool(NetworkUtils.getServerHostName(host), 0), REGION_NAME));
  }

  /**
   * one server two clients create Entries in all vms c1 : register (k1,k2,k3,k4,k5) c2 : put (k1 ->
   * vm2-key-1) and (k6 -> vm2-key-6) c1 : validate (r.getEntry("key-1").getValue() == "vm2-key-1")
   * (r.getEntry("key-6").getValue() == "key-6") // as it is not registered s1 : stop server s1 :
   * start server c2 : put (k1 -> vm2-key-1) and (k6 -> vm2-key-6) c1 : validate
   * (r.getEntry("key-1").getValue() == "vm2-key-1") (r.getEntry("key-6").getValue() == "key-6") //
   * as it is not registered
   *
   */

  @Test
  public void testInterestListRecoveryHA() {
    doTestInterestListRecovery(-1);
  }

  @Test
  public void testInterestListRecoveryNonHA() {
    doTestInterestListRecovery(0);
  }

  public void doTestInterestListRecovery(int redundancyLevel) {
    createServersAndClients(redundancyLevel);
    vm1.invoke(() -> InterestListFailoverDUnitTest.createEntries());
    vm2.invoke(() -> InterestListFailoverDUnitTest.createEntries());
    vm0.invoke(() -> InterestListFailoverDUnitTest.createEntries());
    Integer primaryPort =
        (Integer) vm1.invoke(() -> InterestListFailoverDUnitTest.registerInterestList());
    VM primaryVM;
    if (primaryPort.intValue() == PORT1) {
      primaryVM = vm0;
    } else {
      primaryVM = vm3;
    }
    vm2.invoke(() -> InterestListFailoverDUnitTest.putA());
    // pause(10000);
    vm1.invoke(() -> InterestListFailoverDUnitTest.validateEntriesA());
    primaryVM.invoke(() -> InterestListFailoverDUnitTest.stopServer());
    // pause(10000);
    vm2.invoke(() -> InterestListFailoverDUnitTest.putB());
    // (10000);
    vm1.invoke(() -> InterestListFailoverDUnitTest.validateEntriesB());
  }

  public static void createEntries() {
    try {
      Region r = CacheServerTestUtil.getCache().getRegion("/" + REGION_NAME);
      assertNotNull(r);

      if (!r.containsKey("key-1")) {
        r.create("key-1", "key-1");
      }
      if (!r.containsKey("key-6")) {
        r.create("key-6", "key-6");
      }
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "key-1");
      assertEquals(r.getEntry("key-6").getValue(), "key-6");
    } catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void verifyEntries() {
    try {
      Region r = CacheServerTestUtil.getCache().getRegion("/" + REGION_NAME);
      assertNotNull(r);
      if (r.getEntry("key-1") != null) {
        assertEquals(r.getEntry("key-1").getValue(), "key-1");
      }
      if (r.getEntry("key-6") != null) {
        assertEquals(r.getEntry("key-6").getValue(), "key-6");
      }
    } catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static Integer registerInterestList() {
    try {
      Region r = CacheServerTestUtil.getCache().getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.registerInterest("key-1");
      r.registerInterest("key-2");
      r.registerInterest("key-3");
      r.registerInterest("key-4");
      r.registerInterest("key-5");
      // now return the port of the primary.
      PoolImpl p = (PoolImpl) PoolManager.find(r.getAttributes().getPoolName());
      return new Integer(p.getPrimaryPort());
    } catch (Exception ex) {
      Assert.fail("failed while registering keys k1 to k5", ex);
      return null;
    }
  }

  public static void stopServer() {
    try {
      Iterator iter = CacheServerTestUtil.getCache().getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer) iter.next();
        server.stop();
      }
    } catch (Exception e) {
      fail("failed while stopServer()" + e);
    }
  }

  private Pool getClientPool(String host, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, PORT1).addServer(host, PORT2).setSubscriptionEnabled(true)
        // round robin?
        .setReadTimeout(500).setSocketBufferSize(32768)
        // retryAttempts 5
        // retryInterval 1000
        .setMinConnections(4).setSubscriptionRedundancy(redundancyLevel);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  public static void _put(String v) {
    try {
      Region r = CacheServerTestUtil.getCache().getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.put("key-1", "vm2-key-1" + v);
      r.put("key-6", "vm2-key-6" + v);
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "vm2-key-1" + v);
      assertEquals(r.getEntry("key-6").getValue(), "vm2-key-6" + v);
    } catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public static void putA() {
    _put("A");
  }

  public static void putB() {
    _put("B");
  }

  public static void validateEntriesA() {
    _validateEntries("A");
  }

  public static void validateEntriesB() {
    _validateEntries("B");
  }

  public static void _validateEntries(final String v) {
    try {
      final Region r = CacheServerTestUtil.getCache().getRegion("/" + REGION_NAME);
      final String key1 = "key-1";
      assertNotNull(r);
      // Verify that 'key-1' was updated
      // assertIndexDetailsEquals("vm2-key-1", r.getEntry("key-1").getValue());
      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          Object val = r.get(key1);
          if (val == null) {
            return false;
          }
          if (!val.equals("vm2-key-1" + v)) {
            return false;
          }
          return true;
        }

        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 40 * 1000, 1000, true);

      // Verify that 'key-6' was not invalidated
      assertEquals("key-6", r.getEntry("key-6").getValue());
    } catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    closeAll();
  }

  public void closeAll() {
    // close the clients first
    vm1.invoke(() -> CacheServerTestUtil.closeCache());
    vm2.invoke(() -> CacheServerTestUtil.closeCache());
    // then close the servers
    vm0.invoke(() -> CacheServerTestUtil.closeCache());
    vm3.invoke(() -> CacheServerTestUtil.closeCache());
    CacheServerTestUtil.closeCache();
  }

}


