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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.cq.internal.CqServiceImpl;
import org.apache.geode.cache.query.cq.internal.ServerCQImpl;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class CqResultSetUsingPoolOptimizedExecuteDUnitTest extends CqResultSetUsingPoolDUnitTest {

  public CqResultSetUsingPoolOptimizedExecuteDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpCqResultSetUsingPoolDUnitTest() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      @Override
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = false;
      }
    });
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      @Override
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = true;
      }
    });
  }

  /**
   * Tests CQ Result Caching with CQ Failover. When EXECUTE_QUERY_DURING_INIT is false and new
   * server calls execute during HA the results cache is not initialized.
   *
   */
  @Override
  @Test
  public void testCqResultsCachingWithFailOver() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    cqDUnitTest.createServer(server1);

    final int port1 = server1.invoke(CqQueryUsingPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    String poolName = "testCQFailOver";
    final String cqName = "testCQFailOver_0";

    cqDUnitTest.createPool(client, poolName, new String[] {host0, host0},
        new int[] {port1, ports[0]});

    // create CQ.
    cqDUnitTest.createCQ(client, poolName, cqName, cqDUnitTest.cqs[0]);

    final int numObjects = 300;
    final int totalObjects = 500;

    // initialize Region.
    server1.invoke(new CacheSerializableRunnable("Update Region") {
      @Override
      public void run2() throws CacheException {
        Region region =
            getCache().getRegion(SEPARATOR + "root" + SEPARATOR + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put("" + i, p);
        }
      }
    });

    // Keep updating region (async invocation).
    server1.invokeAsync(new CacheSerializableRunnable("Update Region") {
      @Override
      public void run2() throws CacheException {
        Region region =
            getCache().getRegion(SEPARATOR + "root" + SEPARATOR + cqDUnitTest.regions[0]);
        // Update (totalObjects - 1) entries.
        for (int i = 1; i < totalObjects; i++) {
          // Destroy entries.
          if (i > 25 && i < 201) {
            region.destroy("" + i);
            continue;
          }
          Portfolio p = new Portfolio(i);
          region.put("" + i, p);
        }
        // recreate destroyed entries.
        for (int j = 26; j < 201; j++) {
          Portfolio p = new Portfolio(j);
          region.put("" + j, p);
        }
        // Add the last key.
        Portfolio p = new Portfolio(totalObjects);
        region.put("" + totalObjects, p);
      }
    });

    // Execute CQ.
    // While region operation is in progress execute CQ.
    cqDUnitTest.executeCQ(client, cqName, true, null);

    // Verify CQ Cache results.
    server1.invoke(new CacheSerializableRunnable("Verify CQ Cache results") {
      @Override
      public void run2() throws CacheException {
        CqServiceImpl CqServiceImpl = null;
        try {
          CqServiceImpl =
              (org.apache.geode.cache.query.cq.internal.CqServiceImpl) ((DefaultQueryService) getCache()
                  .getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqServiceImpl.", ex);
          Assert.fail("Failed to get the internal CqServiceImpl.", ex);
        }

        // Wait till all the region update is performed.
        Region region =
            getCache().getRegion(SEPARATOR + "root" + SEPARATOR + cqDUnitTest.regions[0]);
        while (true) {
          if (region.get("" + totalObjects) == null) {
            try {
              Thread.sleep(50);
            } catch (Exception ex) {
              // ignore.
            }
            continue;
          }
          break;
        }
        Collection<? extends InternalCqQuery> cqs = CqServiceImpl.getAllCqs();
        for (InternalCqQuery cq : cqs) {
          ServerCQImpl cqQuery = (ServerCQImpl) cq;
          if (cqQuery.getName().equals(cqName)) {
            int size = cqQuery.getCqResultKeysSize();
            if (size != totalObjects) {
              LogWriterUtils.getLogWriter().info("The number of Cached events " + size
                  + " is not equal to the expected size " + totalObjects);
              HashSet expectedKeys = new HashSet();
              for (int i = 1; i < totalObjects; i++) {
                expectedKeys.add("" + i);
              }
              Set cachedKeys = cqQuery.getCqResultKeyCache();
              expectedKeys.removeAll(cachedKeys);
              LogWriterUtils.getLogWriter().info("Missing keys from the Cache : " + expectedKeys);
            }
            assertEquals("The number of keys cached for cq " + cqName + " is wrong.", totalObjects,
                cqQuery.getCqResultKeysSize());
          }
        }
      }
    });

    cqDUnitTest.createServer(server2, ports[0]);
    final int thePort2 = server2.invoke(CqQueryUsingPoolDUnitTest::getCacheServerPort);
    System.out
        .println("### Port on which server1 running : " + port1 + " Server2 running : " + thePort2);
    Wait.pause(3 * 1000);

    // Close server1 for CQ fail over to server2.
    cqDUnitTest.closeServer(server1);
    Wait.pause(3 * 1000);

    // Verify CQ Cache results.
    server2.invoke(new CacheSerializableRunnable("Verify CQ Cache results") {
      @Override
      public void run2() throws CacheException {
        CqServiceImpl CqServiceImpl = null;
        try {
          CqServiceImpl =
              (CqServiceImpl) ((DefaultQueryService) getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqServiceImpl.", ex);
          Assert.fail("Failed to get the internal CqServiceImpl.", ex);
        }

        // Wait till all the region update is performed.
        Region region =
            getCache().getRegion(SEPARATOR + "root" + SEPARATOR + cqDUnitTest.regions[0]);
        while (true) {
          if (region.get("" + totalObjects) == null) {
            try {
              Thread.sleep(50);
            } catch (Exception ex) {
              // ignore.
            }
            continue;
          }
          break;
        }
        Collection<? extends InternalCqQuery> cqs = CqServiceImpl.getAllCqs();
        for (InternalCqQuery cq : cqs) {
          ServerCQImpl cqQuery = (ServerCQImpl) cq;
          if (cqQuery.getName().equals(cqName)) {
            int size = cqQuery.getCqResultKeysSize();
            assertEquals("The number of keys cached for cq " + cqName + " is wrong.", 0, size);
          }
        }
      }
    });

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server2);
  }

}
