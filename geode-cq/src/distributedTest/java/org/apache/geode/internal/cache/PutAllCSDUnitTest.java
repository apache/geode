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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests putAll for c/s. Also tests removeAll
 *
 * @since GemFire 5.0.23
 */
@Category({ClientServerTest.class, ClientSubscriptionTest.class})
@SuppressWarnings("serial")
public class PutAllCSDUnitTest extends ClientServerTestCase {

  final int numberOfEntries = 100;
  final int testEndPointSwitchNumber = 200;
  final int thousandEntries = 1000;
  final int TOTAL_BUCKETS = 10;

  static Object lockObject = new Object();
  static Object lockObject2 = new Object();
  static Object lockObject3 = new Object();
  static Object lockObject4 = new Object();

  final String expectedExceptions = PutAllPartialResultException.class.getName() + "||"
      + ServerConnectivityException.class.getName() + "||"
      + RegionDestroyedException.class.getName() + "||java.net.ConnectException";

  List<VersionTag> client1Versions = null;
  List<VersionTag> client2Versions = null;

  List<VersionTag> client1RAVersions = null;
  List<VersionTag> client2RAVersions = null;

  List<String> expectedVersions = null;
  List<String> actualVersions = null;

  List<String> expectedRAVersions = null;
  List<String> actualRAVersions = null;

  private static void checkRegionSize(final Region region, final int expectedSize) {
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return region.size() == expectedSize;
      }

      @Override
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 10 * 1000, 1000, true);
    assertEquals(expectedSize, region.size());
  }

  /**
   * Tests putAll to one server.
   */
  @Test
  public void testOneServer() throws CacheException, InterruptedException {
    final String title = "testOneServer:";
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    // set <false, true> means <PR=false, notifyBySubscription=true> to enable registerInterest and
    // CQ
    final int serverPort = createBridgeServer(server, regionName, 0, false, 0, null);
    createClient(client1, regionName, serverHost, new int[] {serverPort}, -1, -1, false, true,
        true);
    createClient(client2, regionName, serverHost, new int[] {serverPort}, -1, -1, false, true,
        true);

    server.invoke(new CacheSerializableRunnable(title + "server add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
      }
    });

    client2
        .invoke(new CacheSerializableRunnable(title + "client2 registerInterest and add listener") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            region.getAttributesMutator().addCacheListener(new MyListener(false));

            // registerInterest for ALL_KEYS
            region.registerInterest("ALL_KEYS");
            LogWriterUtils.getLogWriter()
                .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
          }
        });

    client1.invoke(
        new CacheSerializableRunnable(title + "client1 create local region and run putAll") {
          @Override
          public void run2() throws CacheException {
            AttributesFactory factory2 = new AttributesFactory();
            factory2.setScope(Scope.LOCAL);
            factory2.addCacheListener(new MyListener(false));
            createRegion("localsave", factory2.create());

            Region region = doPutAll(regionName, "key-", numberOfEntries);
            assertEquals(numberOfEntries, region.size());
          }
        });

    AsyncInvocation async1 =
        client1.invokeAsync(new CacheSerializableRunnable(title + "client1 create CQ") {
          @Override
          public void run2() throws CacheException {
            // create a CQ for key 10-20
            Region localregion = getRootRegion().getSubregion("localsave");
            CqAttributesFactory cqf1 = new CqAttributesFactory();
            EOCQEventListener EOCQListener = new EOCQEventListener(localregion);
            cqf1.addCqListener(EOCQListener);
            CqAttributes cqa1 = cqf1.create();
            String cqName1 = "EOInfoTracker";
            String queryStr1 = "SELECT ALL * FROM /root/" + regionName
                + " ii WHERE ii.getTicker() >= '10' and ii.getTicker() < '20'";
            LogWriterUtils.getLogWriter().info("Query String: " + queryStr1);
            try {
              QueryService cqService = getCache().getQueryService();
              CqQuery EOTracker = cqService.newCq(cqName1, queryStr1, cqa1);
              SelectResults rs1 = EOTracker.executeWithInitialResults();

              List list1 = rs1.asList();
              for (int i = 0; i < list1.size(); i++) {
                Struct s = (Struct) list1.get(i);
                TestObject o = (TestObject) s.get("value");
                LogWriterUtils.getLogWriter().info("InitialResult:" + i + ":" + o);
                localregion.put("key-" + i, o);
              }
              if (localregion.size() > 0) {
                LogWriterUtils.getLogWriter().info("CQ is ready");
                synchronized (lockObject) {
                  lockObject.notify();
                }
              }

              waitTillNotify(lockObject2, 20000,
                  (EOCQListener.num_creates == 5 && EOCQListener.num_updates == 5));
              EOTracker.close();
            } catch (CqClosedException e) {
              Assert.fail("CQ", e);
            } catch (RegionNotFoundException e) {
              Assert.fail("CQ", e);
            } catch (QueryInvalidException e) {
              Assert.fail("CQ", e);
            } catch (CqExistsException e) {
              Assert.fail("CQ", e);
            } catch (CqException e) {
              Assert.fail("CQ", e);
            }
          }
        });

    server.invoke(new CacheSerializableRunnable(title + "verify cache server") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    // verify CQ is ready
    client1.invoke(new CacheSerializableRunnable(title + "verify CQ is ready") {
      @Override
      public void run2() throws CacheException {
        Region localregion = getRootRegion().getSubregion("localsave");
        waitTillNotify(lockObject, 10000, (localregion.size() > 0));
        assertTrue(localregion.size() > 0);
      }
    });

    // verify registerInterest result at client2
    client2.invoke(new CacheSerializableRunnable(title + "verify client2") {
      @Override
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }

        // then do update for key 10-20 to trigger CQ at server2
        // destroy key 10-14 to simulate create/update mix case
        region.removeAll(Arrays.asList("key-10", "key-11", "key-12", "key-13", "key-14"),
            "removeAllCallback");
        assertEquals(null, region.get("key-10"));
        assertEquals(null, region.get("key-11"));
        assertEquals(null, region.get("key-12"));
        assertEquals(null, region.get("key-13"));
        assertEquals(null, region.get("key-14"));
      }
    });

    // verify CQ result at client1
    client1.invoke(new CacheSerializableRunnable(title + "Verify client1") {
      @Override
      public void run2() throws CacheException {
        Region localregion = getRootRegion().getSubregion("localsave");
        for (int i = 10; i < 15; i++) {
          TestObject obj = null;
          int cnt = 0;
          while (cnt < 100) {
            obj = (TestObject) localregion.get("key-" + i);
            if (obj != null) {
              // wait for the key to be destroyed
              Wait.pause(100);
              if (LogWriterUtils.getLogWriter().fineEnabled()) {
                LogWriterUtils.getLogWriter()
                    .info("Waiting 100ms(" + cnt + ") for key-" + i + " to be destroyed");
              }
              cnt++;
            } else {
              break;
            }
          }
        }
      }
    });
    client2.invoke(new CacheSerializableRunnable(title + "verify client2") {
      @Override
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(regionName);
        LinkedHashMap map = new LinkedHashMap();
        for (int i = 10; i < 20; i++) {
          map.put("key-" + i, new TestObject(i * 10));
        }
        region.putAll(map, "putAllCallback");
      }
    });
    // verify CQ result at client1
    client1.invoke(new CacheSerializableRunnable(title + "Verify client1") {
      @Override
      public void run2() throws CacheException {
        Region localregion = getRootRegion().getSubregion("localsave");
        for (int i = 10; i < 20; i++) {
          TestObject obj = null;
          int cnt = 0;
          while (cnt < 100) {
            obj = (TestObject) localregion.get("key-" + i);
            if (obj == null || obj.getPrice() != i * 10) {
              Wait.pause(100);
              LogWriterUtils.getLogWriter()
                  .info("Waiting 100ms(" + cnt + ") for obj.getPrice() == i*10 at entry " + i);
              cnt++;
            } else {
              break;
            }
          }
          assertEquals(i * 10, obj.getPrice());
        }
        synchronized (lockObject2) {
          lockObject2.notify();
        }
      }
    });

    ThreadUtils.join(async1, 30 * 1000);

    // verify stats for client putAll into distributed region
    // 1. verify client staus
    /*
     * server2.invoke(new CacheSerializableRunnable("server2 execute putAll") { public void run2()
     * throws CacheException { try { DistributedSystemConfig config =
     * AdminDistributedSystemFactory.defineDistributedSystem(system, null); AdminDistributedSystem
     * ads = AdminDistributedSystemFactory.getDistributedSystem(config); ads.connect();
     * DistributedMember distributedMember = system.getDistributedMember(); SystemMember member =
     * ads.lookupSystemMember(distributedMember);
     *
     * StatisticResource[] resources = member.getStats(); for (int i=0; i<resources.length; i++) {
     * System.out.println("GGG:"+resources[i].getType()); if
     * (resources[i].getType().equals("CacheServerClientStats")) { Statistic[] stats =
     * resources[i].getStatistics(); for (int j=0; i<stats.length; i++) { if
     * (stats[j].getName().equals("putAll")) {
     * System.out.println("GGG:"+stats[j].getName()+":"+stats[j].getValue()); } else if
     * (stats[j].getName().equals("sendPutAllTime")) {
     * System.out.println("GGG:"+stats[j].getName()+":"+stats[j].getValue()); } } } } } catch
     * (AdminException e) { fail("Failed while creating AdminDS", e); } } });
     */

    // Test Exception handling
    // verify CQ is ready
    client1.invoke(new CacheSerializableRunnable(title + "test exception handling") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        Map m = null;
        boolean NPEthrowed = false;
        try {
          region.putAll(m, "putAllCallback");
          fail("Should have thrown NullPointerException");
        } catch (NullPointerException ex) {
          NPEthrowed = true;
        }
        assertTrue(NPEthrowed);

        region.localDestroyRegion();
        boolean RDEthrowed = false;
        try {
          m = new HashMap();
          for (int i = 1; i < 21; i++) {
            m.put(new Integer(i), Integer.toString(i));
          }
          region.putAll(m, "putAllCallback");
          fail("Should have thrown RegionDestroyedException");
        } catch (RegionDestroyedException ex) {
          RDEthrowed = true;
        }
        assertTrue(RDEthrowed);
        try {
          region.removeAll(Arrays.asList("key-10", "key-11"), "removeAllCallback");
          fail("Should have thrown RegionDestroyedException");
        } catch (RegionDestroyedException expected) {
        }
      }
    });

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests putAll afterUpdate event contained oldValue.
   */
  @Test
  public void testOldValueInEvent() throws CacheException, InterruptedException {
    final String title = "testOldValueInEvent:";
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=false to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);
    createClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, -1, false, true,
        true);
    createClient(client2, regionName, serverHost, new int[] {serverPort2}, -1, -1, false, true,
        true);

    client2
        .invoke(new CacheSerializableRunnable(title + "client2 registerInterest and add listener") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            region.getAttributesMutator().addCacheListener(new MyListener(false));

            // registerInterest for ALL_KEYS
            region.registerInterest("ALL_KEYS");
            LogWriterUtils.getLogWriter()
                .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
          }
        });

    client1.invoke(
        new CacheSerializableRunnable(title + "client1 create local region and run putAll") {
          @Override
          public void run2() throws CacheException {
            // create keys
            Region region = getRootRegion().getSubregion(regionName);
            region.getAttributesMutator().addCacheListener(new MyListener(false));
            doPutAll(regionName, title, numberOfEntries);
            assertEquals(numberOfEntries, region.size());

            // update keys
            doPutAll(regionName, title, numberOfEntries);
            assertEquals(numberOfEntries, region.size());
          }
        });

    // verify 41890, the local PUTALL_UPDATE event should contain old value
    client1.invoke(new CacheSerializableRunnable(title + "verify after update events") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        waitTillNotify(lockObject, 20000, (region.size() == numberOfEntries));
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "verify after update events") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        waitTillNotify(lockObject, 20000, (region.size() == numberOfEntries));
      }
    });

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Create PR without redundancy on 2 servers with lucene index. Feed some key s. From a client, do
   * removeAll on keys in server1. During the removeAll, restart server1 and trigger the removeAll
   * to retry. The retried removeAll should return the version tag of tombstones. Do removeAll again
   * on the same key, it should get the version tag again.
   */
  @Test
  public void shouldReturnVersionTagOfTombstoneVersionWhenRemoveAllRetried()
      throws CacheException, InterruptedException {
    final String title = "test51871:";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=false to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 0, "ds1");
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, -1, true);

    client1.invoke(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        doPutAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries, region.size());
      }
    });

    @SuppressWarnings("unchecked")
    VersionedObjectList versions =
        (VersionedObjectList) client1.invoke(new SerializableCallable(title + "client1 removeAll") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            VersionedObjectList versions = doRemoveAll(regionName, "key-", numberOfEntries);
            assertEquals(0, region.size());
            return versions;
          }
        });

    @SuppressWarnings("unchecked")
    VersionedObjectList versionsAfterRetry = (VersionedObjectList) client1
        .invoke(new SerializableCallable(title + "client1 removeAll again") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            VersionedObjectList versions = doRemoveAll(regionName, "key-", numberOfEntries);
            assertEquals(0, region.size());
            return versions;
          }
        });

    LogWriterUtils.getLogWriter().info("Version tags are:" + versions.getVersionTags() + ":"
        + versionsAfterRetry.getVersionTags());
    assertEquals(versionsAfterRetry.getVersionTags(), versions.getVersionTags());

    // clean up
    // Stop serverß
    stopBridgeServers(getCache());
  }

  /**
   * Tests putAll and removeAll to 2 servers. Use Case: 1) putAll from a single-threaded client to a
   * replicated region 2) putAll from a multi-threaded client to a replicated region 3)
   */
  @Test
  public void test2Server() throws CacheException, InterruptedException {
    final String title = "test2Server:";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=false to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);

    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, -1, true);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2}, -1, -1, true);

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        doPutAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().setCacheWriter(new MyWriter("key-"));
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().setCacheWriter(new MyWriter("key-"));
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("key-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doRemoveAll(regionName, "key-", numberOfEntries);
        assertEquals(0, region.size());
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
        MyWriter mywriter = (MyWriter) region.getAttributes().getCacheWriter();
        LogWriterUtils.getLogWriter()
            .info("server cachewriter triggered for destroy: " + mywriter.num_destroyed);
        assertEquals(numberOfEntries, mywriter.num_destroyed);
      }
    });

    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
        MyWriter mywriter = (MyWriter) region.getAttributes().getCacheWriter();
        LogWriterUtils.getLogWriter()
            .info("server cachewriter triggered for destroy: " + mywriter.num_destroyed);
        // beforeDestroys are only triggered at server1 since the removeAll is submitted from
        // client1
        assertEquals(0, mywriter.num_destroyed);
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    {
      // Execute client putAll from multithread client
      AsyncInvocation async1 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll1 from client1") {
            @Override
            public void run2() throws CacheException {
              doPutAll(regionName, "async1key-", numberOfEntries);
            }
          });
      AsyncInvocation async2 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll2 from client1") {
            @Override
            public void run2() throws CacheException {
              doPutAll(regionName, "async2key-", numberOfEntries);
            }
          });

      ThreadUtils.join(async1, 30 * 1000);
      ThreadUtils.join(async2, 30 * 1000);
    }

    client1.invoke(new CacheSerializableRunnable(title + "verify client 1 for async keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries * 2, region.size());
        long ts1 = 0, ts2 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("async1key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();

          obj = (TestObject) region.getEntry("async2key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts2);
          ts2 = obj.getTS();
        }
      }
    });

    // verify cache server 1 for asyn keys
    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1 for async keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries * 2, region.size());
        long ts1 = 0, ts2 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("async1key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();

          obj = (TestObject) region.getEntry("async2key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts2);
          ts2 = obj.getTS();
        }
      }
    });
    // verify cache server 2 for asyn keys
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2 for async keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries * 2, region.size());
        long ts1 = 0, ts2 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("async1key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();

          obj = (TestObject) region.getEntry("async2key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts2);
          ts2 = obj.getTS();
        }
      }
    });
    client2.invoke(new CacheSerializableRunnable(title + "client2 verify async putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries * 2);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("async1key-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("async2key-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });

    {
      // Execute client removeAll from multithread client
      AsyncInvocation async1 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll1 from client1") {
            @Override
            public void run2() throws CacheException {
              doRemoveAll(regionName, "async1key-", numberOfEntries);
            }
          });
      AsyncInvocation async2 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll2 from client1") {
            @Override
            public void run2() throws CacheException {
              doRemoveAll(regionName, "async2key-", numberOfEntries);
            }
          });

      ThreadUtils.join(async1, 30 * 1000);
      ThreadUtils.join(async2, 30 * 1000);
    }

    client1.invoke(new CacheSerializableRunnable(title + "client1 removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doRemoveAll(regionName, "key-", numberOfEntries);
        assertEquals(0, region.size());
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify async removeAll cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify async removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify async removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    // Execute p2p putAll
    server1.invoke(new CacheSerializableRunnable(title + "server1 execute P2P putAll") {
      @Override
      public void run2() throws CacheException {
        doPutAll(regionName, "p2pkey-", numberOfEntries);
        Region region = getRootRegion().getSubregion(regionName);
        long ts1 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("p2pkey-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();
        }
      }
    });

    // verify cache server 2 for p2p keys
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2 for p2p keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        long ts1 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("p2pkey-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();
        }
      }
    });
    client2.invoke(new CacheSerializableRunnable(title + "client2 verify p2p putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("p2pkey-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });
    client1.invoke(new CacheSerializableRunnable(title + "client1 verify p2p putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("p2pkey-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });

    // Execute p2p removeAll
    server1.invoke(new CacheSerializableRunnable(title + "server1 execute P2P removeAll") {
      @Override
      public void run2() throws CacheException {
        doRemoveAll(regionName, "p2pkey-", numberOfEntries);
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });
    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify p2p removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify p2p removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 verify p2p removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    // putAll at client2 to trigger local-invalidates at client1
    client2.invoke(new CacheSerializableRunnable(title + "execute putAll on client2 for key 0-10") {
      @Override
      public void run2() throws CacheException {
        doPutAll(regionName, "key-", 10);
      }
    });

    // verify client 2 for key 0-10
    client1.invoke(new CacheSerializableRunnable(title + "verify client1 for local invalidate") {
      @Override
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < 10; i++) {
          final int ii = i;
          WaitCriterion ev = new WaitCriterion() {
            @Override
            public boolean done() {
              Entry entry = region.getEntry("key-" + ii);
              return entry != null && entry.getValue() == null;
            }

            @Override
            public String description() {
              return null;
            }
          };
          Wait.waitForCriterion(ev, 10 * 1000, 1000, true);
          // local invalidate will set the value to null
          TestObject obj = null;
          obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(null, obj);
        }
      }
    });

    // clean up
    // Stop server
    stopBridgeServers(getCache());
  }

  /* same as test2Server(), but all the servers are using policy normal */
  private int createServerRegion(VM vm, final String regionName, final boolean CCE) {
    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(CCE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.NORMAL);
        createRootRegion(new AttributesFactory().create());
        Region region = createRegion(regionName, af.create());

        CacheServer server = getCache().addCacheServer();
        int port = 0;
        server.setPort(port);
        server.start();
        port = server.getPort();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  public void doTest2NormalServerCCE(boolean CCE) throws CacheException, InterruptedException {
    final String title = "doTest2NormalServerCCE=" + CCE + ":";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=false to test local-invalidates
    int serverPort1 = createServerRegion(server1, regionName, CCE);
    int serverPort2 = createServerRegion(server2, regionName, CCE);
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, 59000, true);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2}, -1, 59000, true);

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
      }
    });

    // test case 1: putAll and removeAll to server1
    client1.invoke(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        doPutAll(regionName, "case1-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("case1-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
        region.put(numberOfEntries, "specialvalue");
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().setCacheWriter(new MyWriter("case1-"));
        region.localDestroy(numberOfEntries);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("case1-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().setCacheWriter(new MyWriter("case1-"));
        // normal policy will not distribute create events
        assertEquals(0, region.size());
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 removeAll") {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries + 1, region.size());
        // do removeAll with some keys not exist
        doRemoveAll(regionName, "case1-", numberOfEntries * 2);
        assertEquals(1, region.size());
        region.localDestroy(numberOfEntries); // do cleanup
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
        MyWriter mywriter = (MyWriter) region.getAttributes().getCacheWriter();
        LogWriterUtils.getLogWriter()
            .info("server cachewriter triggered for destroy: " + mywriter.num_destroyed);
        assertEquals(numberOfEntries, mywriter.num_destroyed);
      }
    });

    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
        MyWriter mywriter = (MyWriter) region.getAttributes().getCacheWriter();
        LogWriterUtils.getLogWriter()
            .info("server cachewriter triggered for destroy: " + mywriter.num_destroyed);
        // beforeDestroys are only triggered at server1 since the removeAll is submitted from
        // client1
        assertEquals(0, mywriter.num_destroyed);
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    // test case 2: putAll to server1, removeAll to server2
    client1.invoke(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doPutAll(regionName, "case2-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("case2-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("case2-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // normal policy will not distribute create events
        assertEquals(0, region.size());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client1 removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doRemoveAll(regionName, "case2-", numberOfEntries);
        assertEquals(0, region.size());
      }
    });

    server1.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(100, region.size());
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 verify removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 100);
        doRemoveAll(regionName, "case2-", numberOfEntries); // do cleanup
      }
    });

    // test case 3: removeAll a list with duplicated keys
    client1.invoke(new CacheSerializableRunnable(title + "put 3 keys then removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.put("case3-1", "case3-1");
        region.put("case3-2", "case3-2");
        region.put("case3-3", "case3-3");
        assertEquals(3, region.size());
        ArrayList keys = new ArrayList();
        keys.add("case3-1");
        keys.add("case3-2");
        keys.add("case3-3");
        keys.add("case3-1");
        region.removeAll(keys, "removeAllCallback");
        assertEquals(0, region.size());
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    // clean up
    // Stop server
    stopBridgeServers(getCache());
  }

  @Test
  public void test2NormalServerCCE() throws CacheException, InterruptedException {
    doTest2NormalServerCCE(true);
    disconnectAllFromDS();
    doTest2NormalServerCCE(false);
    disconnectAllFromDS();
  }

  @Test
  public void testPRServerRVDuplicatedKeys() throws CacheException, InterruptedException {
    doRVDuplicatedKeys(true, 1);
    disconnectAllFromDS();
    doRVDuplicatedKeys(true, 0);
    disconnectAllFromDS();
    doRVDuplicatedKeys(false, 1);
    disconnectAllFromDS();
  }

  public void doRVDuplicatedKeys(final boolean isPR, final int redundantCopies)
      throws CacheException, InterruptedException {
    final String title = "doRVDuplicatedKeys:";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, isPR, redundantCopies, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, isPR, redundantCopies, null);

    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, 59000, false);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2}, -1, 59000, false);

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        doPutAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    // test case 3: removeAll a list with duplicated keys
    client1.invoke(new CacheSerializableRunnable(title + "put 3 keys then removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.put("case3-1", "case3-1");
        region.put("case3-2", "case3-2");
        region.put("case3-3", "case3-3");
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "verify cache server 2") {
      @Override
      public void run2() throws CacheException {
        Wait.pause(5000);
        Region region = getRootRegion().getSubregion(regionName);
        Region.Entry re = region.getEntry("case3-1");
        assertNotNull(re);
        re = region.getEntry("case3-2");
        assertNotNull(re);
        re = region.getEntry("case3-3");
        assertNotNull(re);
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "put 3 keys then removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        ArrayList keys = new ArrayList();
        keys.add("case3-1");
        keys.add("case3-2");
        keys.add("case3-3");
        keys.add("case3-1");
        region.removeAll(keys, "removeAllCallback");
        Region.Entry re = region.getEntry("case3-1");
        assertNull(re);
        re = region.getEntry("case3-2");
        assertNull(re);
        re = region.getEntry("case3-3");
        assertNull(re);
      }
    });

    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        Region.Entry re = region.getEntry("case3-1");
        assertNull(re);
        re = region.getEntry("case3-2");
        assertNull(re);
        re = region.getEntry("case3-3");
        assertNull(re);
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        Region.Entry re = region.getEntry("case3-1");
        assertNull(re);
        re = region.getEntry("case3-2");
        assertNull(re);
        re = region.getEntry("case3-3");
        assertNull(re);
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "verify cache server 2") {
      @Override
      public void run2() throws CacheException {
        Wait.pause(5000);
        Region region = getRootRegion().getSubregion(regionName);
        Region.Entry re = region.getEntry("case3-1");
        assertNull(re);
        re = region.getEntry("case3-2");
        assertNull(re);
        re = region.getEntry("case3-3");
        assertNull(re);
      }
    });

    // clean up
    // Stop server
    stopBridgeServers(getCache());
  }

  @Test
  public void testBug51725() throws CacheException, InterruptedException {
    doBug51725(false);
    disconnectAllFromDS();
  }

  @Test
  public void testBug51725_singlehup() throws CacheException, InterruptedException {
    doBug51725(true);
    disconnectAllFromDS();
  }

  /**
   * One failure found in bug 51725, both for putAll and removeAll: non-singlehop with redundency=1.
   * Do some singlehop putAll to see if retry succeeded after PRE This is a singlehop putAll test.
   */
  public void doBug51725(boolean enableSingleHop) throws CacheException, InterruptedException {
    final String title = "doBug51725:";
    int client1Size;
    int client2Size;
    int server1Size;
    int server2Size;

    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client1 = host.getVM(2);
    final VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 0, "ds1");
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 0, "ds1");


    createClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, 59000, false, false,
        enableSingleHop);
    createClient(client2, regionName, serverHost, new int[] {serverPort1}, -1, 59000, false, false,
        enableSingleHop);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "put 3 keys then removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        // do putAll to create all buckets
        doPutAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
      }
    });

    closeCache(server2);
    client1.invoke(new CacheSerializableRunnable(title + "putAll from client again") {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        try {
          doPutAll(regionName, title, numberOfEntries);
          fail("Expect ServerOperationException caused by PutAllParitialResultException");
        } catch (ServerOperationException soe) {
          assertTrue(soe.getMessage()
              .contains(
                  String.format("Region %s putAll at server applied partial keys due to exception.",
                      region.getFullPath())));
          assertTrue(soe.getCause() instanceof RuntimeException);
        }
        assertEquals(numberOfEntries * 3 / 2, region.size());
        VersionTag tag;
        int cnt = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          tag = region.getVersionTag(title + i);
          if (tag != null && 1 == tag.getEntryVersion()) {
            cnt++;
          }
        }
        assertEquals(numberOfEntries / 2, cnt);

        try {
          doRemoveAll(regionName, title, numberOfEntries);
        } catch (ServerOperationException soe) {
          assertTrue(soe.getMessage()
              .contains(String.format(
                  "Region %s removeAll at server applied partial keys due to exception.",
                  region.getFullPath())));
          assertTrue(soe.getCause() instanceof RuntimeException);
        }
        Region.Entry re;
        // putAll only created 50 entries, removeAll removed them. So 100 entries are all NULL
        for (int i = 0; i < numberOfEntries; i++) {
          re = region.getEntry(title + i);
          assertNull(re);
        }
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "verify entries from client2") {
      @Override
      public void run2() throws CacheException {
        Wait.pause(5000);
        Region region = getRootRegion().getSubregion(regionName);
        Region.Entry re;
        for (int i = 0; i < numberOfEntries; i++) {
          re = region.getEntry(title + i);
          assertNull(re);
        }
      }
    });

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests putAll to 2 PR servers.
   */
  @Test
  public void testPRServer() throws CacheException, InterruptedException {
    final String title = "testPRServer:";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 1, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 1, null);
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, 59000, false);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2}, -1, 59000, false);

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        doPutAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().setCacheWriter(new MyWriter("key-"));
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });

    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().setCacheWriter(new MyWriter("key-"));
        assertEquals(numberOfEntries, region.size());
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(i, obj.getPrice());
        }
      }
    });
    client2.invoke(new CacheSerializableRunnable(title + "verify client2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("key-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doRemoveAll(regionName, "key-", numberOfEntries);
        assertEquals(0, region.size());
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
        MyWriter mywriter = (MyWriter) region.getAttributes().getCacheWriter();
        LogWriterUtils.getLogWriter()
            .info("server cachewriter triggered for destroy: " + mywriter.num_destroyed);
        // beforeDestroys are only triggered at primary buckets. server1 and server2 each holds half
        // of buckets
        assertEquals(numberOfEntries / 2, mywriter.num_destroyed);
      }
    });

    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
        MyWriter mywriter = (MyWriter) region.getAttributes().getCacheWriter();
        LogWriterUtils.getLogWriter()
            .info("server cachewriter triggered for destroy: " + mywriter.num_destroyed);
        // beforeDestroys are only triggered at primary buckets. server1 and server2 each holds half
        // of buckets
        assertEquals(numberOfEntries / 2, mywriter.num_destroyed);
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    // Execute client putAll from multithread client
    {
      AsyncInvocation async1 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll1 from client1") {
            @Override
            public void run2() throws CacheException {
              doPutAll(regionName, "async1key-", numberOfEntries);
            }
          });
      AsyncInvocation async2 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll2 from client1") {
            @Override
            public void run2() throws CacheException {
              doPutAll(regionName, "async2key-", numberOfEntries);
            }
          });

      ThreadUtils.join(async1, 30 * 1000);
      ThreadUtils.join(async2, 30 * 1000);
    }

    client1.invoke(new CacheSerializableRunnable(title + "verify client 1 for async keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(numberOfEntries * 2, region.size());
        long ts1 = 0, ts2 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("async1key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();

          obj = (TestObject) region.getEntry("async2key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts2);
          ts2 = obj.getTS();
        }
      }
    });

    // verify cache server 2 for asyn keys
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2 for async keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        long ts1 = 0, ts2 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("async1key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();

          obj = (TestObject) region.getEntry("async2key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts2);
          ts2 = obj.getTS();
        }
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify async putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries * 2);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("async1key-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("async2key-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });

    {
      // Execute client removeAll from multithread client
      AsyncInvocation async1 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll1 from client1") {
            @Override
            public void run2() throws CacheException {
              doRemoveAll(regionName, "async1key-", numberOfEntries);
            }
          });
      AsyncInvocation async2 =
          client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll2 from client1") {
            @Override
            public void run2() throws CacheException {
              doRemoveAll(regionName, "async2key-", numberOfEntries);
            }
          });

      ThreadUtils.join(async1, 30 * 1000);
      ThreadUtils.join(async2, 30 * 1000);
    }

    client1.invoke(new CacheSerializableRunnable(title + "client1 removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doRemoveAll(regionName, "key-", numberOfEntries);
        assertEquals(0, region.size());
      }
    });

    // verify cache server 1, its data are from client
    server1.invoke(new CacheSerializableRunnable(title + "verify async removeAll cache server 1") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify async removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify async removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    // Execute p2p putAll
    server1.invoke(new CacheSerializableRunnable(title + "server1 execute P2P putAll") {
      @Override
      public void run2() throws CacheException {
        doPutAll(regionName, "p2pkey-", numberOfEntries);
        Region region = getRootRegion().getSubregion(regionName);
        long ts1 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("p2pkey-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();
        }
      }
    });

    // verify cache server 2 for p2p keys
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2 for async keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        long ts1 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("p2pkey-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();
        }
      }
    });
    client2.invoke(new CacheSerializableRunnable(title + "client2 verify p2p putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("p2pkey-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });
    client1.invoke(new CacheSerializableRunnable(title + "client1 verify p2p putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
          Region.Entry re = region.getEntry("p2pkey-" + i);
          assertNotNull(re);
          assertEquals(null, re.getValue());
        }
      }
    });

    // Execute p2p removeAll
    server1.invoke(new CacheSerializableRunnable(title + "server1 execute P2P removeAll") {
      @Override
      public void run2() throws CacheException {
        doRemoveAll(regionName, "p2pkey-", numberOfEntries);
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });
    // verify cache server 2, because its data are from distribution
    server2.invoke(new CacheSerializableRunnable(title + "verify p2p removeAll cache server 2") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, region.size());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 verify p2p removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 verify p2p removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        checkRegionSize(region, 0);
      }
    });

    // putAll at client2 to trigger local-invalidates at client1
    client2.invoke(new CacheSerializableRunnable(title + "execute putAll on client2 for key 0-10") {
      @Override
      public void run2() throws CacheException {
        doPutAll(regionName, "key-", 10);
      }
    });

    // verify client 2 for key 0-10
    client1.invoke(new CacheSerializableRunnable(title + "verify client1 for local invalidate") {
      @Override
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < 10; i++) {
          final int ii = i;
          WaitCriterion ev = new WaitCriterion() {
            @Override
            public boolean done() {
              Entry entry = region.getEntry("key-" + ii);
              return entry != null && entry.getValue() == null;
            }

            @Override
            public String description() {
              return null;
            }
          };
          Wait.waitForCriterion(ev, 10 * 1000, 1000, true);
          // local invalidate will set the value to null
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          assertEquals(null, obj);
        }
      }
    });

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Checks to see if a client does a destroy that throws an exception from CacheWriter
   * beforeDestroy that the size of the region is still correct. See bug 51583.
   */
  @Test
  public void testClientDestroyOfUncreatedEntry() throws CacheException, InterruptedException {
    final String title = "testClientDestroyOfUncreatedEntry:";

    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client1 = host.getVM(1);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    createClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, -1, false, true,
        true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));

    server1.invoke(new CacheSerializableRunnable(title + "server1 add cacheWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Install cacheWriter that causes the very first destroy to fail
        region.getAttributesMutator().setCacheWriter(new MyWriter(0));
      }
    });

    assertEquals(0, getRegionSize(server1, regionName));
    client1.invoke(new CacheSerializableRunnable(title + "client1 destroy") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        try {
          region.destroy("bogusKey");
          fail("Expect ServerOperationException caused by CacheWriterException");
        } catch (ServerOperationException expected) {
          assertTrue(expected.getCause() instanceof CacheWriterException);
        }
      }
    });
    assertEquals(0, getRegionSize(server1, regionName));

    server1.invoke(removeExceptionTag1(expectedExceptions));
    client1.invoke(removeExceptionTag1(expectedExceptions));

    stopBridgeServers(getCache());
  }

  /**
   * Tests partial key putAll and removeAll to 2 servers with local region
   */
  @Test
  public void testPartialKeyInLocalRegion() throws CacheException, InterruptedException {
    final String title = "testPartialKeyInLocalRegion:";

    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client1 = host.getVM(2);
    final VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);
    createClient(client1, regionName, serverHost, new int[] {serverPort1}, -1, -1, false, true,
        true);
    createClient(client2, regionName, serverHost, new int[] {serverPort1}, -1, -1, false, true,
        true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    server1.invoke(new CacheSerializableRunnable(title + "server1 add cacheWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // let the server to trigger exception after created 15 keys
        region.getAttributesMutator().setCacheWriter(new MyWriter(15));
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client1 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        // create keys
        try {
          doPutAll(regionName, title, numberOfEntries);
          fail("Expect ServerOperationException caused by PutAllParitialResultException");
        } catch (ServerOperationException soe) {
          assertTrue(soe.getMessage()
              .contains(
                  String.format("Region %s putAll at server applied partial keys due to exception.",
                      region.getFullPath())));
          assertTrue(soe.getCause() instanceof RuntimeException);
          assertTrue(soe.getCause().getMessage()
              .contains("Triggered exception as planned, created 15 keys"));
        }
      }
    });

    {
      WaitCriterion waitForSizes = new WaitCriterion() {
        @Override
        public String description() {
          return "waiting for conditions to be met";
        }

        @Override
        public boolean done() {
          int c1Size = getRegionSize(client1, regionName);
          int c2Size = getRegionSize(client2, regionName);
          int s1Size = getRegionSize(server1, regionName);
          int s2Size = getRegionSize(server2, regionName);
          LogWriterUtils.getLogWriter()
              .info("region sizes: " + c1Size + "," + c2Size + "," + s1Size + "," + s2Size);
          if (c1Size != 15) {
            LogWriterUtils.getLogWriter().info("waiting for client1 to get all updates");
            return false;
          }
          if (c2Size != 15) {
            LogWriterUtils.getLogWriter().info("waiting for client2 to get all updates");
            return false;
          }
          if (s1Size != 15) {
            LogWriterUtils.getLogWriter().info("waiting for server1 to get all updates");
            return false;
          }
          if (s2Size != 15) {
            LogWriterUtils.getLogWriter().info("waiting for server2 to get all updates");
            return false;
          }
          return true;
        }
      };
      Wait.waitForCriterion(waitForSizes, 10000, 1000, true);
    }
    int server1Size = getRegionSize(server1, regionName);
    int server2Size = getRegionSize(server1, regionName);

    // reset cacheWriter's count to allow another 15 keys to be created
    server1.invoke(new CacheSerializableRunnable(title + "server1 add cacheWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // let the server to trigger exception after created 15 keys
        region.getAttributesMutator().setCacheWriter(new MyWriter(15));
      }
    });

    // p2p putAll on DR and expect exception
    server2.invoke(new CacheSerializableRunnable(title + "server2 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        // create keys
        try {
          doPutAll(regionName, title + "again:", numberOfEntries);
          fail("Expect original RuntimeException caused by cacheWriter");
        } catch (RuntimeException rte) {
          assertTrue(rte.getMessage().contains("Triggered exception as planned, created 15 keys"));
        }
      }
    });

    server2Size = getRegionSize(server1, regionName);
    assertEquals(server1Size + 15, server2Size);

    {
      WaitCriterion waitForSizes = new WaitCriterion() {
        @Override
        public String description() {
          return "waiting for conditions to be met";
        }

        @Override
        public boolean done() {
          int c1Size = getRegionSize(client1, regionName);
          int c2Size = getRegionSize(client2, regionName);
          int s1Size = getRegionSize(server1, regionName);
          int s2Size = getRegionSize(server2, regionName);
          LogWriterUtils.getLogWriter()
              .info("region sizes: " + c1Size + "," + c2Size + "," + s1Size + "," + s2Size);
          if (c1Size != 15) { // client 1 did not register interest
            LogWriterUtils.getLogWriter().info("waiting for client1 to get all updates");
            return false;
          }
          if (c2Size != 15 * 2) {
            LogWriterUtils.getLogWriter().info("waiting for client2 to get all updates");
            return false;
          }
          if (s1Size != 15 * 2) {
            LogWriterUtils.getLogWriter().info("waiting for server1 to get all updates");
            return false;
          }
          if (s2Size != 15 * 2) {
            LogWriterUtils.getLogWriter().info("waiting for server2 to get all updates");
            return false;
          }
          return true;
        }
      };
      Wait.waitForCriterion(waitForSizes, 10000, 1000, true);
    }

    // now do a removeAll that is not allowed to remove everything
    server1.invoke(new CacheSerializableRunnable(title + "server1 add cacheWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // server triggers exception after destroying 5 keys
        region.getAttributesMutator().setCacheWriter(new MyWriter(5));
      }
    });
    client1.invoke(new CacheSerializableRunnable(title + "client1 removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        // create keys
        try {
          doRemoveAll(regionName, title, numberOfEntries);
          fail("Expect ServerOperationException caused by PutAllParitialResultException");
        } catch (ServerOperationException soe) {
          assertTrue(soe.getMessage()
              .contains(String.format(
                  "Region %s removeAll at server applied partial keys due to exception.",
                  region.getFullPath())));
          assertTrue(soe.getCause() instanceof RuntimeException);
          assertTrue(soe.getCause().getMessage()
              .contains("Triggered exception as planned, destroyed 5 keys"));
        }
      }
    });
    {
      WaitCriterion waitForSizes = new WaitCriterion() {
        @Override
        public String description() {
          return "waiting for conditions to be met";
        }

        @Override
        public boolean done() {
          int c1Size = getRegionSize(client1, regionName);
          int c2Size = getRegionSize(client2, regionName);
          int s1Size = getRegionSize(server1, regionName);
          int s2Size = getRegionSize(server2, regionName);
          LogWriterUtils.getLogWriter()
              .info("region sizes: " + c1Size + "," + c2Size + "," + s1Size + "," + s2Size);
          if (c1Size != 15 - 5) { // client 1 did not register interest
            LogWriterUtils.getLogWriter().info("waiting for client1 to get all destroys");
            return false;
          }
          if (c2Size != (15 * 2) - 5) {
            LogWriterUtils.getLogWriter().info("waiting for client2 to get all destroys");
            return false;
          }
          if (s1Size != (15 * 2) - 5) {
            LogWriterUtils.getLogWriter().info("waiting for server1 to get all destroys");
            return false;
          }
          if (s2Size != (15 * 2) - 5) {
            LogWriterUtils.getLogWriter().info("waiting for server2 to get all destroys");
            return false;
          }
          return true;
        }
      };
      Wait.waitForCriterion(waitForSizes, 10000, 1000, true);
    }

    // reset cacheWriter's count to allow another 5 keys to be destroyed
    server1.invoke(new CacheSerializableRunnable(title + "server1 add cacheWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // server triggers exception after destroying 5 keys
        region.getAttributesMutator().setCacheWriter(new MyWriter(5));
      }
    });

    // p2p putAll on DR and expect exception
    server2.invoke(new CacheSerializableRunnable(title + "server2 add listener and removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        // create keys
        try {
          doRemoveAll(regionName, title + "again:", numberOfEntries);
          fail("Expect original RuntimeException caused by cacheWriter");
        } catch (RuntimeException rte) {
          assertTrue(rte.getMessage().contains("Triggered exception as planned, destroyed 5 keys"));
        }
      }
    });

    {
      WaitCriterion waitForSizes = new WaitCriterion() {
        @Override
        public String description() {
          return "waiting for conditions to be met";
        }

        @Override
        public boolean done() {
          int c1Size = getRegionSize(client1, regionName);
          int c2Size = getRegionSize(client2, regionName);
          int s1Size = getRegionSize(server1, regionName);
          int s2Size = getRegionSize(server2, regionName);
          LogWriterUtils.getLogWriter()
              .info("region sizes: " + c1Size + "," + c2Size + "," + s1Size + "," + s2Size);
          if (c1Size != 15 - 5) { // client 1 did not register interest
            LogWriterUtils.getLogWriter().info("waiting for client1 to get all destroys");
            return false;
          }
          if (c2Size != (15 * 2) - 5 - 5) {
            LogWriterUtils.getLogWriter().info("waiting for client2 to get all destroys");
            return false;
          }
          if (s1Size != (15 * 2) - 5 - 5) {
            LogWriterUtils.getLogWriter().info("waiting for server1 to get all destroys");
            return false;
          }
          if (s2Size != (15 * 2) - 5 - 5) {
            LogWriterUtils.getLogWriter().info("waiting for server2 to get all destroys");
            return false;
          }
          return true;
        }
      };
      Wait.waitForCriterion(waitForSizes, 10000, 1000, true);
    }
    server1.invoke(removeExceptionTag1(expectedExceptions));
    server2.invoke(removeExceptionTag1(expectedExceptions));
    client1.invoke(removeExceptionTag1(expectedExceptions));
    client2.invoke(removeExceptionTag1(expectedExceptions));

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests partial key putAll to 2 PR servers, because putting data at server side is different
   * between PR and LR. PR does it in postPutAll. It's not running in singleHop putAll
   */
  @Test
  public void testPartialKeyInPR() throws CacheException, InterruptedException {
    final String title = "testPartialKeyInPR:";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 0, "ds1");
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 0, "ds1");
    createClient(client1, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1, -1,
        false, false, true);
    createClient(client2, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1, -1,
        false, false, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    final SharedCounter sc_server2 = new SharedCounter("server2");

    server2.invoke(new CacheSerializableRunnable(title + "server2 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator()
            .addCacheListener(new MyListener(server2, true, sc_server2, 10));
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    AsyncInvocation async1 = client1
        .invokeAsync(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            region.getAttributesMutator().addCacheListener(new MyListener(false));
            region.registerInterest("ALL_KEYS");

            // create keys
            try {
              doPutAll(regionName, title, numberOfEntries);
              fail("Expect ServerOperationException caused by PutAllParitialResultException");
            } catch (ServerOperationException soe) {
              if (!(soe.getCause() instanceof PartitionOfflineException)) {
                throw soe;
              }
              if (!soe.getMessage()
                  .contains(String.format(
                      "Region %s putAll at server applied partial keys due to exception.",
                      region.getFullPath()))) {
                throw soe;
              }
            }
          }
        });

    // server2 will closeCache after created 10 keys

    ThreadUtils.join(async1, 30 * 1000);
    if (async1.exceptionOccurred()) {
      Assert.fail("Aync1 get exceptions:", async1.getException());
    }

    int client1Size = getRegionSize(client1, regionName);
    // client2Size maybe more than client1Size
    int client2Size = getRegionSize(client2, regionName);
    int server1Size = getRegionSize(server1, regionName);
    LogWriterUtils.getLogWriter()
        .info("region sizes: " + client1Size + "," + client2Size + "," + server1Size);

    // restart server2
    createBridgeServer(server2, regionName, serverPort2, true, 0, "ds1");
    server1Size = getRegionSize(server1, regionName);
    int server2Size = getRegionSize(server2, regionName);
    LogWriterUtils.getLogWriter().info("region sizes after server2 restarted: " + client1Size + ","
        + client2Size + "," + server1Size + ":" + server2Size);
    assertEquals(client2Size, server1Size);
    assertEquals(client2Size, server2Size);

    // close a server to re-run the test
    closeCache(server2);
    server1Size = getRegionSize(server1, regionName);
    client1.invoke(new CacheSerializableRunnable(title + "client1 does putAll again") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        // create keys
        try {
          doPutAll(regionName, title + "again:", numberOfEntries);
          fail("Expect ServerOperationException caused by PutAllParitialResultException");
        } catch (ServerOperationException soe) {
          assertTrue(soe.getMessage()
              .contains(
                  String.format("Region %s putAll at server applied partial keys due to exception.",
                      region.getFullPath())));
          assertTrue(soe.getCause() instanceof PartitionOfflineException);
        }
      }
    });

    int new_server1Size = getRegionSize(server1, regionName);
    int new_client1Size = getRegionSize(client1, regionName);
    int new_client2Size = getRegionSize(client2, regionName);

    LogWriterUtils.getLogWriter().info("region sizes after re-run the putAll: " + new_client1Size
        + "," + new_client2Size + "," + new_server1Size);
    assertEquals(server1Size + numberOfEntries / 2, new_server1Size);
    assertEquals(client1Size + numberOfEntries / 2, new_client1Size);
    assertEquals(client2Size + numberOfEntries / 2, new_client2Size);

    // restart server2
    createBridgeServer(server2, regionName, serverPort2, true, 0, "ds1");
    server1Size = getRegionSize(server1, regionName);
    server2Size = getRegionSize(server2, regionName);
    LogWriterUtils.getLogWriter()
        .info("region sizes after restart server2: " + server1Size + "," + server2Size);
    assertEquals(server1Size, server2Size);

    // add a cacheWriter for server to stop after created 15 keys
    server1.invoke(new CacheSerializableRunnable(title + "server1 execute P2P putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // let the server to trigger exception after created 15 keys
        region.getAttributesMutator().setCacheWriter(new MyWriter(15));
      }
    });

    // p2p putAll on PR and expect exception
    server2.invoke(new CacheSerializableRunnable(title + "server2 add listener and putAll") {
      @Override
      public void run2() throws CacheException {
        // create keys
        try {
          doPutAll(regionName, title + "once again:", numberOfEntries);
          fail("Expected a CacheWriterException to be thrown by test");
        } catch (CacheWriterException rte) {
          assertTrue(rte.getMessage().contains("Triggered exception as planned, created 15 keys"));
        }
      }
    });

    new_server1Size = getRegionSize(server1, regionName);
    int new_server2Size = getRegionSize(server2, regionName);
    LogWriterUtils.getLogWriter()
        .info("region sizes after restart server2: " + new_server1Size + "," + new_server2Size);
    assertEquals(server1Size + 15, new_server1Size);
    assertEquals(server2Size + 15, new_server2Size);
    server1.invoke(removeExceptionTag1(expectedExceptions));
    server2.invoke(removeExceptionTag1(expectedExceptions));
    client1.invoke(removeExceptionTag1(expectedExceptions));
    client2.invoke(removeExceptionTag1(expectedExceptions));

    // Stop server
    stopBridgeServers(getCache());
  }


  /**
   * Tests partial key putAll to 2 PR servers, because putting data at server side is different
   * between PR and LR. PR does it in postPutAll. This is a singlehop putAll test.
   */
  @Test
  public void testPartialKeyInPRSingleHop() throws CacheException, InterruptedException {
    final String title = "testPartialKeyInPRSingleHop_";
    final int cacheWriterAllowedKeyNum = 16;

    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client1 = host.getVM(2);
    final VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 0, "ds1");
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 0, "ds1");
    createClient(client1, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1, -1,
        false, false, true, false);
    createClient(client2, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1, -1,
        false, false, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    try {
      client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);
          region.getAttributesMutator().addCacheListener(new MyListener(false));

          region.registerInterest("ALL_KEYS");
          LogWriterUtils.getLogWriter()
              .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
        }
      });

      client1.invoke(new CacheSerializableRunnable(
          title + "do some putAll to get ClientMetaData for future putAll") {
        @Override
        public void run2() throws CacheException {
          doPutAll(regionName, "key-", numberOfEntries);
        }
      });

      WaitCriterion waitForSizes = new WaitCriterion() {
        @Override
        public String description() {
          return "waiting for conditions to be met";
        }

        @Override
        public boolean done() {
          int c1Size = getRegionSize(client1, regionName);
          int c2Size = getRegionSize(client2, regionName);
          int s1Size = getRegionSize(server1, regionName);
          int s2Size = getRegionSize(server2, regionName);
          LogWriterUtils.getLogWriter()
              .info("region sizes: " + c1Size + "," + c2Size + "," + s1Size + "," + s2Size);
          if (c1Size != numberOfEntries) {
            LogWriterUtils.getLogWriter().info("waiting for client1 to get all updates");
            return false;
          }
          if (c2Size != numberOfEntries) {
            LogWriterUtils.getLogWriter().info("waiting for client2 to get all updates");
            return false;
          }
          if (s1Size != numberOfEntries) {
            LogWriterUtils.getLogWriter().info("waiting for server1 to get all updates");
            return false;
          }
          if (s2Size != numberOfEntries) {
            LogWriterUtils.getLogWriter().info("waiting for server2 to get all updates");
            return false;
          }
          return true;
        }
      };
      Wait.waitForCriterion(waitForSizes, 10000, 1000, true);

      server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);
          region.getAttributesMutator().addCacheListener(new MyListener(true));
        }
      });

      // add a listener that will close the cache at the 10th update
      final SharedCounter sc_server2 = new SharedCounter("server2");
      server2.invoke(new CacheSerializableRunnable(title + "server2 add slow listener") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);
          region.getAttributesMutator()
              .addCacheListener(new MyListener(server2, true, sc_server2, 10));
        }
      });

      AsyncInvocation async1 = client1
          .invokeAsync(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
            @Override
            public void run2() throws CacheException {
              Region region = getRootRegion().getSubregion(regionName);
              region.getAttributesMutator().addCacheListener(new MyListener(false));

              // create keys
              try {
                doPutAll(regionName, title, numberOfEntries);
                fail("Expect ServerOperationException caused by PutAllParitialResultException");
              } catch (ServerOperationException soe) {
                assertTrue(soe.getMessage()
                    .contains(String.format(
                        "Region %s putAll at server applied partial keys due to exception.",
                        region.getFullPath())));
              }
            }
          });

      // server2 will closeCache after creating 10 keys

      ThreadUtils.join(async1, 30 * 1000);
      if (async1.exceptionOccurred()) {
        Assert.fail("putAll client threw an exception", async1.getException());
      }

      // restart server2
      System.out.println("restarting server 2");
      createBridgeServer(server2, regionName, serverPort2, true, 0, "ds1");

      // Test Case1: Trigger singleHop putAll. Stop server2 in middle.
      // numberOfEntries/2 + X keys will be created at servers. i.e. X keys at server2,
      // numberOfEntries/2 keys at server1.
      // The client should receive a PartialResultException due to PartitionOffline

      // close a server to re-run the test
      closeCache(server2);
      client1.invoke(new CacheSerializableRunnable(title + "client1 does putAll again") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);

          // create keys
          try {
            doPutAll(regionName, title + "again:", numberOfEntries);
            fail("Expect ServerOperationException caused by PutAllParitialResultException");
          } catch (ServerOperationException soe) {
            assertTrue(soe.getMessage()
                .contains(String.format(
                    "Region %s putAll at server applied partial keys due to exception.",
                    region.getFullPath())));
          }
        }
      });

      // Test Case 2: based on case 1, but this time, there should be no X keys
      // created on server2.

      // restart server2
      createBridgeServer(server2, regionName, serverPort2, true, 0, "ds1");

      // add a cacheWriter for server to fail putAll after it created cacheWriterAllowedKeyNum keys
      server1.invoke(new CacheSerializableRunnable(
          title + "server1 add cachewriter to throw exception after created some keys") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);
          region.getAttributesMutator().setCacheWriter(new MyWriter(cacheWriterAllowedKeyNum));
        }
      });

      client1.invoke(new CacheSerializableRunnable(title + "client1 does putAll once more") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);

          // create keys
          try {
            doPutAll(regionName, title + "once more:", numberOfEntries);
            fail("Expect ServerOperationException caused by PutAllParitialResultException");
          } catch (ServerOperationException soe) {
            assertTrue(soe.getMessage()
                .contains(String.format(
                    "Region %s putAll at server applied partial keys due to exception.",
                    region.getFullPath())));
          }
        }
      });
    } finally {
      server1.invoke(removeExceptionTag1(expectedExceptions));
      server2.invoke(removeExceptionTag1(expectedExceptions));
      client1.invoke(removeExceptionTag1(expectedExceptions));
      client2.invoke(removeExceptionTag1(expectedExceptions));

      // Stop server
      stopBridgeServers(getCache());
    }
  }

  /**
   * Set redundancy=1 to see if retry succeeded after PRE This is a singlehop putAll test.
   */
  @Test
  public void testPartialKeyInPRSingleHopWithRedundency()
      throws CacheException, InterruptedException {
    final String title = "testPartialKeyInPRSingleHopWithRedundency_";
    int client1Size;
    int client2Size;
    int server1Size;
    int server2Size;

    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client1 = host.getVM(2);
    final VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 1, "ds1");
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 1, "ds1");
    createClient(client1, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1, -1,
        false, false, true, false);
    createClient(client2, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1, -1,
        false, false, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    client2.invoke(new CacheSerializableRunnable(title + "client2 add listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    client1.invoke(new CacheSerializableRunnable(
        title + "do some putAll to get ClientMetaData for future putAll") {
      @Override
      public void run2() throws CacheException {
        doPutAll(regionName, "key-", numberOfEntries);
      }
    });

    WaitCriterion waitForSizes = new WaitCriterion() {
      @Override
      public String description() {
        return "waiting for conditions to be met";
      }

      @Override
      public boolean done() {
        int c1Size = getRegionSize(client1, regionName);
        int c2Size = getRegionSize(client2, regionName);
        int s1Size = getRegionSize(server1, regionName);
        int s2Size = getRegionSize(server2, regionName);
        LogWriterUtils.getLogWriter()
            .info("region sizes: " + c1Size + "," + c2Size + "," + s1Size + "," + s2Size);
        if (c1Size != numberOfEntries) {
          LogWriterUtils.getLogWriter().info("waiting for client1 to get all updates");
          return false;
        }
        if (c2Size != numberOfEntries) {
          LogWriterUtils.getLogWriter().info("waiting for client2 to get all updates");
          return false;
        }
        if (s1Size != numberOfEntries) {
          LogWriterUtils.getLogWriter().info("waiting for server1 to get all updates");
          return false;
        }
        if (s2Size != numberOfEntries) {
          LogWriterUtils.getLogWriter().info("waiting for server2 to get all updates");
          return false;
        }
        return true;
      }
    };
    Wait.waitForCriterion(waitForSizes, 10000, 1000, true);

    client1Size = getRegionSize(client1, regionName);
    client2Size = getRegionSize(client2, regionName);
    server1Size = getRegionSize(server1, regionName);
    server2Size = getRegionSize(server2, regionName);

    server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    final SharedCounter sc_server2 = new SharedCounter("server2");
    server2.invoke(new CacheSerializableRunnable(title + "server2 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator()
            .addCacheListener(new MyListener(server2, true, sc_server2, 10));
      }
    });

    AsyncInvocation async1 = client1
        .invokeAsync(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            region.getAttributesMutator().addCacheListener(new MyListener(false));
            region.registerInterest("ALL_KEYS");

            // create keys
            doPutAll(regionName, title, numberOfEntries);
          }
        });

    // server2 will closeCache after created 10 keys

    ThreadUtils.join(async1, 30 * 1000);
    if (async1.exceptionOccurred()) {
      Assert.fail("Aync1 get exceptions:", async1.getException());
    }

    client1Size = getRegionSize(client1, regionName);
    // client2Size maybe more than client1Size
    client2Size = getRegionSize(client2, regionName);
    server1Size = getRegionSize(server1, regionName);
    // putAll should succeed after retry
    LogWriterUtils.getLogWriter()
        .info("region sizes: " + client1Size + "," + client2Size + "," + server1Size);
    assertEquals(server1Size, client1Size);
    assertEquals(server1Size, client2Size);

    // restart server2
    createBridgeServer(server2, regionName, serverPort2, true, 1, "ds1");

    server1Size = getRegionSize(server1, regionName);
    server2Size = getRegionSize(server2, regionName);
    LogWriterUtils.getLogWriter().info("region sizes after server2 restarted: " + client1Size + ","
        + client2Size + "," + server1Size);
    assertEquals(client2Size, server1Size);
    assertEquals(client2Size, server2Size);

    // close a server to re-run the test
    closeCache(server2);
    server1Size = getRegionSize(server1, regionName);
    client1.invoke(new CacheSerializableRunnable(title + "client1 does putAll again") {
      @Override
      public void run2() throws CacheException {
        doPutAll(regionName, title + "again:", numberOfEntries);
      }
    });

    int new_server1Size = getRegionSize(server1, regionName);
    int new_client1Size = getRegionSize(client1, regionName);
    int new_client2Size = getRegionSize(client2, regionName);

    // putAll should succeed, all the numbers should match
    LogWriterUtils.getLogWriter().info("region sizes after re-run the putAll: " + new_client1Size
        + "," + new_client2Size + "," + new_server1Size);
    assertEquals(new_server1Size, new_client1Size);
    assertEquals(new_server1Size, new_client2Size);

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests bug 41403: let 2 sub maps both failed with partial key applied. This is a singlehop
   * putAll test.
   */
  @Test
  public void testEventIdMisorderInPRSingleHop() throws CacheException, InterruptedException {
    final String title = "testEventIdMisorderInPRSingleHop_";

    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final VM client1 = host.getVM(3);
    final String regionName = getUniqueName();

    final int[] serverPorts = new int[3];
    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    final SharedCounter sc_server1 = new SharedCounter("server1");
    final SharedCounter sc_server2 = new SharedCounter("server2");
    final SharedCounter sc_server3 = new SharedCounter("server3");
    final SharedCounter sc_client2 = new SharedCounter("client2");

    // set <true, false> means <PR=true, notifyBySubscription=false> to test local-invalidates
    serverPorts[0] = createBridgeServer(server1, regionName, 0, true, 0, null);
    serverPorts[1] = createBridgeServer(server2, regionName, 0, true, 0, null);
    serverPorts[2] = createBridgeServer(server3, regionName, 0, true, 0, null);
    createClient(client1, regionName, serverHost, serverPorts, -1, -1, false, true, true, false);

    {
      // Create local region
      Properties config = new Properties();
      config.setProperty(MCAST_PORT, "0");
      config.setProperty(LOCATORS, "");
      getSystem(config);

      // Create Region
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);

      try {
        getCache();
        ClientServerTestCase.configureConnectionPool(factory, serverHost, serverPorts, true, -1, -1,
            null);
        createRegion(regionName, factory.create());
        assertNotNull(getRootRegion().getSubregion(regionName));
      } catch (CacheException ex) {
        Assert.fail("While creating Region on Edge", ex);
      }
    }

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    server3.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    addExceptionTag1(expectedExceptions);

    client1.invoke(new CacheSerializableRunnable(
        title + "do some putAll to get ClientMetaData for future putAll") {
      @Override
      public void run2() throws CacheException {
        doPutAll(regionName, "key-", numberOfEntries);
      }
    });

    // register interest and add listener
    MyListener myListener = new MyListener(false, sc_client2);
    Region region = getRootRegion().getSubregion(regionName);
    region.getAttributesMutator().addCacheListener(myListener);
    region.registerInterest("ALL_KEYS");

    server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion().getSubregion(regionName);
        r.getAttributesMutator().addCacheListener(new MyListener(server1, true, sc_server1, 10));
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "server2 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion().getSubregion(regionName);
        r.getAttributesMutator().addCacheListener(new MyListener(server2, true, sc_server2, 10));
      }
    });

    server3.invoke(new CacheSerializableRunnable(title + "server3 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion().getSubregion(regionName);
        r.getAttributesMutator().addCacheListener(new MyListener(true, sc_server3));
      }
    });

    int client1Size = getRegionSize(client1, regionName);
    int server1Size = getRegionSize(server1, regionName);
    int server2Size = getRegionSize(server2, regionName);
    int server3Size = getRegionSize(server2, regionName);
    LogWriterUtils.getLogWriter().info(
        "region sizes: " + client1Size + "," + server1Size + "," + server2Size + "," + server3Size);

    AsyncInvocation async1 = client1
        .invokeAsync(new CacheSerializableRunnable(title + "client1 add listener and putAll") {
          @Override
          public void run2() throws CacheException {
            Region r = getRootRegion().getSubregion(regionName);
            r.getAttributesMutator().addCacheListener(new MyListener(false));
            doPutAll(regionName, title, numberOfEntries);
          }
        });

    // server1 and server2 will closeCache after created 10 keys

    ThreadUtils.join(async1, 30 * 1000);
    if (async1.exceptionOccurred()) {
      Assert.fail("Aync1 get exceptions:", async1.getException());
    }

    server3.invoke(new CacheSerializableRunnable(title + "server3 print counter") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion().getSubregion(regionName);
        MyListener l = (MyListener) r.getAttributes().getCacheListeners()[0];
        LogWriterUtils.getLogWriter().info("event counters : " + l.sc);
        assertEquals(numberOfEntries, l.sc.num_create_event);
        assertEquals(0, l.sc.num_update_event);
      }
    });

    LogWriterUtils.getLogWriter().info("event counters before wait : " + myListener.sc);
    await()
        .untilAsserted(() -> assertEquals(numberOfEntries, myListener.sc.num_create_event));
    LogWriterUtils.getLogWriter().info("event counters after wait : " + myListener.sc);
    assertEquals(0, myListener.sc.num_update_event);

    server1.invoke(removeExceptionTag1(expectedExceptions));
    server2.invoke(removeExceptionTag1(expectedExceptions));
    server3.invoke(removeExceptionTag1(expectedExceptions));
    client1.invoke(removeExceptionTag1(expectedExceptions));
    removeExceptionTag1(expectedExceptions);

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests while putAll to 2 distributed servers, one server failed over Add a listener to slow down
   * the processing of putAll
   */
  @Test
  public void test2FailOverDistributedServer() throws CacheException, InterruptedException {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    final String title = "test2FailOverDistributedServer:";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);

    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1,
        -1, true);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2, serverPort1}, -1,
        -1, true);

    server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "server2 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    client1.invoke(new CacheSerializableRunnable(title + "client1 registerInterest") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        // registerInterest for ALL_KEYS
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client1 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 registerInterest") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));

        // registerInterest for ALL_KEYS
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    // Execute client putAll from multithread client
    AsyncInvocation async1 =
        client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll1 from client1") {
          @Override
          public void run2() throws CacheException {
            doPutAll(regionName, "async1key-", numberOfEntries);
          }
        });


    Wait.pause(2000);
    server1.invoke(new CacheSerializableRunnable(title + "stop cache server 1") {
      @Override
      public void run2() throws CacheException {
        stopOneBridgeServer(serverPort1);
      }
    });

    ThreadUtils.join(async1, 30 * 1000);

    // verify cache server 2 for asyn keys
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server 2 for async keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        long ts1 = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("async1key-" + i).getValue();
          assertEquals(i, obj.getPrice());
          assertTrue(obj.getTS() >= ts1);
          ts1 = obj.getTS();
        }
      }
    });

    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests while putAll timeout's exception
   */
  @Test
  public void testClientTimeOut() throws CacheException, InterruptedException {
    final String title = "testClientTimeOut:";
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);

    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1, serverPort2}, -1,
        -1, true);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2, serverPort1}, -1,
        -1, true);

    server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "server2 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }

    });

    // Execute client putAll
    client1.invoke(new CacheSerializableRunnable(title + "client1 execute putAll") {
      @Override
      public void run2() throws CacheException {
        boolean exceptionTriggered = false;
        try {
          doPutAll(regionName, "key-", thousandEntries);
        } catch (Exception e) {
          LogWriterUtils.getLogWriter().info(title + "Expected SocketTimeOut:" + e.getMessage());
          exceptionTriggered = true;
        }
        assertTrue(exceptionTriggered);
      }
    });

    // clean up
    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests while putAll timeout at endpoint1 and switch to endpoint2
   */
  @Test
  public void testEndPointSwitch() throws CacheException, InterruptedException {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    final String title = "testEndPointSwitch:";
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);

    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1, serverPort2}, 1, -1,
        true);
    createClient(client2, regionName, serverHost, new int[] {serverPort2, serverPort1}, 1, -1,
        false, true, true);

    // only add slow listener to server1, because we wish it to succeed
    server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    // only register interest on client2
    client2.invoke(new CacheSerializableRunnable(title + "client2 registerInterest") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    // Execute client1 putAll
    client1.invoke(new CacheSerializableRunnable(title + "putAll from client1") {
      @Override
      public void run2() throws CacheException {
        try {
          doPutAll(regionName, title, testEndPointSwitchNumber);
        } catch (Exception e) {
          LogWriterUtils.getLogWriter().info(title + "Expected SocketTimeOut" + e.getMessage());
        }
      }
    });

    // verify client 2 for all keys
    client2.invoke(
        new CacheSerializableRunnable(title + "verify Bridge client2 for keys arrived finally") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            waitTillNotify(lockObject3, 100000, (region.size() == testEndPointSwitchNumber));
            assertEquals(testEndPointSwitchNumber, region.size());
          }
        });

    // clean up
    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Tests while putAll to 2 distributed servers, one server failed over Add a listener to slow down
   * the processing of putAll
   */
  @Test
  public void testHADRFailOver() throws CacheException, InterruptedException {
    final String title = "testHADRFailOver:";
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);
    // set queueRedundency=1
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1, serverPort2}, 1, -1,
        true);
    createClient(client2, regionName, serverHost, new int[] {serverPort2, serverPort1}, 1, -1,
        false, true, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    client1.invoke(new CacheSerializableRunnable(title + "client1 registerInterest") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client1 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 registerInterest") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(false));
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    // Server2 do a putAll to use HARegionQueues
    AsyncInvocation async1 =
        server2.invokeAsync(new CacheSerializableRunnable(title + "async putAll1 from server2") {
          @Override
          public void run2() throws CacheException {
            doPutAll(regionName, title + "1:", thousandEntries);
          }
        });

    // client1 do a putAll to start HARegionQueues
    AsyncInvocation async2 =
        client1.invokeAsync(new CacheSerializableRunnable(title + "async putAll1 from client1") {
          @Override
          public void run2() throws CacheException {
            doPutAll(regionName, title + "2:", thousandEntries);
          }
        });

    Wait.pause(2000);
    server1.invoke(new CacheSerializableRunnable(title + "stop cache server 1") {
      @Override
      public void run2() throws CacheException {
        stopOneBridgeServer(serverPort1);
      }
    });

    ThreadUtils.join(async1, 30 * 1000);
    ThreadUtils.join(async2, 30 * 1000);

    // verify client 2 for asyn keys
    client2
        .invokeAsync(new CacheSerializableRunnable(title + "verify Bridge client2 for async keys") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            waitTillNotify(lockObject4, 100000, (region.size() == thousandEntries * 2));
            assertEquals(thousandEntries * 2, region.size());
          }
        });

    server1.invoke(removeExceptionTag1(expectedExceptions));
    server2.invoke(removeExceptionTag1(expectedExceptions));
    client1.invoke(removeExceptionTag1(expectedExceptions));
    client2.invoke(removeExceptionTag1(expectedExceptions));

    // clean up
    // Stop server
    stopBridgeServers(getCache());
  }

  /**
   * Test TX for putAll. There's no TX for c/s. We only test P2P This is disabled because putAll in
   * TX is disabled.
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testTX() throws CacheException, InterruptedException {
    final String title = "testTX:";
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    final String regionName = getUniqueName();

    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);
    // final String serverHost = getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest



    // add slow listener
    server1.invoke(new CacheSerializableRunnable(title + "server1 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    server2.invoke(new CacheSerializableRunnable(title + "server2 add slow listener") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.getAttributesMutator().addCacheListener(new MyListener(true));
      }
    });

    // TX1: server1 do a putAll
    AsyncInvocation async1 = server1
        .invokeAsync(new CacheSerializableRunnable(title + "TX1: async putAll from server1") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            // Get JNDI(Java Naming and Directory interface) context
            // CacheTransactionManager tx = getCache().getCacheTransactionManager();
            LinkedHashMap map = new LinkedHashMap();
            // tx.begin();
            for (int i = 0; i < numberOfEntries; i++) {
              // region.put("key-"+i, new TestObject(i));
              map.put("key-" + i, new TestObject(i));
            }
            region.putAll(map, "putAllCallback");
            try {
              LogWriterUtils.getLogWriter().info("before commit TX1");
              // tx.commit();
              LogWriterUtils.getLogWriter().info("TX1 committed");
            } catch (CommitConflictException e) {
              LogWriterUtils.getLogWriter().info("TX1 rollbacked");
            }
          }
        });

    // we have to pause a while to let TX1 finish earlier
    Wait.pause(500);
    // TX2: server2 do a putAll
    AsyncInvocation async2 = server2
        .invokeAsync(new CacheSerializableRunnable(title + "TX2: async putAll from server2") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            // Get JNDI(Java Naming and Directory interface) context
            // CacheTransactionManager tx = getCache().getCacheTransactionManager();
            LinkedHashMap map = new LinkedHashMap();
            // tx.begin();
            for (int i = 0; i < numberOfEntries; i++) {
              // region.put("key-"+i, new TestObject(i + numberOfEntries));
              map.put("key-" + i, new TestObject(i + numberOfEntries));
            }
            region.putAll(map, "putAllCallback");
            try {
              LogWriterUtils.getLogWriter().info("before commit TX2");
              // tx.commit();
              LogWriterUtils.getLogWriter().info("TX2 committed");
            } catch (CommitConflictException e) {
              LogWriterUtils.getLogWriter().info("TX2 rollbacked");
            }
          }
        });

    // TX3: server2 do a putAll in another thread
    AsyncInvocation async3 = server2
        .invokeAsync(new CacheSerializableRunnable(title + "TX3: async putAll from server2") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);

            // Get JNDI(Java Naming and Directory interface) context
            // CacheTransactionManager tx = getCache().getCacheTransactionManager();

            LinkedHashMap map = new LinkedHashMap();
            // Can't do tx with putall anymore
            // tx.begin();
            for (int i = 0; i < numberOfEntries; i++) {
              // region.put("key-"+i, new TestObject(i+numberOfEntries*2));
              map.put("key-" + i, new TestObject(i + numberOfEntries * 2));
            }
            region.putAll(map, "putAllCallback");
            try {
              LogWriterUtils.getLogWriter().info("before commit TX3");
              // tx.commit();
              LogWriterUtils.getLogWriter().info("TX3 committed");
            } catch (CommitConflictException e) {
              LogWriterUtils.getLogWriter().info("TX3 rollbacked");
            }
          }
        });

    ThreadUtils.join(async1, 30 * 1000);
    ThreadUtils.join(async2, 30 * 1000);
    ThreadUtils.join(async3, 30 * 1000);

    // verify server 2 for asyn keys
    server2.invoke(new CacheSerializableRunnable(title + "verify cache server2 for keys") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        assertEquals(numberOfEntries, region.size());
        int tx_no = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          TestObject obj = (TestObject) region.getEntry("key-" + i).getValue();
          if (tx_no == 0) {
            // only check which tx took control once
            if (obj.getPrice() == i) {
              tx_no = 1;
            } else if (obj.getPrice() == i + numberOfEntries) {
              tx_no = 2;
            } else if (obj.getPrice() == i + numberOfEntries * 2) {
              tx_no = 3;
            }
            LogWriterUtils.getLogWriter().info("Verifying TX:" + tx_no);
          }
          if (tx_no == 1) {
            assertEquals(i, obj.getPrice());
          } else if (tx_no == 2) {
            assertEquals(i + numberOfEntries, obj.getPrice());
          } else {
            assertEquals(i + numberOfEntries * 2, obj.getPrice());
          }
        }
      }
    });

    // clean up

    // Stop server
    stopBridgeServers(getCache());
  }

  @Test
  public void testVersionsOnClientsWithNotificationsOnly() {
    final String title = "testVersionsInClients";
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 0, null);
    // set queueRedundency=1
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, 0, 59000, true);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2}, 0, 59000, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    client1.invoke(new CacheSerializableRunnable(title + "client1 putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doPutAll(regionName, "key-", numberOfEntries * 2);
        assertEquals(numberOfEntries * 2, region.size());
      }
    });


    client2.invoke(new CacheSerializableRunnable(title + "client2 versions collection") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    client1Versions = (List<VersionTag>) client1
        .invoke(new SerializableCallable(title + "client1 versions collection") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries * 2, region.size());
            List<VersionTag> versions = new ArrayList<VersionTag>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);
              VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
              LogWriterUtils.getLogWriter()
                  .info("Entry version tag on client for " + key + ": " + tag);
              versions.add(tag);
            }

            return versions;
          }
        });

    client2Versions = (List<VersionTag>) client2
        .invoke(new SerializableCallable(title + "client2 versions collection") {

          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries * 2, region.size());
            List<VersionTag> versions = new ArrayList<VersionTag>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);
              VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
              LogWriterUtils.getLogWriter()
                  .info("Entry version tag on client for " + key + ": " + tag);
              versions.add(tag);
            }
            return versions;
          }
        });

    assertEquals(numberOfEntries * 2, client1Versions.size());
    LogWriterUtils.getLogWriter().info(Arrays.toString(client1Versions.toArray()));

    LogWriterUtils.getLogWriter().info(Arrays.toString(client2Versions.toArray()));

    for (VersionTag tag : client1Versions) {
      if (!client2Versions.contains(tag)) {
        fail("client 2 does not have the tag contained in client 1" + tag);
      }
    }
  }

  /**
   * basically same test as testVersionsOnClientsWithNotificationsOnly but also do a removeAll
   */
  @Test
  public void testRAVersionsOnClientsWithNotificationsOnly() {
    final String title = "testRAVersionsInClients";
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 0, null);
    // set queueRedundency=1
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, 0, 59000, true);
    createBridgeClient(client2, regionName, serverHost, new int[] {serverPort2}, 0, 59000, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));
    client2.invoke(addExceptionTag1(expectedExceptions));

    client1.invoke(new CacheSerializableRunnable(title + "client1 putAll+removeAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doPutAll(regionName, "key-", numberOfEntries * 2);
        assertEquals(numberOfEntries * 2, region.size());
        doRemoveAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
      }
    });

    client2.invoke(new CacheSerializableRunnable(title + "client2 versions collection") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.registerInterest("ALL_KEYS");
        assertEquals(numberOfEntries, region.size());
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    client1RAVersions = (List<VersionTag>) client1
        .invoke(new SerializableCallable(title + "client1 versions collection") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries, region.size());
            List<VersionTag> versions = new ArrayList<VersionTag>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;
            assertEquals(numberOfEntries * 2, entries.size());

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);
              VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
              LogWriterUtils.getLogWriter()
                  .info("Entry version tag on client for " + key + ": " + tag);
              versions.add(tag);
            }

            return versions;
          }
        });

    client2RAVersions = (List<VersionTag>) client2
        .invoke(new SerializableCallable(title + "client2 versions collection") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries, region.size());
            List<VersionTag> versions = new ArrayList<VersionTag>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;
            assertEquals(numberOfEntries * 2, entries.size());

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);
              VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
              LogWriterUtils.getLogWriter()
                  .info("Entry version tag on client for " + key + ": " + tag);
              versions.add(tag);
            }
            return versions;
          }
        });

    assertEquals(numberOfEntries * 2, client1RAVersions.size());
    LogWriterUtils.getLogWriter().info(Arrays.toString(client1RAVersions.toArray()));

    LogWriterUtils.getLogWriter().info(Arrays.toString(client2RAVersions.toArray()));

    for (VersionTag tag : client1RAVersions) {
      if (!client2RAVersions.contains(tag)) {
        fail("client 2 does not have the tag contained in client 1" + tag);
      }
    }
  }

  @Test
  public void testVersionsOnServersWithNotificationsOnly() {
    final String title = "testVersionsOnServers";
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM client1 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 1, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 1, null);
    final int serverPort3 = createBridgeServer(server3, regionName, 0, true, 1, null);
    // set queueRedundency=1
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort3}, 0, 59000, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    server3.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));


    client1.invoke(new CacheSerializableRunnable(title + "client2 versions collection") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    server1.invoke(new CacheSerializableRunnable(title + "client1 putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doPutAll(regionName, "key-", numberOfEntries * 2);
        assertEquals(numberOfEntries * 2, region.size());
      }
    });

    expectedVersions = (List<String>) server1
        .invoke(new SerializableCallable(title + "server1 versions collection") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries * 2, region.size());
            List<String> versions = new ArrayList<String>(numberOfEntries * 2);

            Set<BucketRegion> buckets =
                ((PartitionedRegion) region).dataStore.getAllLocalPrimaryBucketRegions();

            for (BucketRegion br : buckets) {

              RegionMap entries = br.entries;

              for (Object key : entries.keySet()) {
                RegionEntry internalRegionEntry = entries.getEntry(key);
                VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
                LogWriterUtils.getLogWriter().info("Entry version tag on server1:" + tag);
                versions.add(key + " " + tag);
              }
            }

            return versions;
          }
        });

    // Let client be updated with all keys.
    Wait.pause(1000);

    actualVersions = (List<String>) client1
        .invoke(new SerializableCallable(title + "client2 versions collection") {

          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries * 2, region.size());
            List<String> versions = new ArrayList<String>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);
              VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
              tag.setMemberID(null);
              versions.add(key + " " + tag);
            }
            return versions;
          }
        });

    LogWriterUtils.getLogWriter().info(Arrays.toString(expectedVersions.toArray()));

    assertEquals(numberOfEntries * 2, actualVersions.size());
    LogWriterUtils.getLogWriter().info(Arrays.toString(actualVersions.toArray()));

    for (String keyTag : expectedVersions) {
      if (!actualVersions.contains(keyTag)) {
        fail("client 2 does not have the tag contained in client 1" + keyTag);
      }
    }

  }

  /**
   * Same test as testVersionsOnServersWithNotificationsOnly but also does a removeAll
   */
  @Test
  public void testRAVersionsOnServersWithNotificationsOnly() {

    final String title = "testRAVersionsOnServers";
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM client1 = host.getVM(3);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, true, 1, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, true, 1, null);
    final int serverPort3 = createBridgeServer(server3, regionName, 0, true, 1, null);
    // set queueRedundency=1
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort3}, 0, 59000, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    server3.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));

    client1.invoke(new CacheSerializableRunnable(title + "client2 versions collection") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        region.registerInterest("ALL_KEYS");
        LogWriterUtils.getLogWriter()
            .info("client2 registerInterest ALL_KEYS at " + region.getFullPath());
      }
    });

    server1.invoke(new CacheSerializableRunnable(title + "client1 putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doPutAll(regionName, "key-", numberOfEntries * 2);
        assertEquals(numberOfEntries * 2, region.size());
        doRemoveAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
      }
    });

    expectedRAVersions = (List<String>) server1
        .invoke(new SerializableCallable(title + "server1 versions collection") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries, region.size());
            List<String> versions = new ArrayList<String>(numberOfEntries * 2);

            Set<BucketRegion> buckets =
                ((PartitionedRegion) region).dataStore.getAllLocalPrimaryBucketRegions();

            for (BucketRegion br : buckets) {

              RegionMap entries = br.entries;

              for (Object key : entries.keySet()) {
                RegionEntry internalRegionEntry = entries.getEntry(key);
                VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
                LogWriterUtils.getLogWriter().info("Entry version tag on server1:" + tag);
                versions.add(key + " " + tag);
              }
            }

            return versions;
          }
        });

    // Let client be updated with all keys.
    Wait.pause(1000);

    actualRAVersions = (List<String>) client1
        .invoke(new SerializableCallable(title + "client2 versions collection") {

          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries, region.size());
            List<String> versions = new ArrayList<String>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;
            assertEquals(numberOfEntries * 2, entries.size());

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);
              VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
              tag.setMemberID(null);
              versions.add(key + " " + tag);
            }
            return versions;
          }
        });

    LogWriterUtils.getLogWriter().info(Arrays.toString(expectedRAVersions.toArray()));

    assertEquals(numberOfEntries * 2, actualRAVersions.size());
    LogWriterUtils.getLogWriter().info(Arrays.toString(actualRAVersions.toArray()));

    for (String keyTag : expectedRAVersions) {
      if (!actualRAVersions.contains(keyTag)) {
        fail("client 2 does not have the tag contained in client 1" + keyTag);
      }
    }
  }

  @Test
  public void testVersionsOnReplicasAfterPutAllAndRemoveAll() {
    final String title = "testVersionsOnReplicas";
    client1Versions = null;
    client2Versions = null;

    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    final String regionName = getUniqueName();

    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    // set notifyBySubscription=true to test register interest
    final int serverPort1 = createBridgeServer(server1, regionName, 0, false, 0, null);
    final int serverPort2 = createBridgeServer(server2, regionName, 0, false, 0, null);
    // set queueRedundency=1
    createBridgeClient(client1, regionName, serverHost, new int[] {serverPort1}, 0, 59000, true);

    server1.invoke(addExceptionTag1(expectedExceptions));
    server2.invoke(addExceptionTag1(expectedExceptions));
    client1.invoke(addExceptionTag1(expectedExceptions));

    client1.invoke(new CacheSerializableRunnable(title + "client1 putAll") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        doPutAll(regionName, "key-", numberOfEntries * 2);
        assertEquals(numberOfEntries * 2, region.size());
        doRemoveAll(regionName, "key-", numberOfEntries);
        assertEquals(numberOfEntries, region.size());
      }
    });

    client1Versions = (List<VersionTag>) server1
        .invoke(new SerializableCallable(title + "client1 versions collection") {
          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries, region.size());
            List<VersionTag> versions = new ArrayList<VersionTag>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;
            assertEquals(numberOfEntries * 2, entries.size());

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);
              VersionTag tag = internalRegionEntry.getVersionStamp().asVersionTag();
              LogWriterUtils.getLogWriter().info("Entry version tag on client:" + tag);
              versions.add(tag);
            }

            return versions;
          }
        });

    client2Versions = (List<VersionTag>) server2
        .invoke(new SerializableCallable(title + "client2 versions collection") {

          @Override
          public Object call() throws CacheException {
            Region region = getRootRegion().getSubregion(regionName);
            assertEquals(numberOfEntries, region.size());
            List<VersionTag> versions = new ArrayList<VersionTag>(numberOfEntries * 2);

            RegionMap entries = ((LocalRegion) region).entries;
            assertEquals(numberOfEntries * 2, entries.size());

            for (Object key : entries.keySet()) {
              RegionEntry internalRegionEntry = entries.getEntry(key);

              versions.add(internalRegionEntry.getVersionStamp().asVersionTag());
            }
            return versions;
          }
        });

    assertEquals(numberOfEntries * 2, client1Versions.size());
    LogWriterUtils.getLogWriter().info(Arrays.toString(client1Versions.toArray()));

    LogWriterUtils.getLogWriter().info(Arrays.toString(client2Versions.toArray()));

    for (VersionTag tag : client2Versions) {
      tag.setMemberID(null);
      if (!client1Versions.contains(tag)) {
        fail("client 2 have the tag NOT contained in client 1" + tag);
      }
    }
  }

  private int createBridgeServer(VM server, final String regionName, final int serverPort,
      final boolean createPR, final int redundantCopies, final String diskStoreName) {
    return (Integer) server.invoke(new SerializableCallable("Create server") {
      @Override
      @SuppressWarnings("synthetic-access")
      public Object call() throws Exception {
        // Create DS
        Properties config = new Properties();
        config.setProperty(LOCATORS,
            "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        Cache cache = getCache();

        // enable concurrency checks (not for disk now - disk doesn't support versions yet)
        if (diskStoreName == null) {
          factory.setConcurrencyChecksEnabled(true);
        }

        // create diskStore if required
        if (diskStoreName != null) {
          DiskStore ds = cache.findDiskStore(diskStoreName);
          if (ds == null) {
            ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(diskStoreName);
          }
        }

        /*
         * In this test, no cacheLoader should be defined, otherwise, it will create a value for
         * destroyed key factory.setCacheLoader(new CacheServerCacheLoader());
         */
        if (createPR) {
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(redundantCopies);
          paf.setTotalNumBuckets(TOTAL_BUCKETS);
          factory.setPartitionAttributes(paf.create());
          if (diskStoreName != null) {
            factory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
            factory.setDiskStoreName(diskStoreName);
          } else {
            factory.setDataPolicy(DataPolicy.PARTITION);
          }
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          if (diskStoreName != null) {
            factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
            factory.setDiskStoreName(diskStoreName);
          } else {
            factory.setDataPolicy(DataPolicy.REPLICATE);
          }
        }
        createRootRegion(new AttributesFactory().create());
        Region region = createRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        } else {
          assertTrue(region instanceof DistributedRegion);
        }
        int retPort = startBridgeServer(serverPort);
        LogWriterUtils.getLogWriter().info("Cache Server Started:" + retPort + ":" + serverPort);
        return retPort;
      }
    });
  }

  private void createClient(VM client, String regionName, String serverHost, int[] serverPorts,
      int redundancy, int readTimeOut, boolean receiveInvalidates, boolean concurrencyChecks,
      boolean enableSingleHop) {
    createClient(client, regionName, serverHost, serverPorts, redundancy, readTimeOut,
        receiveInvalidates, concurrencyChecks, enableSingleHop, true /* subscriptionEnabled */);
  }

  private void createClient(VM client, final String regionName, final String serverHost,
      final int[] serverPorts, final int redundency, final int readTimeOut,
      final boolean receiveInvalidates, final boolean concurrencyChecks,
      final boolean enableSingleHop, final boolean subscriptionEnabled) {
    client.invoke(new CacheSerializableRunnable("Create client") {
      @Override
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty(MCAST_PORT, "0");
        config.setProperty(LOCATORS, "");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        if (concurrencyChecks) {
          factory.setConcurrencyChecksEnabled(true);
        }

        try {
          getCache();
          IgnoredException
              .addIgnoredException("java.net.ConnectException||java.net.SocketException");
          if (readTimeOut > 0) {
            PoolFactory pf = PoolManager.createFactory();
            for (int i = 0; i < serverPorts.length; i++) {
              pf.addServer(serverHost, serverPorts[i]);
            }

            pf.setReadTimeout(readTimeOut).setSubscriptionEnabled(subscriptionEnabled)
                .setPRSingleHopEnabled(enableSingleHop).create("myPool");
            factory.setPoolName("myPool");
          } else {
            ClientServerTestCase.configureConnectionPool(factory, serverHost, serverPorts, true,
                redundency, -1, null);
          }
          Region r = createRegion(regionName, factory.create());
          if (receiveInvalidates) {
            r.registerInterestRegex(".*", false, false);
          }
          assertNotNull(getRootRegion().getSubregion(regionName));
        } catch (CacheException ex) {
          Assert.fail("While creating Region on Edge", ex);
        }
      }
    });
  }

  private void createBridgeClient(VM client, final String regionName, final String serverHost,
      final int[] serverPorts, final int redundency, final int readTimeOut,
      boolean enableSingleHop) {
    createClient(client, regionName, serverHost, serverPorts, redundency, readTimeOut, true, true,
        enableSingleHop);
  }

  protected Region doPutAll(String regionName, String keyStub, int numEntries) {
    Region region = getRootRegion().getSubregion(regionName);
    LinkedHashMap map = new LinkedHashMap();
    for (int i = 0; i < numEntries; i++) {
      map.put(keyStub + i, new TestObject(i));
    }
    region.putAll(map, "putAllCallback");
    return region;
  }

  protected VersionedObjectList doRemoveAll(String regionName, String keyStub, int numEntries) {
    Region region = getRootRegion().getSubregion(regionName);
    ArrayList<String> keys = new ArrayList<String>();
    for (int i = 0; i < numEntries; i++) {
      keys.add(keyStub + i);
    }
    // region.removeAll(keys, "removeAllCallback");
    LocalRegion lr = (LocalRegion) region;
    final EntryEventImpl event = EntryEventImpl.create(lr, Operation.REMOVEALL_DESTROY, null, null,
        "removeAllCallback", false, lr.getMyId());
    event.disallowOffHeapValues();
    DistributedRemoveAllOperation removeAllOp =
        new DistributedRemoveAllOperation(event, keys.size(), false);
    VersionedObjectList versions = lr.basicRemoveAll((Collection) keys, removeAllOp, null);
    return versions;
  }

  public static void waitTillNotify(Object lock_object, int waitTime, boolean ready) {
    synchronized (lock_object) {
      if (!ready) {
        try {
          long then = System.currentTimeMillis();
          lock_object.wait(waitTime);
          long now = System.currentTimeMillis();
          if (now - then > waitTime) {
            fail("Did not receive expected events");
          }
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
      }
    }
  }

  /**
   * Stops the cache server specified by port
   */
  public void stopOneBridgeServer(int port) {
    CacheServer bridge = null;
    boolean foundServer = false;
    for (Iterator bsI = getCache().getCacheServers().iterator(); bsI.hasNext();) {
      bridge = (CacheServer) bsI.next();
      if (bridge.getPort() == port) {
        bridge.stop();
        assertFalse(bridge.isRunning());
        foundServer = true;
        break;
      }
    }
    assertTrue(foundServer);
  }

  protected void closeCache(VM vm0) {
    SerializableRunnable close = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };

    vm0.invoke(close);
  }

  protected void closeCacheAsync(VM vm0) {
    SerializableRunnable close = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };

    vm0.invokeAsync(close);
  }

  protected int getRegionSize(VM vm, final String regionName) {
    SerializableCallable getRegionSize = new SerializableCallable("get region size") {

      @Override
      public Object call() throws Exception {
        Region region = getRootRegion().getSubregion(regionName);
        return new Integer(region.size());
      }
    };

    return (Integer) (vm.invoke(getRegionSize));
  }

  public static class TestObject implements DataSerializable {

    protected String _ticker;

    protected int _price;

    protected long _ts = System.currentTimeMillis();

    public TestObject() {}

    public TestObject(int price) {
      this._price = price;
      this._ticker = Integer.toString(price);
    }

    public TestObject(String ticker, int price) {
      this._ticker = ticker;
      this._price = price;
    }

    public String getTicker() {
      return this._ticker;
    }

    public int getPrice() {
      return this._price;
    }

    public long getTS() {
      return this._ts;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      // System.out.println("Is serializing in WAN: " +
      // GatewayEventImpl.isSerializingValue());
      DataSerializer.writeString(this._ticker, out);
      out.writeInt(this._price);
      out.writeLong(this._ts);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      // System.out.println("Is deserializing in WAN: " +
      // GatewayEventImpl.isDeserializingValue());
      this._ticker = DataSerializer.readString(in);
      this._price = in.readInt();
      this._ts = in.readLong();
    }

    public boolean equals(TestObject o) {
      if (this._price == o._price && this._ticker.equals(o._ticker))
        return true;
      else
        return false;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("Price=" + this._price);
      return sb.toString();
    }
  }

  static class EOCQEventListener implements CqListener {

    Region localregion;

    public int num_creates;

    public int num_updates;

    public EOCQEventListener(Region region) {
      localregion = region;
    }

    @Override
    public void onError(CqEvent cqEvent) {}

    @Override
    public void onEvent(CqEvent cqEvent) {
      if (cqEvent.getQueryOperation() == Operation.DESTROY)
        return;
      Object key = cqEvent.getKey();
      Object newValue = cqEvent.getNewValue();
      if (newValue == null) {
        localregion.create(key, newValue);
        num_creates++;
      } else {
        localregion.put(key, newValue);
        num_updates++;
      }
      LogWriterUtils.getLogWriter().info("CQListener:TestObject:" + key + ":" + newValue);
    }

    @Override
    public void close() {}
  }

  static class SharedCounter implements Serializable {

    public String owner;
    public int num_create_event;
    public int num_update_event;
    public int num_invalidate_event;
    public int num_destroy_event;

    public SharedCounter(String owner) {
      this.owner = owner;
      num_create_event = 0;
      num_update_event = 0;
      num_invalidate_event = 0;
      num_destroy_event = 0;
    }

    @Override
    public String toString() {
      String str = "Owner=" + owner + ",create=" + num_create_event + ",update=" + num_update_event
          + ",invalidate=" + num_invalidate_event + ",destroy=" + num_destroy_event;
      return str;
    }
  }

  class MyListener extends CacheListenerAdapter implements Declarable {

    boolean delay = false;
    public int num_testEndPointSwitch;
    public int num_testHADRFailOver;
    public int num_oldValueInAfterUpdate;
    public SharedCounter sc;
    public VM vm;
    public int closeCacheAtItem = -1;

    public MyListener(boolean delay) {
      this.delay = delay;
      this.sc = new SharedCounter("dummy");
    }

    public MyListener(boolean delay, SharedCounter sc) {
      this.delay = delay;
      this.sc = sc;
    }

    public MyListener(VM vm, boolean delay, SharedCounter sc, int closeCacheAtItem) {
      this.vm = vm;
      this.delay = delay;
      this.sc = sc;
      this.closeCacheAtItem = closeCacheAtItem;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public void afterCreate(EntryEvent event) {
      sc.num_create_event++;
      if (closeCacheAtItem != -1 && sc.num_create_event >= closeCacheAtItem) {
        closeCacheAsync(vm);
      }
      LogWriterUtils.getLogWriter()
          .fine("MyListener:afterCreate " + event.getKey() + ":" + event.getNewValue()
              + ":num_create_event=" + sc.num_create_event + ":eventID="
              + ((EntryEventImpl) event).getEventId());
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllCallback", event.getCallbackArgument());
      }
      if (delay) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          // this can happen in this test because there are asynchronous
          // operations being performed during shutdown
          Thread.currentThread().interrupt();
        }
        return;
      }
      if (event.getKey().toString().startsWith("testEndPointSwitch")) {
        num_testEndPointSwitch++;
        if (num_testEndPointSwitch == testEndPointSwitchNumber) {
          LogWriterUtils.getLogWriter().info("testEndPointSwitch received expected events");
          synchronized (lockObject3) {
            lockObject3.notify();
          }
        }
      }
      if (event.getKey().toString().startsWith("testHADRFailOver")) {
        num_testHADRFailOver++;
        if (num_testHADRFailOver == thousandEntries * 2) {
          LogWriterUtils.getLogWriter().info("testHADRFailOver received expected events");
          synchronized (lockObject4) {
            lockObject4.notify();
          }
        }
      }
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      sc.num_update_event++;
      LogWriterUtils.getLogWriter()
          .fine("MyListener:afterUpdate " + event.getKey() + ":" + event.getNewValue() + ":"
              + event.getOldValue() + ":num_update_event=" + sc.num_update_event + ":eventID="
              + ((EntryEventImpl) event).getEventId());
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllCallback", event.getCallbackArgument());
      }
      if (delay) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          // this can happen in this test because there are asynchronous
          // operations being performed during shutdown
          Thread.currentThread().interrupt();
        }
        return;
      }
      if (event.getKey().toString().contains("OldValue")) {
        if (event.getOldValue() != null) {
          num_oldValueInAfterUpdate++;
          if (num_oldValueInAfterUpdate == numberOfEntries) {
            LogWriterUtils.getLogWriter().info("received expected OldValue events");
            synchronized (lockObject) {
              lockObject.notify();
            }
          }
        }
      }
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      sc.num_invalidate_event++;
      LogWriterUtils.getLogWriter().info("local invalidate is triggered for " + event.getKey()
          + ":num_invalidate_event=" + sc.num_invalidate_event);
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      sc.num_destroy_event++;
      if (event.getOperation().isRemoveAll()) {
        assertEquals("removeAllCallback", event.getCallbackArgument());
      }
      LogWriterUtils.getLogWriter().info("local destroy is triggered for " + event.getKey()
          + ":num_destroy_event=" + sc.num_destroy_event);
    }
  }

  /**
   * we need cacheWriter for to slow down P2P operations, listener only works for c/s in this case
   */
  static class MyWriter extends CacheWriterAdapter implements Declarable {

    int exceptionAtItem = -1;
    public int num_created;
    public int num_destroyed;
    public String keystub = null;

    public MyWriter(int exceptionAtItem) {
      this.exceptionAtItem = exceptionAtItem;
    }

    public MyWriter(String keystub) {
      this.keystub = keystub;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public synchronized void beforeCreate(EntryEvent event) {
      if (exceptionAtItem != -1 && num_created >= exceptionAtItem) {
        throw new CacheWriterException(
            "Triggered exception as planned, created " + num_created + " keys.");
      }
      LogWriterUtils.getLogWriter().info("MyWriter:beforeCreate " + event.getKey() + ":"
          + event.getNewValue() + "num_created=" + num_created);
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllCallback", event.getCallbackArgument());
      }
      try {
        if (keystub == null) {
          Thread.sleep(50);
        }
      } catch (InterruptedException e) {
        // this can happen in this test because there are asynchronous
        // operations being performed during shutdown
        Thread.currentThread().interrupt();
      }
      num_created++;
    }

    @Override
    public void beforeUpdate(EntryEvent event) {
      LogWriterUtils.getLogWriter()
          .info("MyWriter:beforeUpdate " + event.getKey() + ":" + event.getNewValue());
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllCallback", event.getCallbackArgument());
      }
      try {
        if (keystub == null) {
          Thread.sleep(50);
        }
      } catch (InterruptedException e) {
        // this can happen in this test because there are asynchronous
        // operations being performed during shutdown
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void beforeDestroy(EntryEvent event) {
      if (exceptionAtItem != -1 && num_destroyed >= exceptionAtItem) {
        throw new CacheWriterException(
            "Triggered exception as planned, destroyed " + num_destroyed + " keys.");
      }
      LogWriterUtils.getLogWriter().info(
          "MyWriter:beforeDestroy " + event.getKey() + ":" + "num_destroyed=" + num_destroyed);
      if (event.getOperation().isRemoveAll()) {
        assertEquals("removeAllCallback", event.getCallbackArgument());
      }
      try {
        if (keystub == null) {
          Thread.sleep(50);
        }
      } catch (InterruptedException e) {
        // this can happen in this test because there are asynchronous
        // operations being performed during shutdown
        Thread.currentThread().interrupt();
      }
      num_destroyed++;
    }

  }
}
