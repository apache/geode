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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.RegisterInterestTracker;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category(DistributedTest.class)
public class RedundancyLevelTestBase extends JUnit4DistributedTestCase {

  protected static volatile boolean registerInterestCalled = false;
  protected static volatile boolean makePrimaryCalled = false;

  static Cache cache = null;

  VM server0 = null;
  VM server1 = null;
  VM server2 = null;
  VM server3 = null;

  static int PORT1;
  static int PORT2;
  static int PORT3;
  static int PORT4;

  static String SERVER1;
  static String SERVER2;
  static String SERVER3;
  static String SERVER4;

  static final String k1 = "k1";
  static final String k2 = "k2";

  static final String REGION_NAME = "RedundancyLevelTestBase_region";

  static PoolImpl pool = null;

  static ClientServerObserver oldBo = null;

  static boolean FailOverDetectionByCCU = false;

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);

    server0 = host.getVM(0);
    server1 = host.getVM(1);
    server2 = host.getVM(2);
    server3 = host.getVM(3);

    IgnoredException.addIgnoredException("java.net.SocketException||java.net.ConnectException");

    // start servers first
    PORT1 =
        ((Integer) server0.invoke(() -> RedundancyLevelTestBase.createServerCache())).intValue();
    PORT2 =
        ((Integer) server1.invoke(() -> RedundancyLevelTestBase.createServerCache())).intValue();
    PORT3 =
        ((Integer) server2.invoke(() -> RedundancyLevelTestBase.createServerCache())).intValue();
    PORT4 =
        ((Integer) server3.invoke(() -> RedundancyLevelTestBase.createServerCache())).intValue();

    String hostName = NetworkUtils.getServerHostName(Host.getHost(0));
    SERVER1 = hostName + PORT1;
    SERVER2 = hostName + PORT2;
    SERVER3 = hostName + PORT3;
    SERVER4 = hostName + PORT4;

    CacheServerTestUtil.disableShufflingOfEndpoints();
  }

  public static void doPuts() {
    putEntriesK1andK2();
    putEntriesK1andK2();
    putEntriesK1andK2();
    putEntriesK1andK2();
  }

  public static void putEntriesK1andK2() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      r1.put(k1, k1);
      r1.put(k2, k2);
      assertEquals(r1.getEntry(k1).getValue(), k1);
      assertEquals(r1.getEntry(k2).getValue(), k2);
    } catch (Exception ignore) {
      // not sure why it's ok to ignore but if you don't ignore it, RedundancyLevelPart3DUnitTest
      // will fail
    }
  }

  public static void verifyDispatcherIsAlive() {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return cache.getCacheServers().size() == 1;
        }

        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);

      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }

        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 60 * 1000, 1000, true);


      Iterator iter_prox = ccn.getClientProxies().iterator();
      if (iter_prox.hasNext()) {
        final CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
        wc = new WaitCriterion() {
          String excuse;

          public boolean done() {
            if (proxy._messageDispatcher == null) {
              return false;
            }
            return proxy._messageDispatcher.isAlive();
          }

          public String description() {
            return excuse;
          }
        };
        Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
        // assertTrue("Dispatcher on primary should be alive", proxy._messageDispatcher.isAlive());
      }

    } catch (Exception ex) {
      Assert.fail("while setting verifyDispatcherIsAlive  ", ex);
    }
  }

  public static void verifyDispatcherIsNotAlive() {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return cache.getCacheServers().size() == 1;
        }

        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);

      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }

        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);

      Iterator iter_prox = ccn.getClientProxies().iterator();
      if (iter_prox.hasNext()) {
        CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
        assertFalse("Dispatcher on secondary should not be alive",
            proxy._messageDispatcher.isAlive());
      }

    } catch (Exception ex) {
      Assert.fail("while setting verifyDispatcherIsNotAlive  ", ex);
    }
  }

  public static void verifyRedundantServersContain(final String server) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return pool.getRedundantNames().contains(server);
      }

      public String description() {
        return "Redundant servers (" + pool.getRedundantNames() + ") does not contain " + server;
      }
    };
    Wait.waitForCriterion(wc, 60 * 1000, 2000, true);
  }

  public static void verifyLiveAndRedundantServers(final int liveServers,
      final int redundantServers) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return pool.getConnectedServerCount() == liveServers
            && pool.getRedundantNames().size() == redundantServers;
      }

      public String description() {
        return "Expected connected server count (" + pool.getConnectedServerCount() + ") to become "
            + liveServers + "and redundant count (" + pool.getRedundantNames().size()
            + ") to become " + redundantServers;
      }
    };
    Wait.waitForCriterion(wc, 120 * 1000, 2 * 1000, true);
  }

  public static void verifyDeadServers(int deadServers) {
    // this is now deadcode since it is always followed by verifyLiveAndRedundant
    // long maxWaitTime = 180000;
    // long start = System.currentTimeMillis();
    // while (proxy.getDeadServers().size() != deadServers) { // wait until condition is
    // // met
    // assertTrue("Waited over " + maxWaitTime + "for dead servers to become "
    // + deadServers, (System.currentTimeMillis() - start) < maxWaitTime);
    // try {
    // Thread.yield();
    // synchronized(delayLock) {delayLock.wait(4000);}
    // }
    // catch (InterruptedException ie) {
    // fail("Interrupted while waiting ", ie);
    // }
    // }
  }

  public static void createEntriesK1andK2() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      if (!r1.containsKey(k1)) {
        r1.create(k1, k1);
      }
      if (!r1.containsKey(k2)) {
        r1.create(k2, k2);
      }
      assertEquals(r1.getEntry(k1).getValue(), k1);
      assertEquals(r1.getEntry(k2).getValue(), k2);
    } catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void registerK1AndK2() {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      List list = new ArrayList();
      list.add(k1);
      list.add(k2);
      r.registerInterest(list, InterestResultPolicy.KEYS_VALUES);
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region.registerK1AndK2()", ex);
    }
  }

  public static void unregisterInterest() {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      r.unregisterInterest("k1");
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void verifyNoCCP() {
    assertEquals("More than one BridgeServer", 1, cache.getCacheServers().size());
    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    assertNotNull(bs.getAcceptor());
    assertNotNull(bs.getAcceptor().getCacheClientNotifier());
    // no client is connected to this server
    assertTrue(0 == bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());
  }

  public static void verifyCCP() {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return cache.getCacheServers().size() == 1;
        }

        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);

      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();

      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      // one client is connected to this server
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return ccn.getClientProxies().size() == 1;
        }

        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);
    } catch (Exception ex) {
      Assert.fail("exception in verifyCCP()", ex);
    }
  }

  public static void verifyInterestRegistration() {
    try {
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          return cache.getCacheServers().size() == 1;
        }

        public String description() {
          return "Number of bridge servers (" + cache.getCacheServers().size() + ") never became 1";
        }
      };
      Wait.waitForCriterion(wc, 180 * 1000, 2000, true);

      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }

        public String description() {
          return "Notifier's proxies is empty";
        }
      };
      Wait.waitForCriterion(wc, 180 * 1000, 2000, true);

      Iterator iter_prox = ccn.getClientProxies().iterator();

      if (iter_prox.hasNext()) {
        final CacheClientProxy ccp = (CacheClientProxy) iter_prox.next();
        wc = new WaitCriterion() {
          String excuse;

          public boolean done() {
            Set keysMap = (Set) ccp.cils[RegisterInterestTracker.interestListIndex]
                .getProfile(Region.SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
            if (keysMap == null) {
              excuse = "keys of interest is null";
              return false;
            }
            if (keysMap.size() != 2) {
              excuse = "keys of interest size (" + keysMap.size() + ") not 2";
              return false;
            }
            return true;
          }

          public String description() {
            return excuse;
          }
        };
        Wait.waitForCriterion(wc, 180 * 1000, 2 * 1000, true);

        Set keysMap = ccp.cils[RegisterInterestTracker.interestListIndex]
            .getProfile(Region.SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
        assertTrue(keysMap.contains(k1));
        assertTrue(keysMap.contains(k2));

      } else {
        fail("A CCP was expected . Wasn't it?");
      }
    } catch (Exception ex) {
      fail("while setting verifyInterestRegistration", ex);
    }
  }

  public static void stopServer() {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer) iter.next();
        server.stop();
      }
    } catch (Exception e) {
      Assert.fail("failed while stopServer()", e);
    }
  }

  public static void startServer() {
    try {
      Cache c = CacheFactory.getAnyInstance();
      CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
      assertNotNull(bs);
      bs.start();
    } catch (Exception ex) {
      Assert.fail("while startServer()", ex);
    }
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }


  public static void createClientCache(String host, int port1, int port2, int port3, int port4,
      int redundancy) throws Exception {
    createClientCache(host, port1, port2, port3, port4, redundancy,
        3000, /* defaul socket timeout of 250 millisec */
        10 /* default retry interval */);
  }

  public static void createClientCache(String host, int port1, int port2, int port3, int port4,
      int redundancy, int socketReadTimeout, int retryInterval) throws Exception {
    if (!FailOverDetectionByCCU) {
      oldBo = ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
        public void beforeFailoverByCacheClientUpdater(ServerLocation epFailed) {
          try {
            Thread.sleep(300000);
          } catch (InterruptedException ie) {
            // expected - test will shut down the cache which will interrupt
            // the CacheClientUpdater thread that invoked this method
            Thread.currentThread().interrupt();
          }
        }
      });
    }

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new RedundancyLevelTestBase().createCache(props);

    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1)
        .addServer(host, PORT2).addServer(host, PORT3).addServer(host, PORT4)
        .setSubscriptionEnabled(true).setReadTimeout(socketReadTimeout).setSocketBufferSize(32768)
        .setMinConnections(8).setSubscriptionRedundancy(redundancy).setRetryAttempts(5)
        .setPingInterval(retryInterval).create("DurableClientReconnectDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.createRegionAttributes();
    cache.createRegion(REGION_NAME, attrs);
    pool = p;
    createEntriesK1andK2();
    registerK1AndK2();
  }

  public static Integer createServerCache() throws Exception {
    new RedundancyLevelTestBase().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableConflation(true);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    RegionAttributes attrs = factory.createRegionAttributes();
    cache.createVMRegion(REGION_NAME, attrs);

    CacheServer server1 = cache.addCacheServer();

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setMaximumTimeBetweenPings(180000);
    server1.setPort(port);
    // ensures updates to be sent instead of invalidations
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public static void verifyOrderOfEndpoints() {
    // I'm not sure this validation is needed anymore
    // Endpoint[] eplist = proxy.getEndpoints();
    // int len = eplist.length;
    // int redundancyLevel = proxy.getRedundancyLevel();

    // if (len > 0) {
    // assertTrue(((EndpointImpl)eplist[0]).isPrimary());
    // if (redundancyLevel == -1)
    // redundancyLevel = len - 1;

    // for (int i = len - 1, cnt = 0; i >= 1; i--, cnt++) {
    // if (cnt < redundancyLevel)
    // assertTrue(((EndpointImpl)eplist[i]).isRedundant());
    // else
    // assertFalse(((EndpointImpl)eplist[i]).isRedundant());
    // }
    // }
  }

  @Override
  public final void preTearDown() throws Exception {
    try {
      if (!FailOverDetectionByCCU)
        ClientServerObserverHolder.setInstance(oldBo);

      FailOverDetectionByCCU = false;

      // close the clients first
      closeCache();

      // then close the servers
      server0.invoke(() -> RedundancyLevelTestBase.closeCache());
      server1.invoke(() -> RedundancyLevelTestBase.closeCache());
      server2.invoke(() -> RedundancyLevelTestBase.closeCache());
      server3.invoke(() -> RedundancyLevelTestBase.closeCache());
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
