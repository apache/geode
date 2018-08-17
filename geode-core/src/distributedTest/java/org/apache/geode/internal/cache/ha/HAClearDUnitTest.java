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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheObserverAdapter;
import org.apache.geode.internal.cache.CacheObserverHolder;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * This is the Dunit test to verify clear and destroyRegion operation in Client-Server
 * configuration.
 */
@Category({ClientServerTest.class})
public class HAClearDUnitTest extends JUnit4DistributedTestCase {

  static VM server1 = null;

  static VM server2 = null;

  static VM client1 = null;

  static VM client2 = null;

  private static final String REGION_NAME = "HAClearDUnitTest_Region";

  protected static Cache cache = null;

  static CacheServerImpl server = null;

  static final int NO_OF_PUTS = 100;

  public static final Object dummyObj = "dummyObject";

  static boolean waitFlag = true;

  static int put_counter = 0;

  static boolean gotClearCallback = false;

  static boolean gotDestroyRegionCallback = false;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);

    server1 = host.getVM(0);
    server1.invoke(() -> ConflationDUnitTestHelper.unsetIsSlowStart());

    server2 = host.getVM(1);
    server2.invoke(() -> ConflationDUnitTestHelper.unsetIsSlowStart());

    client1 = host.getVM(2);

    client2 = host.getVM(3);

    CacheObserverHolder.setInstance(new CacheObserverAdapter());
  }

  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(() -> HAClearDUnitTest.closeCache());
    client2.invoke(() -> HAClearDUnitTest.closeCache());
    server1.invoke(() -> HAClearDUnitTest.closeCache());
    server2.invoke(() -> HAClearDUnitTest.closeCache());
    closeCache();
  }

  /**
   * The test perorms following operations 1. Create 2 servers and 3 client 2. Perform put
   * operations for knows set of keys directy from the client1. 3. Perform clear operation from
   * client1 4. verify the result of operation for other clients and other servers.
   */
  @Test
  public void testClearWithOperationFromClient() throws Exception {
    createClientServerConfigurationForClearTest();
    gotClearCallback = false;
    putKnownKeys();
    Thread.sleep(5000);
    int regionSize = NO_OF_PUTS;
    server1.invoke(checkSizeRegion(regionSize));
    server2.invoke(checkSizeRegion(regionSize));
    client1.invoke(checkSizeRegion(regionSize));
    client2.invoke(checkSizeRegion(regionSize));
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    assertEquals(region.size(), regionSize);
    clearRegion();

    client1.invoke(new CacheSerializableRunnable("waitForClearToCompleteFromClient1") {

      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotClearCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("Interrupted");
            }
          }
        }

        if (!gotClearCallback)
          fail("test failed");
        gotClearCallback = false;
      }
    });

    client2.invoke(new CacheSerializableRunnable("waitForClearToCompleteFromClient2") {

      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotClearCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (!gotClearCallback)
          fail("test failed");
        gotClearCallback = false;
      }
    });

    regionSize = 0;
    client1.invoke(checkSizeRegion(regionSize));
    client2.invoke(checkSizeRegion(regionSize));
    server1.invoke(checkSizeRegion(regionSize));
    server2.invoke(checkSizeRegion(regionSize));
  }

  /**
   * The test performs following operations 1. Create 2 servers and 3 clients 2. Perform put
   * operations for known set of keys directy from the server1. 3. Perform clear operation from
   * server1 4. verify the result of operation for other clients and other servers.
   */
  @Test
  public void testClearWithOperationFromServer() throws Exception {
    createClientServerConfigurationForClearTest();
    gotClearCallback = false;
    server1.invoke(putFromServer());
    int regionSize = NO_OF_PUTS;
    Thread.sleep(5000);
    server2.invoke(checkSizeRegion(regionSize));
    client1.invoke(checkSizeRegion(regionSize));
    client2.invoke(checkSizeRegion(regionSize));
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    assertEquals(region.size(), NO_OF_PUTS);
    server1.invoke(clearRegionFromServer());

    client1.invoke(new CacheSerializableRunnable("waitForClearToCompleteCleint1") {
      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotClearCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (!gotClearCallback)
          fail("test failed");
        gotClearCallback = false;
      }
    });

    client2.invoke(new CacheSerializableRunnable("waitForClearToCompleteClient2") {

      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotClearCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (!gotClearCallback)
          fail("test failed");
        gotClearCallback = false;
      }
    });

    regionSize = 0;
    region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    synchronized (HAClearDUnitTest.class) {
      while (!gotClearCallback) {
        try {
          HAClearDUnitTest.class.wait();
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    }

    if (!gotClearCallback) {
      fail("test failed");
    }
    gotClearCallback = false;

    assertEquals(region.size(), regionSize);
    server1.invoke(checkSizeRegion(regionSize));
    server2.invoke(checkSizeRegion(regionSize));
    client1.invoke(checkSizeRegion(regionSize));
    client2.invoke(checkSizeRegion(regionSize));
  }


  /**
   * The test performs following operations 1. Create 2 servers and 3 client 2. Perform put
   * operations for knows set of keys directy from the client1. 3. Perform destroyRegion operation
   * from client1 4. verify the result of operation for other clients and other servers.
   */
  @Test
  public void testDestroyRegionWithOperationFromClient() throws Exception {
    createClientServerConfigurationForClearTest();
    gotDestroyRegionCallback = false;
    putKnownKeys();
    Thread.sleep(5000);
    int regionSize = NO_OF_PUTS;
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    assertEquals(region.size(), regionSize);
    server1.invoke(checkSizeRegion(regionSize));
    server2.invoke(checkSizeRegion(regionSize));
    client1.invoke(checkSizeRegion(regionSize));
    client2.invoke(checkSizeRegion(regionSize));
    destroyRegion();
    Thread.sleep(1000);

    client1.invoke(new CacheSerializableRunnable("waitForDestroyRegionToCompleteClient1") {

      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotDestroyRegionCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (!gotDestroyRegionCallback)
          fail("test failed");
        gotDestroyRegionCallback = false;
      }
    });

    client2.invoke(new CacheSerializableRunnable("waitForDestroyRegionToCompleteClient2") {

      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotDestroyRegionCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (!gotDestroyRegionCallback)
          fail("test failed");
        gotDestroyRegionCallback = false;
      }
    });

    region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNull(region);
    client1.invoke(checkDestroyRegion());
    client2.invoke(checkDestroyRegion());
    server1.invoke(checkDestroyRegion());
    server2.invoke(checkDestroyRegion());
  }


  /**
   * The test performs following operations 1. Create 2 servers and 3 clients 2. Perform put
   * operations for known set of keys directy from the server1. 3. Perform destroyRegion operation
   * from server1 4. verify the result of operation for other clients and other servers.
   */
  @Test
  public void testDestroyRegionWithOperationFromServer() throws Exception {
    createClientServerConfigurationForClearTest();
    gotDestroyRegionCallback = false;
    server1.invoke(putFromServer());
    int regionSize = NO_OF_PUTS;
    Thread.sleep(5000);
    server2.invoke(checkSizeRegion(regionSize));
    client1.invoke(checkSizeRegion(regionSize));
    client2.invoke(checkSizeRegion(regionSize));
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    assertEquals(region.size(), NO_OF_PUTS);
    server1.invoke(destroyRegionFromServer());

    client1.invoke(new CacheSerializableRunnable("waitForDestroyRegionToCompleteFromClient1") {
      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotDestroyRegionCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (!gotDestroyRegionCallback)
          fail("test failed");
        gotDestroyRegionCallback = false;
      }
    });

    client2.invoke(new CacheSerializableRunnable("waitForDestroyRegionToCompleteFromClient2") {
      public void run2() throws CacheException {
        synchronized (HAClearDUnitTest.class) {
          while (!gotDestroyRegionCallback) {
            try {
              HAClearDUnitTest.class.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (!gotDestroyRegionCallback)
          fail("test failed");
        gotDestroyRegionCallback = false;
      }
    });

    synchronized (HAClearDUnitTest.class) {
      while (!gotDestroyRegionCallback) {
        try {
          HAClearDUnitTest.class.wait();
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    }

    if (!gotDestroyRegionCallback) {
      fail("test failed");
    }
    gotDestroyRegionCallback = false;

    region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNull(region);
    client1.invoke(checkDestroyRegion());
    client2.invoke(checkDestroyRegion());
    server1.invoke(checkDestroyRegion());
    server2.invoke(checkDestroyRegion());

  }


  private CacheSerializableRunnable putFromServer() {
    CacheSerializableRunnable putFromServer = new CacheSerializableRunnable("putFromServer") {
      public void run2() throws CacheException {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        for (int i = 0; i < NO_OF_PUTS; i++) {
          region.put("key" + i, "value" + i);
        }
      }
    };
    return putFromServer;

  }

  // function to perform put operations for the known set of keys.
  private void putKnownKeys() {
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    for (int i = 0; i < NO_OF_PUTS; i++) {
      region.put("key" + i, "value" + i);
    }
  }

  // function to perform clear operation from client.
  private void clearRegion() {
    LocalRegion region = (LocalRegion) cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.clear();
  }

  // function to perform destroyRegion operation from client.
  private void destroyRegion() {
    LocalRegion region = (LocalRegion) cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.destroyRegion();
  }


  // function to perform clear operation from server.
  private CacheSerializableRunnable clearRegionFromServer() {
    CacheSerializableRunnable clearFromServer = new CacheSerializableRunnable("clearFromServer") {
      public void run2() {
        LocalRegion region = (LocalRegion) cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        region.clear();
      }
    };
    return clearFromServer;
  }

  // function to perform destroyRegion operation from server.
  private CacheSerializableRunnable destroyRegionFromServer() {
    CacheSerializableRunnable clearFromServer =
        new CacheSerializableRunnable("destroyRegionFromServer") {
          public void run2() {
            LocalRegion region = (LocalRegion) cache.getRegion(Region.SEPARATOR + REGION_NAME);
            assertNotNull(region);
            region.destroyRegion();
          }
        };
    return clearFromServer;
  }

  // function to check the size of the region.
  private CacheSerializableRunnable checkSizeRegion(final int size) {

    CacheSerializableRunnable clearRegion = new CacheSerializableRunnable("checkSize") {

      public void run2() throws CacheException {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        LogWriterUtils.getLogWriter().info("Size of the region " + region.size());
        assertEquals(size, region.size());
      }
    };
    return clearRegion;
  }

  // function to check whether region is destroyed.
  private CacheSerializableRunnable checkDestroyRegion() {
    CacheSerializableRunnable destroyRegion = new CacheSerializableRunnable("checkDestroyRegion") {
      public void run2() throws CacheException {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        LogWriterUtils.getLogWriter().warning("Found region " + region);
        assertNull(region);
      }
    };

    return destroyRegion;

  }

  // function to create 2servers and 3 clients
  private void createClientServerConfigurationForClearTest() throws Exception {
    int PORT1 = ((Integer) server1.invoke(() -> HAClearDUnitTest.createServerCache())).intValue();
    int PORT2 = ((Integer) server2.invoke(() -> HAClearDUnitTest.createServerCache())).intValue();
    String hostname = NetworkUtils.getServerHostName(Host.getHost(0));
    client1.invoke(() -> HAClearDUnitTest.createClientCache(hostname, new Integer(PORT1),
        new Integer(PORT2), new Boolean(true), new Boolean(true)));
    client2.invoke(() -> HAClearDUnitTest.createClientCache(hostname, new Integer(PORT1),
        new Integer(PORT2), new Boolean(true), new Boolean(true)));
    createClientCache(hostname, new Integer(PORT1), new Integer(PORT2), new Boolean(true),
        new Boolean(true));
  }

  public static Integer createServerCache() throws Exception {
    new HAClearDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.start();
    return new Integer(server.getPort());
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String hostName, Integer port1, Integer port2,
      Boolean listenerAttached, Boolean registerInterest) throws Exception {
    int PORT1 = port1.intValue();
    int PORT2 = port2.intValue();
    boolean isListenerAttached = listenerAttached.booleanValue();
    boolean isRegisterInterest = registerInterest.booleanValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAClearDUnitTest().createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1, PORT2}, true,
        -1, 2, null);
    if (isListenerAttached) {
      factory.setCacheListener(new CacheListenerAdapter() {
        public void afterRegionClear(RegionEvent event) {
          LogWriterUtils.getLogWriter().info("-------> afterRegionClear received");
          synchronized (HAClearDUnitTest.class) {
            gotClearCallback = true;
            HAClearDUnitTest.class.notifyAll();
          }
        }

        public void afterRegionDestroy(RegionEvent event) {
          synchronized (HAClearDUnitTest.class) {
            LogWriterUtils.getLogWriter().info("-------> afterRegionDestroy received");
            gotDestroyRegionCallback = true;
            HAClearDUnitTest.class.notifyAll();
          }
        }
      });
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    if (isRegisterInterest) {
      region.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    } else {
      region.registerInterestRegex("*.", false, false); // don't want data but do want events
    }
  }

  public static void closeCache() {
    String p1 = "Problem removing all interest on region=";

    if (cache != null && !cache.isClosed()) {
      cache.getDistributedSystem().getLogWriter()
          .info("<ExpectedException action=add>" + p1 + "</ExpectedException>");
      cache.close();
      cache.getDistributedSystem().getLogWriter()
          .info("<ExpectedException action=remove>" + p1 + "</ExpectedException>");
      cache.getDistributedSystem().disconnect();
    }
  }
}
