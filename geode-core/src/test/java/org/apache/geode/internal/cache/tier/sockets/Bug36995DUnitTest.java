/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;

import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug36995DUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  private static VM server1 = null;

  private static VM server2 = null;

  private static VM server3 = null;

  protected static PoolImpl pool = null;

  private static final String regionName = Bug36995DUnitTest.class.getSimpleName() + "_Region";

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, int port1, int port2, int port3)
  {
    try {
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");
      new Bug36995DUnitTest().createCache(props);
      PoolImpl p = (PoolImpl)PoolManager.createFactory()
        .addServer(host, port1)
        .addServer(host, port2)
        .addServer(host, port3)
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setSubscriptionMessageTrackingTimeout(54321)
        .setIdleTimeout(-1)
        .setPingInterval(200)
        .create("Bug36995UnitTestPool1");
      AttributesFactory factory = new AttributesFactory();
      factory.setPoolName(p.getName());
      RegionAttributes attrs = factory.create();
      cache.createRegion(regionName, attrs);
      pool = p;
    }
    catch (Exception e) {
      fail("Test failed due to ", e);
    }
  }

  public static void createClientCacheWithDefaultMessageTrackingTimeout(
      String host, int port1, int port2, int port3)
  {
    try {
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");
      new Bug36995DUnitTest().createCache(props);
      PoolImpl p = (PoolImpl)PoolManager.createFactory()
        .addServer(host, port1)
        .addServer(host, port2)
        .addServer(host, port3)
        .create("Bug36995UnitTestPool2");
      AttributesFactory factory = new AttributesFactory();
      factory.setPoolName(p.getName());
      RegionAttributes attrs = factory.create();
      cache.createRegion(regionName, attrs);
      pool = p;
    }
    catch (Exception e) {
      fail("Test failed due to ", e);
    }
  }

  public static Integer createServerCache() throws Exception
  {
    new Bug36995DUnitTest().createCache(new Properties());
    // no region is created on server 
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.start();
    return new Integer(server1.getPort());
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    server1.invoke(() -> Bug36995DUnitTest.closeCache());
    server2.invoke(() -> Bug36995DUnitTest.closeCache());
    server3.invoke(() -> Bug36995DUnitTest.closeCache());
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * Tests messageTrackingTimeout is set correctly to default or not if not specified
   */
  @Test
  public void testBug36995_Default()
  {
    Integer port1 = ((Integer)server1.invoke(() -> Bug36995DUnitTest.createServerCache()));
    Integer port2 = ((Integer)server2.invoke(() -> Bug36995DUnitTest.createServerCache()));
    Integer port3 = ((Integer)server3.invoke(() -> Bug36995DUnitTest.createServerCache()));
    createClientCacheWithDefaultMessageTrackingTimeout(
        NetworkUtils.getServerHostName(server1.getHost()), port1.intValue(), port2
        .intValue(), port3.intValue());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT,
                 pool.getSubscriptionMessageTrackingTimeout());
  }

  /**
   * Tests messageTrackingTimeout is set correctly as pwr user specified
   */
  @Test
  public void testBug36995_UserSpecified()
  {
    //work around GEODE-507
    IgnoredException.addIgnoredException("Connection reset");
    Integer port1 = ((Integer)server1.invoke(() -> Bug36995DUnitTest.createServerCache()));
    Integer port2 = ((Integer)server2.invoke(() -> Bug36995DUnitTest.createServerCache()));
    Integer port3 = ((Integer)server3.invoke(() -> Bug36995DUnitTest.createServerCache()));
    createClientCache(NetworkUtils.getServerHostName(server1.getHost()),
        port1.intValue(), port2.intValue(), port3.intValue());
    assertEquals(54321, pool.getSubscriptionMessageTrackingTimeout());
  }

  /**
   * BugTest for 36526 : 
   */
  @Test
  public void testBug36526()
  {
    Integer port1 = ((Integer)server1.invoke(() -> Bug36995DUnitTest.createServerCache()));
    Integer port2 = ((Integer)server2.invoke(() -> Bug36995DUnitTest.createServerCache()));
    Integer port3 = ((Integer)server3.invoke(() -> Bug36995DUnitTest.createServerCache()));
    createClientCache(NetworkUtils.getServerHostName(server1.getHost()),
        port1.intValue(), port2.intValue(), port3.intValue());
    verifyDeadAndLiveServers(0, 3);
    server2.invoke(() -> Bug36995DUnitTest.stopServer());
    verifyDeadAndLiveServers(1, 2);
  }

  public static void stopServer()
  {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.stop();
      }
    }
    catch (Exception e) {
      fail("failed while stopServer()", e);
    }
  }

  public static void verifyDeadAndLiveServers(final int expectedDeadServers,
      final int expectedLiveServers)
  {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        return pool.getConnectedServerCount() == expectedLiveServers;
      }
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);
  }
}
