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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * bug test for bug 36805
 * 
 * 
 * When server is running but region is not created on server. Client sends
 * register interest request, server checks for region, and if region is not
 * exist on server, it throws an exception to the client. Hence, client marks
 * server as dead.
 * 
 * To avoid this, there should not be any check of region before registration.
 * And region registration should not fail due to non existent region.
 * 
 */
public class Bug36805DUnitTest extends DistributedTestCase
{
  private static Cache cache = null;

  private static VM server1 = null;

  private static VM server2 = null;

  private static VM client1 = null;

  private static VM client2 = null;

  protected static PoolImpl pool = null;

  // static boolean isFaileoverHappened = false;

  private static final String regionName = "Bug36805DUnitTest_Region";

  /** constructor */
  public Bug36805DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);

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

  public static void createClientCache(String host, Integer port1, Integer port2)
      throws Exception
  {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new Bug36805DUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory()
      .addServer(host, port1.intValue())
      .addServer(host, port2.intValue())
      .setSubscriptionEnabled(true)
      .setMinConnections(4)
      // .setRetryInterval(2345671)
      .create("Bug36805UnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    pool = p;

  }

  public static Integer createServerCache() throws Exception
  {
    new Bug36805DUnitTest("temp").createCache(new Properties());
   // no region is created on server 
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  @Override
  protected final void preTearDown() throws Exception {
    // close the clients first
    client1.invoke(Bug36805DUnitTest.class, "closeCache");
    client2.invoke(Bug36805DUnitTest.class, "closeCache");
    // then close the servers
    server1.invoke(Bug36805DUnitTest.class, "closeCache");
    server2.invoke(Bug36805DUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public void testBug36805()
  {
    Integer port1 = ((Integer)server1.invoke(Bug36805DUnitTest.class,
        "createServerCache"));
    Integer port2 = ((Integer)server2.invoke(Bug36805DUnitTest.class,
        "createServerCache"));
    client1.invoke(Bug36805DUnitTest.class, "createClientCache", new Object[] {
        NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    client2.invoke(Bug36805DUnitTest.class, "createClientCache", new Object[] {
        NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    // set a cllabck so that we come to know that whether a failover is called
    // or not
    // if failover is called means this bug is present.
    // client2.invoke(Bug36805DUnitTest.class, "setClientServerObserver");
    client1.invoke(Bug36805DUnitTest.class, "registerInterest"); // register
                                                                  // interest
                                                                  // shoud not
                                                                  // cause any
                                                                  // failure

    client2.invoke(Bug36805DUnitTest.class, "verifyDeadAndLiveServers",
        new Object[] { new Integer(0), new Integer(2) });
    client1.invoke(Bug36805DUnitTest.class, "verifyDeadAndLiveServers",
        new Object[] { new Integer(0), new Integer(2) });

  }

  
  public static void registerInterest()
  {
    cache.getLogger().info(
        "<ExpectedException action=add>" + "RegionDestroyedException"
        + "</ExpectedException>");
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      List listOfKeys = new ArrayList();
      listOfKeys.add("key-1");
      listOfKeys.add("key-2");
      listOfKeys.add("key-3");
      listOfKeys.add("key-4");
      listOfKeys.add("key-5");
      r.registerInterest(listOfKeys);
      fail("expected RegionDestroyedException");
    } catch (ServerOperationException expected) {
      assertEquals(RegionDestroyedException.class, expected.getCause().getClass());
    }
    finally {
      cache.getLogger().info(
          "<ExpectedException action=remove>" + "RegionDestroyedException"
          + "</ExpectedException>");
    }
  }

  public static void verifyDeadAndLiveServers(final Integer expectedDeadServers,
      final Integer expectedLiveServers)
  {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        return pool.getConnectedServerCount() == expectedLiveServers.intValue();
      }
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);

    // we no longer verify dead servers; live is good enough
//     start = System.currentTimeMillis();
//     while (proxy.getDeadServers().size() != expectedDeadServers.intValue()) { // wait
//                                                                               // until
//                                                                               // condition
//                                                                               // is
//                                                                               // met
//       assertTrue(
//           "Waited over "
//               + maxWaitTime
//               + " for dead servers to become: "
//               + expectedDeadServers.intValue()
//               + " but it is: " + proxy.getDeadServers().size()
//               + ". This issue can occur on Solaris as DSM thread get stuck in connectForServer() call, and hence not recovering any newly started server This may be beacuase of tcp_ip_abort_cinterval kernal level property on solaris which has 3 minutes as a default value",
//           (System.currentTimeMillis() - start) < maxWaitTime);
//       try {
//         Thread.yield();
//         synchronized (delayLock) {
//           delayLock.wait(2000);
//         }
//       }
//       catch (InterruptedException ie) {
//         fail("Interrupted while waiting ", ie);
//       }
//     }
  }
}
