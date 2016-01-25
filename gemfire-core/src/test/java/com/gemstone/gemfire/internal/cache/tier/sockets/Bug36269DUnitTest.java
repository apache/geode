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

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * The Region Destroy Operation from Cache Client does not pass the Client side
 * Context object nor does the p2p messaging has provision of sending Context
 * object in the DestroyRegionMessage. This can cause sender to recieve it own
 * region destruction message.
 */

public class Bug36269DUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM server2 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String REGION_NAME = "Bug36269DUnitTest_region";

  protected static Cache cache = null;

  private static PoolImpl pool = null;

  /** constructor */
  public Bug36269DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    PORT1 = ((Integer)server1.invoke(Bug36269DUnitTest.class,
        "createServerCache")).intValue();
    PORT2 = ((Integer)server2.invoke(Bug36269DUnitTest.class,
        "createServerCache")).intValue();

  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

//  static private final String WAIT_PROPERTY = "Bug36269DUnitTest.maxWaitTime";
//
//  static private final int WAIT_DEFAULT = 60000;

  /**
   * This tests whether the region destroy are not received by the sender
   * 
   */
  public void testRegionDestroyNotReceivedBySender() throws Exception
  {
    try {
      createClientCache();
      acquireConnectionsAndDestroyRegion(getServerHostName(Host.getHost(0)));
      server1.invoke(Bug36269DUnitTest.class, "verifyRegionDestroy");
      server2.invoke(Bug36269DUnitTest.class, "verifyRegionDestroy");
      pause(5000);
      verifyNoRegionDestroyOnOriginator();
    }
    catch (Exception ex) {
      fail("failed gggg testRegionDestroyNotReceivedBySender  " + ex);
    }

  }

  public static void acquireConnectionsAndDestroyRegion(String host)
  {
    try {
      Connection desCon = pool.acquireConnection(new ServerLocation(host, PORT2));
      ServerRegionProxy srp = new ServerRegionProxy(Region.SEPARATOR + REGION_NAME, pool);
      srp.destroyRegionOnForTestsOnly(desCon, new EventID(new byte[] {1}, 1, 1), null);
    }
    catch (Exception ex) {
      fail("while setting acquireConnections", ex);
    }
  }

  public static void createClientCache() throws Exception
  {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new Bug36269DUnitTest("temp").createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl p;
    String host = getServerHostName(Host.getHost(0));
    try {
      p = (PoolImpl)PoolManager.createFactory()
        .addServer(host, PORT1)
        .addServer(host, PORT2)
        .setSubscriptionEnabled(true)
        .setReadTimeout(2000)
        .setSocketBufferSize(1000)
        .setMinConnections(4)
        // .setRetryAttempts(2)
        // .setRetryInterval(250)
        .create("Bug36269DUnitTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    pool = p;
    assertNotNull(pool);
    cache.createRegion(REGION_NAME, factory.create());

  }

  public static Integer createServerCache() throws Exception
  {
    new Bug36269DUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    cache.createRegion(REGION_NAME, factory.create());
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  public static void verifyNoRegionDestroyOnOriginator()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
    }
    catch (Exception ex) {
      fail("failed while verifyNoRegionDestroyOnOriginator()", ex);
    }
  }

  public static void verifyRegionDestroy()
  {
    try {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return cache.getRegion(Region.SEPARATOR + REGION_NAME) == null;
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 40 * 1000, 200, true);
    }
    catch (Exception ex) {
      fail("failed while verifyRegionDestroy", ex);
    }
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public void tearDown2() throws Exception
  {
    closeCache();
    // close server
    server1.invoke(Bug36269DUnitTest.class, "closeCache");
    server2.invoke(Bug36269DUnitTest.class, "closeCache");

  }

}
