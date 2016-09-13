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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * The Region Destroy Operation from Cache Client does not pass the Client side
 * Context object nor does the p2p messaging has provision of sending Context
 * object in the DestroyRegionMessage. This can cause sender to recieve it own
 * region destruction message.
 */
@Category(DistributedTest.class)
public class Bug36269DUnitTest extends JUnit4DistributedTestCase {

  VM server1 = null;

  VM server2 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String REGION_NAME = "Bug36269DUnitTest_region";

  protected static Cache cache = null;

  private static PoolImpl pool = null;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);

    PORT1 = server1.invoke(() -> Bug36269DUnitTest.createServerCache());
    PORT2 = server2.invoke(() -> Bug36269DUnitTest.createServerCache());
  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  /**
   * This tests whether the region destroy are not received by the sender
   */
  @Test
  public void testRegionDestroyNotReceivedBySender() throws Exception {
    createClientCache();
    acquireConnectionsAndDestroyRegion(NetworkUtils.getServerHostName(Host.getHost(0)));
    server1.invoke(() -> Bug36269DUnitTest.verifyRegionDestroy());
    server2.invoke(() -> Bug36269DUnitTest.verifyRegionDestroy());
    Wait.pause(5000);
    verifyNoRegionDestroyOnOriginator();
  }

  public static void acquireConnectionsAndDestroyRegion(String host) {
    try {
      Connection desCon = pool.acquireConnection(new ServerLocation(host, PORT2));
      ServerRegionProxy srp = new ServerRegionProxy(Region.SEPARATOR + REGION_NAME, pool);
      srp.destroyRegionOnForTestsOnly(desCon, new EventID(new byte[] {1}, 1, 1), null);
    }
    catch (Exception ex) {
      Assert.fail("while setting acquireConnections", ex);
    }
  }

  public static void createClientCache() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new Bug36269DUnitTest().createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl p;
    String host = NetworkUtils.getServerHostName(Host.getHost(0));
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
    new Bug36269DUnitTest().createCache(new Properties());
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
      Assert.fail("failed while verifyNoRegionDestroyOnOriginator()", ex);
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
      Wait.waitForCriterion(ev, 40 * 1000, 200, true);
    }
    catch (Exception ex) {
      Assert.fail("failed while verifyRegionDestroy", ex);
    }
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    // close server
    server1.invoke(() -> Bug36269DUnitTest.closeCache());
    server2.invoke(() -> Bug36269DUnitTest.closeCache());
  }
}
