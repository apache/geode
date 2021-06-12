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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test code copied from UpdatePropagationDUnitTest Tests that registering interest KEYS works
 * correctly.
 */
@Category({ClientSubscriptionTest.class})
public class RegisterInterestKeysDUnitTest extends JUnit4DistributedTestCase {

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  private int PORT1;

  private int PORT2;

  private static final String REGION_NAME = "RegisterInterestKeysDUnitTest_region";

  private static Cache cache = null;

  static RegisterInterestKeysDUnitTest impl;

  /** constructor */
  public RegisterInterestKeysDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    Wait.pause(5000);

    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    // Client 1 VM
    client1 = host.getVM(2);

    // client 2 VM
    client2 = host.getVM(3);

    createImpl();
    for (int i = 0; i < 4; i++) {
      host.getVM(i).invoke(getClass(), "createImpl", null);
    }

    LogWriterUtils.getLogWriter().info("implementation class is " + impl.getClass());

    PORT1 = ((Integer) server1.invoke(() -> impl.createServerCache())).intValue();
    PORT2 = ((Integer) server2.invoke(() -> impl.createServerCache())).intValue();

    client1.invoke(() -> impl.createClientCache(NetworkUtils.getServerHostName(server1.getHost()),
        new Integer(PORT1), new Integer(PORT2)));
    client2.invoke(() -> impl.createClientCache(NetworkUtils.getServerHostName(server1.getHost()),
        new Integer(PORT1), new Integer(PORT2)));
  }

  /** subclass support */
  public static void createImpl() {
    impl = new RegisterInterestKeysDUnitTest();
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }


  /**
   * This tests whether the updates are received by other clients or not , if there are situation of
   * Interest List fail over
   *
   */
  @Test
  public void testRegisterCreatesInvalidEntry() {
    // First create entries on both servers via the two client
    client1.invoke(() -> impl.createEntriesK1());
    client2.invoke(() -> impl.registerKeysK1());
  }


  /**
   * Creates entries on the server
   *
   */
  public static void createEntriesK1() {
    try {
      Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      r1.create("key1", "key-1");
      assertEquals(r1.getEntry("key1").getValue(), "key-1");
    } catch (Exception ex) {
      Assert.fail("failed while createEntriesK1()", ex);
    }
  }

  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception {
    int PORT1 = port1.intValue();
    int PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new RegisterInterestKeysDUnitTest().createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, PORT1).addServer(host, PORT2)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setSocketBufferSize(1000)
          .setMinConnections(4)
          // retryAttempts 2
          // retryInterval 250
          .create("RegisterInterestKeysDUnitTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static Integer createServerCache() throws Exception {
    new RegisterInterestKeysDUnitTest().createCache(new Properties());

    RegionAttributes attrs = impl.createServerCacheAttributes();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = getRandomAvailableTCPPort();
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  protected RegionAttributes createServerCacheAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory.create();
  }


  public static void registerKeysK1() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      assertEquals(false, r.containsKey("key1"));
      List list = new ArrayList();
      list.add("key1");
      r.registerInterest(list, InterestResultPolicy.KEYS);
      assertEquals(true, r.containsKey("key1"));
      assertEquals(false, r.containsValueForKey("key1"));
      {
        Region.Entry re = r.getEntry("key1");
        assertNotNull(re);
        assertNull(re.getValue());
      }


    } catch (Exception ex) {
      Assert.fail("failed while registering interest", ex);
    }
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    // close client
    client1.invoke(() -> impl.closeCache());
    client2.invoke(() -> impl.closeCache());
    // close server
    server1.invoke(() -> impl.closeCache());
    server2.invoke(() -> impl.closeCache());
  }
}
