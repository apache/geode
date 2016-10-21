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
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Bug Test for bug#36457
 * 
 * Region destroy message from server to client results in client calling unregister to server (an
 * unnecessary callback). The unregister encounters an error because the region has been destroyed
 * on the server and hence falsely marks the server dead.
 */
@Category(DistributedTest.class)
public class Bug36457DUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  private VM server1 = null;

  private static VM server2 = null;

  private static VM client1 = null;

  private static VM client2 = null;

  static boolean isFaileoverHappened = false;

  private static final String regionName = "Bug36457DUnitTest_Region";

  /** constructor */
  public Bug36457DUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new Bug36457DUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port1.intValue())
        .addServer(host, port2.intValue()).setSubscriptionEnabled(true).setMinConnections(4)
        // .setRetryInterval(2345671)
        .create("Bug36457DUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(regionName, attrs);
    List listOfKeys = new ArrayList();
    listOfKeys.add("key-1");
    listOfKeys.add("key-2");
    listOfKeys.add("key-3");
    listOfKeys.add("key-4");
    listOfKeys.add("key-5");
    r.registerInterest(listOfKeys);

  }

  public static Integer createServerCache() throws Exception {
    new Bug36457DUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    client1.invoke(() -> Bug36457DUnitTest.closeCache());
    client2.invoke(() -> Bug36457DUnitTest.closeCache());
    // then close the servers
    server1.invoke(() -> Bug36457DUnitTest.closeCache());
    server2.invoke(() -> Bug36457DUnitTest.closeCache());
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Test
  public void testBug36457() {
    Integer port1 = ((Integer) server1.invoke(() -> Bug36457DUnitTest.createServerCache()));
    Integer port2 = ((Integer) server2.invoke(() -> Bug36457DUnitTest.createServerCache()));
    client1.invoke(() -> Bug36457DUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port1, port2));
    client2.invoke(() -> Bug36457DUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port1, port2));
    // set a cllabck so that we come to know that whether a failover is called or not
    // if failover is called means this bug is present.
    client2.invoke(() -> Bug36457DUnitTest.setClientServerObserver());
    client1.invoke(() -> Bug36457DUnitTest.destroyRegion());
    isFaileoverHappened =
        ((Boolean) client2.invoke(() -> Bug36457DUnitTest.isFaileoverHappened())).booleanValue();
    if (isFaileoverHappened) { // if failover is called means this bug is present, else fixed
      fail("Test failed because of unregistration failed due to region  is destroyed on server");
    }
  }

  public static Boolean isFaileoverHappened() {
    return new Boolean(isFaileoverHappened);
  }

  public static void setClientServerObserver() {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void afterPrimaryIdentificationFromBackup(ServerLocation primaryEndpoint) {
        LogWriterUtils.getLogWriter().fine("TEST FAILED HERE YOGI ");
        isFaileoverHappened = true;
      }
    });
  }

  public static void unSetClientServerObserver() {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
  }

  public static void destroyRegion() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.destroyRegion();
    } catch (Exception ex) {
      Assert.fail("failed while destroy region ", ex);
    }
  }

}
