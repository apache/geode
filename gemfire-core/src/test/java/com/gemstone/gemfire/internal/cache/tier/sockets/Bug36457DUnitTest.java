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
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Bug Test for bug#36457
 * 
 * Region destroy message from server to client results in client calling
 * unregister to server (an unnecessary callback). The unregister encounters an
 * error because the region has been destroyed on the server and hence falsely
 * marks the server dead.
 */
public class Bug36457DUnitTest extends DistributedTestCase
{
  private static Cache cache = null;

  private static VM server1 = null;

  private static VM server2 = null;

  private static VM client1 = null;

  private static VM client2 = null;

  static boolean isFaileoverHappened = false;

  private static final String regionName = "Bug36457DUnitTest_Region";

  /** constructor */
  public Bug36457DUnitTest(String name) {
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
    new Bug36457DUnitTest("temp").createCache(props);
    Pool p = PoolManager.createFactory()
      .addServer(host, port1.intValue())
      .addServer(host, port2.intValue())
      .setSubscriptionEnabled(true)
      .setMinConnections(4)
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

  public static Integer createServerCache() throws Exception
  {
    new Bug36457DUnitTest("temp").createCache(new Properties());
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

  public void tearDown2() throws Exception
  {
    super.tearDown2();
    // close the clients first
    client1.invoke(Bug36457DUnitTest.class, "closeCache");
    client2.invoke(Bug36457DUnitTest.class, "closeCache");
    // then close the servers
    server1.invoke(Bug36457DUnitTest.class, "closeCache");
    server2.invoke(Bug36457DUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public void testBug36457()
  {
    Integer port1 = ((Integer)server1.invoke(Bug36457DUnitTest.class,
        "createServerCache"));
    Integer port2 = ((Integer)server2.invoke(Bug36457DUnitTest.class,
        "createServerCache"));
    client1.invoke(Bug36457DUnitTest.class, "createClientCache", new Object[] {
        getServerHostName(server1.getHost()), port1, port2 });
    client2.invoke(Bug36457DUnitTest.class, "createClientCache", new Object[] {
        getServerHostName(server1.getHost()), port1, port2 });
    //set a cllabck so that we come to know that whether a failover is called or not
    // if failover is called means this bug is present.
    client2.invoke(Bug36457DUnitTest.class, "setClientServerObserver");
    client1.invoke(Bug36457DUnitTest.class, "destroyRegion");
    isFaileoverHappened = ((Boolean)client2.invoke(Bug36457DUnitTest.class,
        "isFaileoverHappened")).booleanValue();
    if (isFaileoverHappened) { //if failover is called means this bug is present, else fixed
      fail("Test failed because of unregistration failed due to region  is destroyed on server");
    }
  }

  public static Boolean isFaileoverHappened()
  {
    return new Boolean(isFaileoverHappened);
  }

  public static void setClientServerObserver()
  {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    ClientServerObserverHolder
        .setInstance(new ClientServerObserverAdapter() {
          public void afterPrimaryIdentificationFromBackup(ServerLocation primaryEndpoint)
          {
            getLogWriter().fine("TEST FAILED HERE YOGI ");
            isFaileoverHappened = true;
          }
        });
  }

  public static void unSetClientServerObserver()
  {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
  }

  public static void destroyRegion()
  {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.destroyRegion();
    }
    catch (Exception ex) {
      fail("failed while destroy region ", ex);
    }
  }

}
