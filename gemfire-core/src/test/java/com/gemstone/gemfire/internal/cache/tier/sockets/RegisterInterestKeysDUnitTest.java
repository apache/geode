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

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.cache.client.*;

/**
 * Test code copied from UpdatePropagationDUnitTest
 * Tests that registering interest KEYS works correctly.
 *
 * @author Darrel
 *
 */

public class RegisterInterestKeysDUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  private static  int PORT1 ;

  private static  int PORT2 ;

  private static final String REGION_NAME = "RegisterInterestKeysDUnitTest_region";

  private static Cache cache = null;

  static RegisterInterestKeysDUnitTest impl;

  /** constructor */
  public RegisterInterestKeysDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    pause(5000);

    final Host host = Host.getHost(0);
    //Server1 VM
    server1 = host.getVM(0);

    //Server2 VM
    server2 = host.getVM(1);

    //Client 1 VM
    client1 = host.getVM(2);

    //client 2 VM
    client2 = host.getVM(3);

    createImpl();
    for (int i=0; i<4; i++) {
      host.getVM(i).invoke(getClass(), "createImpl", null);
    }

    getLogWriter().info("implementation class is " + impl.getClass());

    PORT1 =  ((Integer)server1.invoke(impl.getClass(), "createServerCache" )).intValue();
    PORT2 =  ((Integer)server2.invoke(impl.getClass(), "createServerCache" )).intValue();
    
    client1.invoke(impl.getClass(), "createClientCache", new Object[] { 
      getServerHostName(server1.getHost()), new Integer(PORT1),new Integer(PORT2)});
    client2.invoke(impl.getClass(), "createClientCache", new Object[] {
      getServerHostName(server1.getHost()), new Integer(PORT1),new Integer(PORT2)});

  }

  /** subclass support */
  public static void createImpl() {
    impl = new RegisterInterestKeysDUnitTest("temp");
  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }


  /**
   * This tests whether the updates are received by other clients or not , if there are
   * situation of Interest List fail over
   *
   */
  public void testRegisterCreatesInvalidEntry()
  {
    //  First create entries on both servers via the two client
    client1.invoke(impl.getClass(), "createEntriesK1");
    client2.invoke(impl.getClass(), "registerKeysK1");
  }


  /**
   * Creates entries on the server
   *
   */
  public static void createEntriesK1()
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR+REGION_NAME);
      assertNotNull(r1);
      r1.create("key1", "key-1");
      assertEquals(r1.getEntry("key1").getValue(), "key-1");
    }
    catch (Exception ex) {
      fail("failed while createEntriesK1()", ex);
    }
  }

  public static void createClientCache(String host, Integer port1 , Integer port2 ) throws Exception
  {
    PORT1 = port1.intValue() ;
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new RegisterInterestKeysDUnitTest("temp").createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory()
        .addServer(host, PORT1)
        .addServer(host, PORT2)
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setReadTimeout(2000)
        .setSocketBufferSize(1000)
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

  public static Integer createServerCache() throws Exception
  {
    new RegisterInterestKeysDUnitTest("temp").createCache(new Properties());
   
    RegionAttributes attrs = impl.createServerCacheAttributes();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  protected RegionAttributes createServerCacheAttributes()
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory.create();
  }


  /**
   *
   */
  public static void registerKeysK1()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
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


    }
    catch (Exception ex) {
      fail("failed while registering interest", ex);
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
    //close client
    client1.invoke(impl.getClass(), "closeCache");
    client2.invoke(impl.getClass(), "closeCache");
    //close server
    server1.invoke(impl.getClass(), "closeCache");
    server2.invoke(impl.getClass(), "closeCache");

  }

}
