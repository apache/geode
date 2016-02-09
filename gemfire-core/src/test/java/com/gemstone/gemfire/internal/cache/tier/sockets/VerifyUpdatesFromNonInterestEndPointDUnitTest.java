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
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;

/**
 * One Client , two servers.
 * Ensure that client 1 has registered interest list on server 2.
 * Now Client does a put on server1 .
 * The Client should not receive callback of his own put.

 * @author Yogesh Mahajan
 * @author Suyog Bhokare
 *
 */
public class VerifyUpdatesFromNonInterestEndPointDUnitTest extends DistributedTestCase
{
  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  private static int PORT1;
  private static int PORT2;
  private static final String REGION_NAME = "VerifyUpdatesFromNonInterestEndPointDUnitTest_region";

  private static Cache cache = null;

  /** constructor */
  public VerifyUpdatesFromNonInterestEndPointDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    Wait.pause(5000);
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);


    PORT1 =  ((Integer)vm0.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "createServerCache" )).intValue();
    PORT2 =  ((Integer)vm1.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "createServerCache" )).intValue();

    vm2.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1),new Integer(PORT2)});


  }

  private Cache createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }


  public void testVerifyUpdatesFromNonInterestEndPoint()
  {
    vm2.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "createEntries");
    vm1.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "createEntries");
    vm0.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "createEntries");

    vm2.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "registerKey");

    vm2.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "acquireConnectionsAndPut", new Object[] { new Integer(PORT2)});
    Wait.pause(30000);
    vm2.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "verifyPut");
  }


  public static void acquireConnectionsAndPut(Integer port)
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      String poolName = r1.getAttributes().getPoolName();
      assertNotNull(poolName);
      PoolImpl pool = (PoolImpl)PoolManager.find(poolName);
      assertNotNull(pool);
      Connection conn1 = pool.acquireConnection();
      Connection conn2 = pool.acquireConnection();
      ServerRegionProxy srp = new ServerRegionProxy(Region.SEPARATOR + REGION_NAME, pool);
      // put on a connection which is is not interest list ep
      if(conn1.getServer().getPort() == port.intValue() ){
        srp.putOnForTestsOnly(conn1, "key-1", "server-value1", new EventID(new byte[] { 1 },1,1),null);
        srp.putOnForTestsOnly(conn1, "key-2", "server-value2", new EventID(new byte[] { 1 },1,2),null);
      }
      else if(conn2.getServer().getPort() == port.intValue()){
        srp.putOnForTestsOnly(conn2, "key-1", "server-value1", new EventID(new byte[] { 1 },1,1),null);
        srp.putOnForTestsOnly(conn2, "key-2", "server-value2", new EventID(new byte[] { 1 },1,2),null);
      }
    }
    catch (Exception ex) {
      fail("while setting acquireConnections  "+ ex);
    }
  }

  public static void createEntries()
  {
    try {
      LocalRegion r1 = (LocalRegion)cache.getRegion(Region.SEPARATOR + REGION_NAME);
      if (!r1.containsKey("key-1")) {
        r1.create("key-1", "key-1");
      }
      if (!r1.containsKey("key-2")) {
        r1.create("key-2", "key-2");
      }
      assertEquals(r1.getEntry("key-1").getValue(), "key-1");
      assertEquals(r1.getEntry("key-2").getValue(), "key-2");
    }
    catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception
  {
    VerifyUpdatesFromNonInterestEndPointDUnitTest test = new VerifyUpdatesFromNonInterestEndPointDUnitTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = test.createCache(props);
    Pool p;
    try {
      p = PoolManager.createFactory()
        .addServer(host, port1.intValue())
        .addServer(host, port2.intValue())
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setMinConnections(6)
        .setSocketBufferSize(32768)
        .setReadTimeout(2000)
        // .setRetryInterval(250)
        // .setRetryAttempts(5)
        .create("UpdatePropagationDUnitTestPool");
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
    cache = new VerifyUpdatesFromNonInterestEndPointDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server1 = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }


  public static void registerKey()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.registerInterest("key-1");

    }
    catch (Exception ex) {
      Assert.fail("failed while registerKey()", ex);
    }
  }

  public static void verifyPut()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      // verify no update
      assertEquals("key-1", r.getEntry("key-1").getValue());
      assertEquals("key-2", r.getEntry("key-2").getValue());
    }
    catch (Exception ex) {
      Assert.fail("failed while verifyPut()", ex);
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
  protected final void preTearDown() throws Exception {
    //close client
    vm2.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "closeCache");
    //close server
    vm0.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "closeCache");
    vm1.invoke(VerifyUpdatesFromNonInterestEndPointDUnitTest.class, "closeCache");
  }
}
