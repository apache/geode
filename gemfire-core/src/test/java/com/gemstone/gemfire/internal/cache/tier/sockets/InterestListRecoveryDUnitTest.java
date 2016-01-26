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

import java.io.EOFException;
import java.net.SocketException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.RegisterInterestTracker;

/**
 *
 * Test Scenario :
 *
 * one client(c1) two servers(s1,s2)
 * s1,s2 ----> available
 * c1: register k1,k2,k3,k4,k5
 * s1 ----> unavailable // fail over should happen to server s2
 * see all keys k1,k2,k3,k4,k5 are registered on s2
 * c1: unregister k1,k2,k3
 * see interest list on s1 contains only s4, s5
 * s2 ----> unavaliable // fail over should to s1 with intrest list s4,s5
 * see only k4 and k5 are registerd on s1
 *
 * @author Yogesh Mahajan
 * @author Suyog Bhokare
 */
public class InterestListRecoveryDUnitTest extends DistributedTestCase
{
  private static Cache cache = null;

  VM server1 = null;

  VM server2 = null;

  protected static PoolImpl pool = null;

  private static int PORT1;
  private static int PORT2;


  private static final String REGION_NAME = "InterestListRecoveryDUnitTest_region";

  /** constructor */
  public InterestListRecoveryDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    disconnectAllFromDS();
    pause(2000);
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    //start servers first
    PORT1 =  ((Integer)server1.invoke(InterestListRecoveryDUnitTest.class, "createServerCache" )).intValue();
    PORT2 =  ((Integer)server2.invoke(InterestListRecoveryDUnitTest.class, "createServerCache" )).intValue();

    getLogWriter().info("server1 port is " + String.valueOf(PORT1));
    getLogWriter().info("server2 port is " + String.valueOf(PORT2));

    createClientCache(getServerHostName(host), new Integer(PORT1), new Integer(PORT2));
  }

  // this test fails because of bug# 35352 , hence commented the bug is Deferred to: Danube
  public void XtestKeyInterestRecoveryWhileServerFailover() throws Exception
  {
    createEntries();
    server1.invoke(InterestListRecoveryDUnitTest.class, "createEntries");
    registerK1toK5();
    setServerUnavailable("localhost"+PORT1);
    pause(20000);
    unregisterK1toK3();
    setServerAvailable("localhost"+PORT1);
    pause(20000);
    setServerUnavailable("localhost"+PORT2);
    pause(20000);
    server1.invoke(InterestListRecoveryDUnitTest.class, "verifyUnregisterK1toK3");

  }

  public void testKeyInterestRecoveryWhileProcessException() throws Exception {
    VM serverFirstRegistered = null;
    VM serverSecondRegistered = null;

    LogWriter logger = system.getLogWriter();
    createEntries();
    server2.invoke(InterestListRecoveryDUnitTest.class, "createEntries");
    server1.invoke(InterestListRecoveryDUnitTest.class, "createEntries");

    registerK1toK5();
    logger.fine("After registerK1toK5");

    // Check which server InterestList is registered. Based on it verify
    // Register/Unregister on respective servers.
    if (isInterestListRegisteredToServer1()) {
      serverFirstRegistered = server1;
      serverSecondRegistered = server2;
      logger.fine("serverFirstRegistered is server1 and serverSecondRegistered is server2");
    } else {
      serverFirstRegistered = server2;
      serverSecondRegistered = server1;
      logger.fine("serverFirstRegistered is server2 and serverSecondRegistered is server1");
    }
    verifyDeadAndLiveServers(0,2);
    serverFirstRegistered.invoke(InterestListRecoveryDUnitTest.class, "verifyRegionToProxyMapForFullRegistration");
    logger.fine("After verifyRegionToProxyMapForFullRegistration on serverFirstRegistered");
    logger.info("<ExpectedException action=add>"
        + SocketException.class.getName() + "</ExpectedException>");
    logger.info("<ExpectedException action=add>"
        + EOFException.class.getName() + "</ExpectedException>");
    killCurrentEndpoint();
    logger.fine("After killCurrentEndpoint1");
    serverSecondRegistered.invoke(InterestListRecoveryDUnitTest.class, "verifyRegionToProxyMapForFullRegistrationRetry");
    logger.fine("After verifyRegionToProxyMapForFullRegistration on serverSecondRegistered");
    unregisterK1toK3();
    serverSecondRegistered.invoke(InterestListRecoveryDUnitTest.class, "verifyRegisterK4toK5Retry");
    logger.fine("After verifyRegisterK4toK5Retry on serverSecondRegistered");
  }

  private boolean isInterestListRegisteredToServer1() {
    /*
    try {
      server1.invoke(InterestListRecoveryDUnitTest.class, "verifyRegionToProxyMapForFullRegistration");
    } catch (Throwable t) {
      // Means its registered on server2.
      return false;
    }
    return true;
    */
    // check whether the primary endpoint is connected to server1 or server2
    try {
      Region<?, ?> r1 = cache.getRegion("/" + REGION_NAME);
      String poolName = r1.getAttributes().getPoolName();
      assertNotNull(poolName);
      pool = (PoolImpl)PoolManager.find(poolName);
      assertNotNull(pool);
      return (pool.getPrimaryPort() == PORT1);
    } catch (Exception ex) {
      fail("while isInterestListRegisteredToServer1", ex);
    }
    // never reached
    return false;
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

  public static void createClientCache(String host, Integer port1, Integer port2 ) throws Exception
  {
    InterestListRecoveryDUnitTest test = new InterestListRecoveryDUnitTest(
        "temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = test.createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory()
      .addServer(host, port1.intValue())
      .addServer(host, port2.intValue())
      .setSubscriptionEnabled(true)
      .setSubscriptionRedundancy(-1)
      .setReadTimeout(250)
      .setThreadLocalConnections(true)
      .setSocketBufferSize(32768)
      .setMinConnections(4)
      // .setRetryAttempts(5)
      // .setRetryInterval(1000)
      .create("InterestListRecoveryDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    pool = p;

  }

  public static Integer createServerCache() throws Exception
  {
    InterestListRecoveryDUnitTest test = new InterestListRecoveryDUnitTest(
        "temp");
    cache = test.createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }




  public static void createEntries()
  {
    try {
      LocalRegion r1 = (LocalRegion)cache.getRegion("/" + REGION_NAME);
      for(int i=1 ; i<6; i++){
      if (!r1.containsKey("key-"+i)) {
        r1.create("key-"+i, "key-"+i);
      }
      assertEquals(r1.getEntry("key-"+i).getValue(), "key-"+i);
      }
    }
    catch (Exception ex) {
      fail("failed while createEntries()", ex);
    }
  }

  public static void registerK1toK5()
  {
    try {
      LocalRegion r = (LocalRegion)cache.getRegion("/" + REGION_NAME);
      for(int i=1 ; i<6 ; i++){
      r.registerInterest("key-"+i, InterestResultPolicy.KEYS);
      }
    }
    catch (Exception ex) {
      fail("failed while registering keys", ex);
    }
  }

  public static void unregisterK1toK3()
  {
    try {
      LocalRegion r = (LocalRegion)cache.getRegion("/" + REGION_NAME);
      for (int i=1 ; i<4 ; i++){
      r.unregisterInterest("key-"+i);
      }
    }
    catch (Exception ex) {
      fail("failed while un-registering keys", ex);
    }
  }


  public static void setServerUnavailable(String server)
  {
    try {
      throw new Exception("nyi");
      //ConnectionProxyImpl.markServerUnavailable(server);
    }
    catch (Exception ex) {
      fail("while setting server unavailable  "+ server, ex);
    }
  }
  public static void setServerAvailable(String server)
  {
    try {
      throw new Exception("nyi");
      //ConnectionProxyImpl.markServerAvailable(server);
    }
    catch (Exception ex) {
      fail("while setting server available  "+ server, ex);
    }
  }

  public static void killCurrentEndpoint()
  {
    try {
      Region r1 = cache.getRegion("/" + REGION_NAME);
      String poolName = r1.getAttributes().getPoolName();
      assertNotNull(poolName);
      pool = (PoolImpl)PoolManager.find(poolName);
      assertNotNull(pool);
      pool.killPrimaryEndpoint();
    }
    catch (Exception ex) {
      fail("while killCurrentEndpoint  "+ ex);
    }
  }



  public static void put(String key)
  {
    try {
      Region r1 = cache.getRegion("/" + REGION_NAME);
      r1.put(key, "server-"+key);
    }
    catch (Exception ex) {
      fail("failed while r.put()", ex);
    }
  }


  public static void verifyRegionToProxyMapForFullRegistrationRetry() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          verifyRegionToProxyMapForFullRegistration();
          return true;
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Error e) {
          return false;
        } 
        catch (RuntimeException re) {
          return false;
        }
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 20 * 1000, 200, true);
  }
  
   public static void verifyRegionToProxyMapForFullRegistration()
   {
     Iterator iter = getCacheClientProxies().iterator();
     if(iter.hasNext()){
       Set keys = getKeysOfInterestMap((CacheClientProxy)iter.next(), "/" + REGION_NAME);
       assertNotNull(keys);

       assertTrue(keys.contains("key-1"));
       assertTrue(keys.contains("key-2"));
       assertTrue(keys.contains("key-3"));
       assertTrue(keys.contains("key-4"));
       assertTrue(keys.contains("key-5"));
     }
   }


  public static void verifyRegisterK4toK5Retry() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          verifyRegisterK4toK5();
          return true;
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Error e) {
          return false;
        } 
        catch (RuntimeException re) {
          return false;
        }
      }

      public String description() {
        return "verifyRegisterK4toK5Retry";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 20 * 1000, 200, true);
  }

   public static void verifyRegisterK4toK5() {
     Iterator iter = getCacheClientProxies().iterator();
     if (iter.hasNext()) {
       Set keysMap = getKeysOfInterestMap((CacheClientProxy)iter.next(), "/" + REGION_NAME);
       assertNotNull(keysMap);

       assertFalse(keysMap.contains("key-1"));
       assertFalse(keysMap.contains("key-2"));
       assertFalse(keysMap.contains("key-3"));
       assertTrue(keysMap.contains("key-4"));
       assertTrue(keysMap.contains("key-5"));
     }
   }

  public static void verifyRegionToProxyMapForNoRegistrationRetry() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          verifyRegionToProxyMapForNoRegistration();
          return true;
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Error e) {
          return false;
        } 
        catch (RuntimeException re) {
          return false;
        }
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 20 * 1000, 200, true);
  }

 public static void verifyRegionToProxyMapForNoRegistration()
 {
   Iterator iter = getCacheClientProxies().iterator();
   if (iter.hasNext()) {
     Set keysMap = getKeysOfInterestMap((CacheClientProxy)iter.next(), "/" + REGION_NAME);
     if (keysMap != null) { // its ok not to have an empty map, just means there is no registration
       assertFalse(keysMap.contains("key-1"));
       assertFalse(keysMap.contains("key-2"));
       assertFalse(keysMap.contains("key-3"));
       assertFalse(keysMap.contains("key-4"));
       assertFalse(keysMap.contains("key-5"));
     }
   }
 }

 public static Set getCacheClientProxies() {
   Cache c = CacheFactory.getAnyInstance();
   assertEquals("More than one CacheServer", 1, c.getCacheServers().size());
   CacheServerImpl bs = (CacheServerImpl)c.getCacheServers().iterator()
   .next();
   assertNotNull(bs);
   assertNotNull(bs.getAcceptor());
   assertNotNull(bs.getAcceptor().getCacheClientNotifier());
   return new HashSet(bs.getAcceptor().getCacheClientNotifier().getClientProxies());
 }

 public static Set getKeysOfInterestMap(CacheClientProxy proxy, String regionName) {
   //assertNotNull(proxy.cils[RegisterInterestTracker.interestListIndex]);
   //assertNotNull(proxy.cils[RegisterInterestTracker.interestListIndex]._keysOfInterest);
   return proxy.cils[RegisterInterestTracker.interestListIndex]
     .getProfile(regionName).getKeysOfInterestFor(proxy.getProxyID());
 }

  @Override
  public void tearDown2() throws Exception
  {
    // close the clients first
    server2.invoke(InterestListRecoveryDUnitTest.class, "closeCache");
    closeCache();
    // then close the servers
    server1.invoke(InterestListRecoveryDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  
  public static void verifyDeadAndLiveServers(final int expectedDeadServers, 
      final int expectedLiveServers)
  {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int sz = pool.getConnectedServerCount();
        return sz == expectedLiveServers;
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
  }
}
