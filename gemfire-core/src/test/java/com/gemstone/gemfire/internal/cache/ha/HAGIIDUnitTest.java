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

package com.gemstone.gemfire.internal.cache.ha;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEventImpl;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientTombstoneMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Client is connected to S1 which has a slow dispatcher. Puts are made on S1.  Then S2 is started
 * and made available for the client. After that , S1 's  server is stopped. The client fails over to
 * S2. The client should receive all the puts . These puts have arrived on S2 via GII of HARegion.
 *
 *  @author Suyog Bhokare
 *
 */

class GIIChecker extends CacheListenerAdapter
{
  private boolean gotFirst = false;
  private boolean gotSecond = false;
  private boolean gotThird = false;
  private int updates = 0;
  
  public void afterUpdate(EntryEvent event) {
    
    this.updates++;
    
    String key = (String) event.getKey();
    String value = (String) event.getNewValue();
    
    if (key.equals("key-1") && value.equals("value-1")) {
      this.gotFirst = true;
    }
    
    if (key.equals("key-2") && value.equals("value-2")) {
      this.gotSecond = true;
    }
    
    if (key.equals("key-3") && value.equals("value-3")) {
      this.gotThird = true;
    }
  }
  
  public int getUpdates() {
    return this.updates;
  }
  
  public boolean gotFirst() {
    return this.gotFirst;
  }
  
  public boolean gotSecond() {
    return this.gotSecond;
  }
  
  public boolean gotThird() {
    return this.gotThird;
  }
}

public class HAGIIDUnitTest extends DistributedTestCase
{
  private static Cache cache = null;
  //server
  private static VM server0 = null;
  private static VM server1 = null;
  private static VM client0 = null;

  private static int PORT1;
  private static int PORT2;

  private static final String REGION_NAME = "HAGIIDUnitTest_region";
  
  protected static GIIChecker checker = new GIIChecker();

  /** constructor */
  public HAGIIDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
	super.setUp();
    final Host host = Host.getHost(0);

    //server
    server0 = host.getVM(0);
    server1 = host.getVM(1);

    //client
    client0 = host.getVM(2);

    //start server1
    PORT1 = ((Integer)server0.invoke(HAGIIDUnitTest.class, "createServer1Cache" )).intValue();
    server0.invoke(ConflationDUnitTest.class, "setIsSlowStart");
    server0.invoke(HAGIIDUnitTest.class, "setSystemProperty");


    PORT2 =  AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    //Start the client
    client0.invoke(HAGIIDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(host), new Integer(PORT1),new Integer(PORT2)});
  }
  
  public void testGIIRegionQueue()
  {
    client0.invoke(HAGIIDUnitTest.class, "createEntries");
    client0.invoke(HAGIIDUnitTest.class, "registerInterestList");
    server0.invoke(HAGIIDUnitTest.class, "put");
    
    server0.invoke(HAGIIDUnitTest.class, "tombstonegc");

    client0.invoke(HAGIIDUnitTest.class, "verifyEntries");
    server1.invoke(HAGIIDUnitTest.class, "createServer2Cache" ,new Object[] {new Integer(PORT2)});
    Wait.pause(6000);
    server0.invoke(HAGIIDUnitTest.class, "stopServer");
    //pause(10000);
    client0.invoke(HAGIIDUnitTest.class, "verifyEntriesAfterGiiViaListener");
  }

  public void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1 , Integer port2) throws Exception
  {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HAGIIDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    ClientServerTestCase.configureConnectionPool(factory, host, new int[] {PORT1,PORT2}, true, -1, 2, null, 1000, -1, false, -1);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.addCacheListener(HAGIIDUnitTest.checker);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  public static Integer createServer1Cache() throws Exception
  {
    new HAGIIDUnitTest("temp").createCache(new Properties());
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

  public static void createServer2Cache(Integer port) throws Exception
  {
    new HAGIIDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port.intValue());
    server1.setNotifyBySubscription(true);
    server1.start();
  }

  public static void registerInterestList()
  {
    try {
      Region r = cache.getRegion("/"+ REGION_NAME);
      assertNotNull(r);
      r.registerInterest("key-1",InterestResultPolicy.KEYS_VALUES);
      r.registerInterest("key-2",InterestResultPolicy.KEYS_VALUES);
      r.registerInterest("key-3",InterestResultPolicy.KEYS_VALUES);
    }
    catch (Exception ex) {
      Assert.fail("failed while registering keys ", ex);
    }
  }
  public static void createEntries()
  {
    try {
      Region r = cache.getRegion("/"+ REGION_NAME);
      assertNotNull(r);
      r.create("key-1", "key-1");
      r.create("key-2", "key-2");
      r.create("key-3", "key-3");

    }
    catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
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
      fail("failed while stopServer()" + e);
    }
  }

  public static void put()
  {
    try {
      Region r = cache.getRegion("/"+ REGION_NAME);
      assertNotNull(r);

      r.put("key-1", "value-1");
      r.put("key-2", "value-2");
      r.put("key-3", "value-3");

    }
    catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }
  
  /** queue a tombstone GC message for the client.  See bug #46832 */
  public static void tombstonegc() throws Exception {
    LocalRegion r = (LocalRegion)cache.getRegion("/"+REGION_NAME);
    assertNotNull(r);
    
    DistributedMember id = r.getCache().getDistributedSystem().getDistributedMember();
    RegionEventImpl regionEvent = new RegionEventImpl(r, Operation.REGION_DESTROY, null, true, id); 

    FilterInfo clientRouting = r.getFilterProfile().getLocalFilterRouting(regionEvent);
    assertTrue(clientRouting.getInterestedClients().size() > 0);
    
    regionEvent.setLocalFilterInfo(clientRouting); 

    Map<VersionSource, Long> map = Collections.emptyMap();
    ClientTombstoneMessage message = ClientTombstoneMessage.gc(
        r, map, new EventID(r.getCache().getDistributedSystem()));
    CacheClientNotifier.notifyClients(regionEvent, message);
  }

  public static void verifyEntries()
  {
    try {
      final Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      // wait until we
      // have a dead
      // server
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return r.getEntry("key-1").getValue().equals("key-1");
        }
        public String description() {
          return null;
        }
      };
      // assertEquals( "key-1",r.getEntry("key-1").getValue());
      // wait until we
      // have a dead
      // server
      ev = new WaitCriterion() {
        public boolean done() {
          return r.getEntry("key-2").getValue().equals("key-2");
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 60 * 1000, 200, true);
      // assertEquals( "key-2",r.getEntry("key-2").getValue());
      
      // wait until we
      // have a dead
      // server
      ev = new WaitCriterion() {
        public boolean done() {
          return r.getEntry("key-3").getValue().equals("key-3");
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 60 * 1000, 200, true);
      // assertEquals( "key-3",r.getEntry("key-3").getValue());
    }
    catch (Exception ex) {
      Assert.fail("failed while verifyEntries()", ex);
    }
  }

  public static void verifyEntriesAfterGiiViaListener() {
    
    // Check whether just the 3 expected updates arrive.
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return HAGIIDUnitTest.checker.gotFirst();
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 90 * 1000, 200, true);

    ev = new WaitCriterion() {
      public boolean done() {
        return HAGIIDUnitTest.checker.gotSecond();
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 60 * 1000, 200, true);

    ev = new WaitCriterion() {
      public boolean done() {
        return HAGIIDUnitTest.checker.gotThird();
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 60 * 1000, 200, true);
    
    assertEquals(3, HAGIIDUnitTest.checker.getUpdates());
  }
  
  public static void verifyEntriesAfterGII()
  {
    try {
      final Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      // wait until
      // we have a
      // dead server
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return r.getEntry("key-1").getValue().equals("value-1");
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 60 * 1000, 200, true);

      // wait until
      // we have a
      // dead server
      ev = new WaitCriterion() {
        public boolean done() {
          return r.getEntry("key-2").getValue().equals("value-2");
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 60 * 1000, 200, true);
      // assertEquals( "key-2",r.getEntry("key-2").getValue());


      // wait until
      // we have a
      // dead server
      ev = new WaitCriterion() {
        public boolean done() {
          return r.getEntry("key-3").getValue().equals("value-3");
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 60 * 1000, 200, true);
      
      /*
       * assertEquals( "value-1",r.getEntry("key-1").getValue()); assertEquals(
       * "value-2",r.getEntry("key-2").getValue()); assertEquals(
       * "value-3",r.getEntry("key-3").getValue());
       */

    }
    catch (Exception ex) {
      Assert.fail("failed while verifyEntriesAfterGII()", ex);
    }
  }

  public static void setSystemProperty()
  {
      System.setProperty("slowStartTimeForTesting", "120000");
  }

  @Override
  protected final void preTearDown() throws Exception {
    ConflationDUnitTest.unsetIsSlowStart();
    Invoke.invokeInEveryVM(ConflationDUnitTest.class, "unsetIsSlowStart");
    // close the clients first
    client0.invoke(HAGIIDUnitTest.class, "closeCache");
    // then close the servers
    server0.invoke(HAGIIDUnitTest.class, "closeCache");
    server1.invoke(HAGIIDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
