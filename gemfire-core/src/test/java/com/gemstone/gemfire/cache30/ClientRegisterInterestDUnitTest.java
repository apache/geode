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
package com.gemstone.gemfire.cache30;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.cache.client.SubscriptionNotEnabledException;

/**
 * Tests the client register interest
 *
 * @author Kirk Lund
 * @since 4.2.3
 */
public class ClientRegisterInterestDUnitTest extends ClientServerTestCase {

  public ClientRegisterInterestDUnitTest(String name) {
    super(name);
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    disconnectAllFromDS(); // cleans up bridge server and client and lonerDS
  }
  
  /**
   * Tests for Bug 35381 Calling register interest if 
   * establishCallbackConnection is not set causes bridge server NPE.
   */
  public void testBug35381() throws Exception {
    final Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1]; // 1 server in this test
    
    final int whichVM = 0;
    final VM vm = Host.getHost(0).getVM(whichVM);
    vm.invoke(new CacheSerializableRunnable("Create bridge server") {
      public void run2() throws CacheException {
        getLogWriter().info("[testBug35381] Create BridgeServer");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name, factory.create());
        assertNotNull(region);
        assertNotNull(getRootRegion().getSubregion(name));
        region.put("KEY-1", "VAL-1");
        
        try {
          bridgeServerPort = startBridgeServer(0);
        }
        catch (IOException e) {
          getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException " + e.getMessage());
        }
        
        assertTrue(bridgeServerPort != 0);
    
        getLogWriter().info("[testBug35381] port=" + bridgeServerPort);
        getLogWriter().info("[testBug35381] serverMemberId=" + getMemberId());
      }
    });
    ports[whichVM] = vm.invokeInt(ClientRegisterInterestDUnitTest.class, 
                                  "getBridgeServerPort");
    assertTrue(ports[whichVM] != 0);
    
    getLogWriter().info("[testBug35381] create bridge client");
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(config);
    getCache();
    
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    getLogWriter().info("[testBug35381] creating connection pool");
    boolean establishCallbackConnection = false; // SOURCE OF BUG 35381
    ClientServerTestCase.configureConnectionPool(factory, getServerHostName(host), ports, establishCallbackConnection, -1, -1, null);
    Region region = createRegion(name, factory.create());
    assertNotNull(getRootRegion().getSubregion(name));
    try {
      region.registerInterest("KEY-1");
      fail("registerInterest failed to throw SubscriptionNotEnabledException with establishCallbackConnection set to false"); 
    }
    catch (SubscriptionNotEnabledException expected) {
    }
  }
  protected static int bridgeServerPort;
  private static int getBridgeServerPort() {
    return bridgeServerPort;
  }
  
  /**
   * Tests failover of register interest from client point of view. Related
   * bugs include:
   *
   * <p>Bug 35654 "failed re-registration may never be detected and thus
   * may never re-re-register"
   *
   * <p>Bug 35639 "registerInterest re-registration happens everytime a healthy
   * server is detected"
   *
   * <p>Bug 35655 "a single failed re-registration causes all other pending
   * re-registrations to be cancelled"
   */
  public void _testRegisterInterestFailover() throws Exception {
    // controller is bridge client
    
    final Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final String regionName1 = name+"-1";
    final String regionName2 = name+"-2";
    final String regionName3 = name+"-3";
    final String key1 = "KEY-"+regionName1+"-1";
    final String key2 = "KEY-"+regionName1+"-2";
    final String key3 = "KEY-"+regionName1+"-3";
    final int[] ports = new int[3]; // 3 servers in this test
    
    // create first bridge server with region for client...
    final int firstServerIdx = 0;
    final VM firstServerVM = Host.getHost(0).getVM(firstServerIdx);
    firstServerVM.invoke(new CacheSerializableRunnable("Create first bridge server") {
      public void run2() throws CacheException {
        getLogWriter().info("[testRegisterInterestFailover] Create first bridge server");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region1 = createRootRegion(regionName1, factory.create());
        Region region2 = createRootRegion(regionName2, factory.create());
        Region region3 = createRootRegion(regionName3, factory.create());
        region1.put(key1, "VAL-1");
        region2.put(key2, "VAL-1");
        region3.put(key3, "VAL-1");
        
        try {
          bridgeServerPort = startBridgeServer(0);
        }
        catch (IOException e) {
          getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException " + e.getMessage());
        }
        
        assertTrue(bridgeServerPort != 0);
    
        getLogWriter().info("[testRegisterInterestFailover] " +
          "firstServer port=" + bridgeServerPort);
        getLogWriter().info("[testRegisterInterestFailover] " +
          "firstServer memberId=" + getMemberId());
      }
    });

    // create second bridge server missing region for client...
    final int secondServerIdx = 1;
    final VM secondServerVM = Host.getHost(0).getVM(secondServerIdx);
    secondServerVM.invoke(new CacheSerializableRunnable("Create second bridge server") {
      public void run2() throws CacheException {
        getLogWriter().info("[testRegisterInterestFailover] Create second bridge server");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region1 = createRootRegion(regionName1, factory.create());
        Region region3 = createRootRegion(regionName3, factory.create());
        region1.put(key1, "VAL-2");
        region3.put(key3, "VAL-2");
        
        try {
          bridgeServerPort = startBridgeServer(0);
        }
        catch (IOException e) {
          getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException " + e.getMessage());
        }
        
        assertTrue(bridgeServerPort != 0);
    
        getLogWriter().info("[testRegisterInterestFailover] " +
          "secondServer port=" + bridgeServerPort);
        getLogWriter().info("[testRegisterInterestFailover] " +
          "secondServer memberId=" + getMemberId());
      }
    });

    // get the bridge server ports...
    ports[firstServerIdx] = firstServerVM.invokeInt(
      ClientRegisterInterestDUnitTest.class, "getBridgeServerPort");
    assertTrue(ports[firstServerIdx] != 0);
    ports[secondServerIdx] = secondServerVM.invokeInt(
      ClientRegisterInterestDUnitTest.class, "getBridgeServerPort");
    assertTrue(ports[secondServerIdx] != 0);
    assertTrue(ports[firstServerIdx] != ports[secondServerIdx]);
    
    // stop second and third servers
    secondServerVM.invoke(new CacheSerializableRunnable("Stop second bridge server") {
      public void run2() throws CacheException {
        stopBridgeServers(getCache());
      }
    });
    
    // create the bridge client
    getLogWriter().info("[testBug35654] create bridge client");
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(config);
    getCache();
    
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    getLogWriter().info("[testRegisterInterestFailover] creating connection pool");
    boolean establishCallbackConnection = true;
    final PoolImpl p = (PoolImpl)ClientServerTestCase.configureConnectionPool(factory, getServerHostName(host), ports, establishCallbackConnection, -1, -1, null);

    final Region region1 = createRootRegion(regionName1, factory.create());
    final Region region2 = createRootRegion(regionName2, factory.create());
    final Region region3 = createRootRegion(regionName3, factory.create());

    assertTrue(region1.getInterestList().isEmpty());
    assertTrue(region2.getInterestList().isEmpty());
    assertTrue(region3.getInterestList().isEmpty());

    region1.registerInterest(key1);
    region2.registerInterest(key2);
    region3.registerInterest(key3);

    assertTrue(region1.getInterestList().contains(key1));
    assertTrue(region2.getInterestList().contains(key2));
    assertTrue(region3.getInterestList().contains(key3));
    
    assertTrue(region1.getInterestListRegex().isEmpty());
    assertTrue(region2.getInterestListRegex().isEmpty());
    assertTrue(region3.getInterestListRegex().isEmpty());
    
    // get ConnectionProxy and wait until connected to first server
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return p.getPrimaryPort() != -1;
      }
      public String description() {
        return "primary port remained invalid";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
    assertEquals(ports[firstServerIdx], p.getPrimaryPort()); 
    
    // assert intial values
    assertEquals("VAL-1", region1.get(key1));
    assertEquals("VAL-1", region2.get(key2));
    assertEquals("VAL-1", region3.get(key3));
    
    // do puts on server1 and make sure values come thru for all 3 registrations
    firstServerVM.invoke(new CacheSerializableRunnable("Puts from first bridge server") {
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion(regionName1);
        region1.put(key1, "VAL-1-1");
        Region region2 = getCache().getRegion(regionName2);
        region2.put(key2, "VAL-1-1");
        Region region3 = getCache().getRegion(regionName3);
        region3.put(key3, "VAL-1-1");
      }
    });

    ev = new WaitCriterion() {
      public boolean done() {
        if (!"VAL-1-1".equals(region1.get(key1)) || 
            !"VAL-1-1".equals(region2.get(key2)) ||
            !"VAL-1-1".equals(region3.get(key3))
            ) return  false;
        return true;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
    assertEquals("VAL-1-1", region1.get(key1));
    assertEquals("VAL-1-1", region2.get(key2));
    assertEquals("VAL-1-1", region3.get(key3));
    
    // force failover to server 2
    secondServerVM.invoke(new CacheSerializableRunnable("Start second bridge server") {
      public void run2() throws CacheException {
        try {
          startBridgeServer(ports[secondServerIdx]);
        }
        catch (IOException e) {
          getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException " + e.getMessage());
        }
      }
    });
   
    firstServerVM.invoke(new CacheSerializableRunnable("Stop first bridge server") {
      public void run2() throws CacheException {
        stopBridgeServers(getCache());
      }
    });

    // wait for failover to second server
    ev = new WaitCriterion() {
      public boolean done() {
        return ports[secondServerIdx] == p.getPrimaryPort();
      }
      public String description() {
        return "primary port never became " + ports[secondServerIdx];
      }
    };
    DistributedTestCase.waitForCriterion(ev, 100 * 1000, 200, true);
    
    try {
      assertEquals(null, region2.get(key2));
      fail("CacheLoaderException expected");
    }
    catch (com.gemstone.gemfire.cache.CacheLoaderException e) {
    }
  
    // region2 registration should be gone now
    // do puts on server2 and make sure values come thru for only 2 registrations
    secondServerVM.invoke(new CacheSerializableRunnable("Puts from second bridge server") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRootRegion(regionName2, factory.create());
      }
    });
    
    // assert that there is no actively registered interest on region2
    assertTrue(region2.getInterestList().isEmpty());
    assertTrue(region2.getInterestListRegex().isEmpty());

    region2.put(key2, "VAL-0");
    
    secondServerVM.invoke(new CacheSerializableRunnable("Put from second bridge server") {
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion(regionName1);
        region1.put(key1, "VAL-2-2");
        Region region2 = getCache().getRegion(regionName2);
        region2.put(key2, "VAL-2-1");
        Region region3 = getCache().getRegion(regionName3);
        region3.put(key3, "VAL-2-2");
      }
    });
    
    // wait for updates to come thru
    ev = new WaitCriterion() {
      public boolean done() {
        if (!"VAL-2-2".equals(region1.get(key1)) || 
            !"VAL-2-2".equals(region3.get(key3)))
          return false;
        return true;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 100 * 1000, 200, true);
    assertEquals("VAL-2-2", region1.get(key1));
    assertEquals("VAL-0",   region2.get(key2));
    assertEquals("VAL-2-2", region3.get(key3));

    // assert again that there is no actively registered interest on region2
    assertTrue(region2.getInterestList().isEmpty());

    // register interest again on region2 and make
    region2.registerInterest(key2);
    assertEquals("VAL-2-1", region2.get(key2));
    
    secondServerVM.invoke(new CacheSerializableRunnable("Put from second bridge server") {
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion(regionName1);
        region1.put(key1, "VAL-2-3");
        Region region2 = getCache().getRegion(regionName2);
        region2.put(key2, "VAL-2-2");
        Region region3 = getCache().getRegion(regionName3);
        region3.put(key3, "VAL-2-3");
      }
    });
    
    // wait for updates to come thru
    ev = new WaitCriterion() {
      public boolean done() {
        if (!"VAL-2-3".equals(region1.get(key1)) || 
            !"VAL-2-2".equals(region2.get(key2)) ||
            !"VAL-2-3".equals(region3.get(key3)))
          return false;
        return true;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 100 * 1000, 200, true);
    assertEquals("VAL-2-3", region1.get(key1));
    assertEquals("VAL-2-2", region2.get(key2));
    assertEquals("VAL-2-3", region3.get(key3));

    // assert public methods report actively registered interest on region2
    assertTrue(region2.getInterestList().contains(key2));
  }
  
}

