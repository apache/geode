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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.client.internal.ClientPartitionAdvisor;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * @author skumar
 *
 */
public class PartitionedRegionSingleHopWithServerGroupDUnitTest extends CacheTestCase{

  private static final long serialVersionUID = 1L;

  protected static final String PR_NAME = "single_hop_pr";
  protected static final String PR_NAME2 = "single_hop_pr_2";
  protected static final String PR_NAME3 = "single_hop_pr_3";
  private static final String CUSTOMER = "CUSTOMER";
  private static final String ORDER = "ORDER";
  private static final String SHIPMENT = "SHIPMENT";

  private static final String CUSTOMER2 = "CUSTOMER2";
  private static final String ORDER2 = "ORDER2";
  private static final String SHIPMENT2 = "SHIPMENT2";
  
  protected VM member0 = null;

  protected VM member1 = null;

  protected VM member2 = null;

  protected VM member3 = null;

  protected static Region region = null;

  protected static Region customerRegion = null;

  protected static Region orderRegion = null;

  protected static Region shipmentRegion = null;

  protected static Region region2 = null;

  protected static Region customerRegion2 = null;

  protected static Region orderRegion2 = null;

  protected static Region shipmentRegion2 = null;
  
  protected static Cache cache = null;

  protected static final int locatorPort = 12345;

  protected static Locator locator = null;
  
  public PartitionedRegionSingleHopWithServerGroupDUnitTest(String name) {
    super(name);
  }
  

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    member0 = host.getVM(0);
    member1 = host.getVM(1);
    member2 = host.getVM(2);
    member3 = host.getVM(3);
    addExpectedException("java.net.SocketException");
  }
  
  public void tearDown2() throws Exception {
    try {

      // close the clients first
      member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "closeCache");
      member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "closeCache");
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "closeCache");
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "closeCache");
      closeCache();

      super.tearDown2();

      member0 = null;
      member1 = null;
      member2 = null;
      member3 = null;
      invokeInEveryVM(new SerializableRunnable() { public void run() {
        cache = null;
        orderRegion = null;
        orderRegion2 = null;
        customerRegion = null;
        customerRegion2 = null;
        shipmentRegion = null;
        shipmentRegion2 = null;
        region = null;
        region2 = null;
        locator = null;
      } });

    }
    finally {
      unregisterAllDataSerializersFromAllVms();
    }
  }

  public static void closeCache() {
    resetHonourServerGroupsInPRSingleHop();
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void stopServer() {
    for (CacheServer cacheServer : cache.getCacheServers()) {
      cacheServer.stop();
    }
  }

  public void test_SingleHopWith2ServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "group1");

    // put
    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    try{
      verifyMetadata(4,1);
    }
    finally{
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "stopLocator");
    }
  }

  public void test_SingleHopWith2ServerGroup2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "group1");

    // put
    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    try{
      verifyMetadata(4,2);  
    }
    finally{
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "stopLocator");  
    }
  }
  
  public void test_SingleHopWith2ServerGroup2WithoutSystemProperty() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });

    createClientWithLocator(host0, port3, "group1");

    // put
    putIntoPartitionedRegions();

    getFromPartitionedRegions();

      verifyMetadata(4,3);
    }
    finally{
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "stopLocator");      
    }
  }

  public void test_SingleHopWithServerGroupAccessor() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 0,2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "group1");

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    try{
      verifyMetadata(0,0);  
    }
    finally{
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "stopLocator");  
    }
  }
  
  public void test_SingleHopWithServerGroupOneServerInTwoGroups() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,
            2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,
            2, 8, "group1" });
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,
            2, 8, "group1,group2" });

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "group1");

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    try{
      verifyMetadata(4, 3);  
    }
    finally{
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
      "stopLocator");  
    }
    
    
  }
  
  public void test_SingleHopWithServerGroupWithOneDefaultServer() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "" });

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "group1");

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    try{
      verifyMetadata(4,2);
    }
    finally{
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class, "stopLocator");  
    }
  }
  
  public void test_SingleHopWithServerGroupClientServerGroupNull() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group3" });

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "");

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    try {
      verifyMetadata(4, 3);
    } finally {
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "stopLocator");
    }
  }
  
  public void test_SingleHopWithServerGroupTwoClientServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "setHonourServerGroupsInPRSingleHop", new Object[] {});

    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createClientWithLocator", new Object[] {host0,port3,"group1"});

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "");

    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "putIntoPartitionedRegions", new Object[] {});
    
    putIntoPartitionedRegions();

    getFromPartitionedRegions();
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "getFromPartitionedRegions", new Object[] {});
    
    try {
      verifyMetadata(4, 2);
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "verifyMetadata", new Object[] {new Integer(4),new Integer(1)});
      
    } finally {
      resetHonourServerGroupsInPRSingleHop();
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "resetHonourServerGroupsInPRSingleHop", new Object[] {});
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "stopLocator");
    }
  }
  
  public void test_SingleHopWithServerGroupTwoClientServerGroup2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1,group2" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "setHonourServerGroupsInPRSingleHop", new Object[] {});

    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createClientWithLocator", new Object[] {host0,port3,"group1"});

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "group2");
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "putIntoPartitionedRegions", new Object[] {});
    putIntoPartitionedRegions();
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "getFromPartitionedRegions", new Object[] {});
    getFromPartitionedRegions();

    try {
      verifyMetadata(4, 2);
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "verifyMetadata", new Object[] {new Integer(4),new Integer(1)});

    } finally {
      resetHonourServerGroupsInPRSingleHop();
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "resetHonourServerGroupsInPRSingleHop", new Object[] {});
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "stopLocator");
    }
  }
  
  public void test_SingleHopWithServerGroupTwoClientOneWithOneWithoutServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1,group2" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createClientWithLocator", new Object[] {host0,port3,"group1"});

    setHonourServerGroupsInPRSingleHop();
    createClientWithLocator(host0, port3, "group2");
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "putIntoPartitionedRegions", new Object[] {});
    putIntoPartitionedRegions();
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "getFromPartitionedRegions", new Object[] {});
    getFromPartitionedRegions();

    try {
      verifyMetadata(4, 2);
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "verifyMetadata", new Object[] {new Integer(4),new Integer(2)});

    } finally {
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "stopLocator");
    }
  }
  
  public void test_SingleHopWithServerGroup2ClientInOneVMServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup2Regions", new Object[] { locator, 100,2, 8, "group1,group2" });
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup2Regions", new Object[] { locator, 100,2, 8, "group2" });
    
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "setHonourServerGroupsInPRSingleHop", new Object[] {});

    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "create2ClientWithLocator", new Object[] {host0,port3,"group1",""});

    setHonourServerGroupsInPRSingleHop();
    create2ClientWithLocator(host0, port3, "group2","group1");
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "putIntoPartitionedRegions2Client", new Object[] {});
    putIntoPartitionedRegions2Client();
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "getFromPartitionedRegions2Client", new Object[] {});
    getFromPartitionedRegions2Client();

    try {
      verifyMetadataFor2ClientsInOneVM(8, 2, 1);
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "verifyMetadataFor2ClientsInOneVM", new Object[] {new Integer(8),new Integer(1), new Integer(2)});

    } finally {
      resetHonourServerGroupsInPRSingleHop();
      member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "resetHonourServerGroupsInPRSingleHop", new Object[] {});
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "stopLocator");
    }
  }

  public void test_SingleHopWithServerGroupColocatedRegionsInDifferentGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group1,group2" });
    
    member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group2" });
    
    member2.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
        "createServerWithLocatorAndServerGroup", new Object[] { locator, 100,2, 8, "group3" });
    
    setHonourServerGroupsInPRSingleHop();
    createClientWith3PoolLocator(host0, port3, "group2","group1","");
    putIntoPartitionedRegions();
    getFromPartitionedRegions();

    try {
      verifyMetadataForColocatedRegionWithDiffPool(4, 2,1,3);
    } finally {
      resetHonourServerGroupsInPRSingleHop();
    }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest.class,
          "stopLocator");
    }
  }

  
  public static void verifyMetadata(final int numRegions, final int numBucketLocations) {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms
        .getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (regionMetaData.size()  == numRegions) {
          return true;
        }
        return false;
      }
      public String description() {
        return "expected metadata for each region to be" + numRegions + " but it is "  + regionMetaData.size() + "Metadata is " + regionMetaData.keySet();
      }
    };
    
    DistributedTestCase.waitForCriterion(wc, 60000, 1000, true);
    
    if (numRegions != 0) {
      assertTrue(regionMetaData.containsKey(region.getFullPath()));
      ClientPartitionAdvisor prMetaData = regionMetaData.get(region
          .getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        cache.getLogger().fine(
            "For bucket id " + e.getKey() + " the locations are "
                + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        assertEquals(numBucketLocations, ((List)entry.getValue()).size());
      }
    }
  }
  
  public static void verifyMetadataForColocatedRegionWithDiffPool(
      final int numRegions, final int numBucketLocations,
      final int numBucketLocations2, final int numBucketLocations3) {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms
        .getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (regionMetaData.size() == numRegions) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadata for each region to be " + numRegions
            + " but it is " + regionMetaData.size() + " they are "
            + regionMetaData.keySet();
      }
    };

    DistributedTestCase.waitForCriterion(wc, 120000, 1000, true);
    
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region
        .getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      cache.getLogger().fine(
          "For bucket id " + e.getKey() + " the locations are "
              + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      assertEquals(numBucketLocations, ((List)entry.getValue()).size());
    }

    assertTrue(regionMetaData.containsKey(customerRegion.getFullPath()));
    prMetaData = regionMetaData.get(customerRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      cache.getLogger().fine(
          "For bucket id " + e.getKey() + " the locations are "
              + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      assertEquals(numBucketLocations, ((List)entry.getValue()).size());
    }
    
    assertTrue(regionMetaData.containsKey(orderRegion.getFullPath()));
    prMetaData = regionMetaData.get(orderRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      cache.getLogger().fine(
          "For bucket id " + e.getKey() + " the locations are "
              + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      assertEquals(numBucketLocations2, ((List)entry.getValue()).size());
    }
    
    assertTrue(regionMetaData.containsKey(shipmentRegion.getFullPath()));
    prMetaData = regionMetaData.get(shipmentRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      cache.getLogger().fine(
          "For bucket id " + e.getKey() + " the locations are "
              + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      assertEquals(numBucketLocations3, ((List)entry.getValue()).size());
    }    

  }
  
  public static void verifyMetadataFor2ClientsInOneVM(final int numRegions, final int numBucketLocations, final int numBucketLocations2) {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms
        .getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (regionMetaData.size()  == numRegions) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadata for each region to be " + numRegions
            + " but it is " + regionMetaData.size() + " they are "
            + regionMetaData.keySet();
      }
    };
    
    DistributedTestCase.waitForCriterion(wc, 120000, 1000, true);
    
    if (numRegions != 0) {
      assertTrue(regionMetaData.containsKey(region.getFullPath()));
      ClientPartitionAdvisor prMetaData = regionMetaData.get(region
          .getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        cache.getLogger().fine(
            "For bucket id " + e.getKey() + " the locations are "
                + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        assertEquals(numBucketLocations, ((List)entry.getValue()).size());
      }

      assertTrue(regionMetaData.containsKey(customerRegion.getFullPath()));
      prMetaData = regionMetaData.get(customerRegion.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        cache.getLogger().fine(
            "For bucket id " + e.getKey() + " the locations are "
                + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        assertEquals(numBucketLocations, ((List)entry.getValue()).size());
      }
      
      assertTrue(regionMetaData.containsKey(customerRegion2.getFullPath()));
      prMetaData = regionMetaData.get(customerRegion2.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        cache.getLogger().fine(
            "For bucket id " + e.getKey() + " the locations are "
                + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
          .entrySet()) {
        assertEquals(numBucketLocations2, ((List)entry.getValue()).size());
      }
    }
  }
  
  public static int createServer(int redundantCopies, int totalNoofBuckets, String group) {
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    if(group.length() != 0)
      server.setGroups(new String[]{group});
    try {
      server.start();
    }
    catch (IOException e) {
      fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    return port;
  }
  
  public static int createServerWithLocatorAndServerGroup(String locator,
      int localMaxMemory,int redundantCopies, int totalNoofBuckets, String group) {

    Properties props = new Properties();
    props = new Properties();
    props.setProperty("locators", locator);
    
    System.setProperty("gemfire.PoolImpl.honourServerGroupsInPRSingleHop", "true");
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    CacheServer server = cache.addCacheServer();
    if (group.length() != 0) {
      StringTokenizer t = new StringTokenizer(group, ",");
      String[] a = new String[t.countTokens()];
      int i = 0;
      while (t.hasMoreTokens()) {
        a[i] = t.nextToken();
        i++;
      }
      server.setGroups(a);
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    return port;
  }
  
  public static int createServerWithLocatorAndServerGroup2Regions(String locator,
      int localMaxMemory,int redundantCopies, int totalNoofBuckets, String group) {

    Properties props = new Properties();
    props = new Properties();
    props.setProperty("locators", locator);
    
    System.setProperty("gemfire.PoolImpl.honourServerGroupsInPRSingleHop", "true");
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    CacheServer server = cache.addCacheServer();
    if (group.length() != 0) {
      StringTokenizer t = new StringTokenizer(group, ",");
      String[] a = new String[t.countTokens()];
      int i = 0;
      while (t.hasMoreTokens()) {
        a[i] = t.nextToken();
        i++;
      }
      server.setGroups(a);
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    
    
    
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets);
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region2 = cache.createRegion(PR_NAME2, attr.create());
    assertNotNull(region2);
    getLogWriter().info(
        "Partitioned Region " + PR_NAME2 + " created Successfully :"
            + region2.toString());

    
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion2 = cache.createRegion(CUSTOMER2, attr.create());
    assertNotNull(customerRegion2);
    getLogWriter().info(
        "Partitioned Region CUSTOMER2 created Successfully :"
            + customerRegion2.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion2 = cache.createRegion(ORDER2, attr.create());
    assertNotNull(orderRegion2);
    getLogWriter().info(
        "Partitioned Region ORDER2 created Successfully :"
            + orderRegion2.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion2 = cache.createRegion(SHIPMENT2, attr.create());
    assertNotNull(shipmentRegion2);
    getLogWriter().info(
        "Partitioned Region SHIPMENT2 created Successfully :"
            + shipmentRegion2.toString());
    
    return port;
  }
  
  public static void createClientWithLocator(String host, int port0, String group) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "");
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }
  
  public static void create2ClientWithLocator(String host, int port0, String group1, String group2) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1,p2,p3;
    try {
      p1 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group1).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME);
      p2 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group2).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME2);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    create2RegionsInClientCache(p1.getName(),p2.getName());
  }
  
  public static void createClientWith3PoolLocator(String host, int port0, String group1, String group2,String group3) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1,p2,p3;
    try {
      p1 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group1).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME);
      p2 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group2).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME2);
      p3 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group3).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME3);
      
      
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    createColocatedRegionsInClientCacheWithDiffPool(p1.getName(),p2.getName(),p3.getName());
  }
  
  private static void createRegionsInClientCache(String poolName) {
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(poolName);
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(PR_NAME, attrs);
    assertNotNull(region);
    getLogWriter().info(
        "Distributed Region " + PR_NAME + " created Successfully :"
            + region.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    attrs = factory.create();
    customerRegion = cache.createRegion("CUSTOMER", attrs);
    assertNotNull(customerRegion);
    getLogWriter().info(
        "Distributed Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    attrs = factory.create();
    orderRegion = cache.createRegion("ORDER", attrs);
    assertNotNull(orderRegion);
    getLogWriter().info(
        "Distributed Region ORDER created Successfully :"
            + orderRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    attrs = factory.create();
    shipmentRegion = cache.createRegion("SHIPMENT", attrs);
    assertNotNull(shipmentRegion);
    getLogWriter().info(
        "Distributed Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
  }
  
  private static void create2RegionsInClientCache(String poolName1, String poolName2) {
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(PR_NAME, attrs);
    assertNotNull(region);
    getLogWriter().info(
        "Distributed Region " + PR_NAME + " created Successfully :"
            + region.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    customerRegion = cache.createRegion("CUSTOMER", attrs);
    assertNotNull(customerRegion);
    getLogWriter().info(
        "Distributed Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    orderRegion = cache.createRegion("ORDER", attrs);
    assertNotNull(orderRegion);
    getLogWriter().info(
        "Distributed Region ORDER created Successfully :"
            + orderRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    shipmentRegion = cache.createRegion("SHIPMENT", attrs);
    assertNotNull(shipmentRegion);
    getLogWriter().info(
        "Distributed Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    
    
    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    factory.setDataPolicy(DataPolicy.EMPTY);
    attrs = factory.create();
    region2 = cache.createRegion(PR_NAME2, attrs);
    assertNotNull(region2);
    getLogWriter().info(
        "Distributed Region " + PR_NAME2 + " created Successfully :"
            + region2.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    customerRegion2 = cache.createRegion(CUSTOMER2, attrs);
    assertNotNull(customerRegion2);
    getLogWriter().info(
        "Distributed Region CUSTOMER2 created Successfully :"
            + customerRegion2.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    orderRegion2 = cache.createRegion(ORDER2, attrs);
    assertNotNull(orderRegion2);
    getLogWriter().info(
        "Distributed Region ORDER2 created Successfully :"
            + orderRegion2.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    shipmentRegion2 = cache.createRegion(SHIPMENT2, attrs);
    assertNotNull(shipmentRegion2);
    getLogWriter().info(
        "Distributed Region SHIPMENT2 created Successfully :"
            + shipmentRegion2.toString());
  }

  private static void createColocatedRegionsInClientCacheWithDiffPool(String poolName1, String poolName2,String poolName3) {
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(PR_NAME, attrs);
    assertNotNull(region);
    getLogWriter().info(
        "Distributed Region " + PR_NAME + " created Successfully :"
            + region.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    customerRegion = cache.createRegion("CUSTOMER", attrs);
    assertNotNull(customerRegion);
    getLogWriter().info(
        "Distributed Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    orderRegion = cache.createRegion("ORDER", attrs);
    assertNotNull(orderRegion);
    getLogWriter().info(
        "Distributed Region ORDER created Successfully :"
            + orderRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName3);
    attrs = factory.create();
    shipmentRegion = cache.createRegion("SHIPMENT", attrs);
    assertNotNull(shipmentRegion);
    getLogWriter().info(
        "Distributed Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    
  }

  
  public static int createAccessorServer(int redundantCopies, int numBuckets, String group) {
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    if(group.length() != 0) {
      server.setGroups(new String[]{group});
    }
    try {
      server.start();
    }
    catch (IOException e) {
      fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());

    assertNotNull(region);
    getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0)
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0)
        .setColocatedWith("CUSTOMER").setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0)
        .setColocatedWith("ORDER").setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    return port;
  }
  
  public static void createClientWithLocatorWithoutSystemProperty(String host, int port0, String group) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopWithServerGroupDUnitTest(
        "PartitionedRegionSingleHopWithServerGroupDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  
  public static void putIntoPartitionedRegions() {
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      Order order = new Order("OREDR" + j);
      orderRegion.put(orderId, order);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      Shipment shipment = new Shipment("Shipment" + k);
      shipmentRegion.put(shipmentId, shipment);
    }

    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    region.put(new Integer(4), "create0");
    region.put(new Integer(5), "create1");
    region.put(new Integer(6), "create2");
    region.put(new Integer(7), "create3");
    
    region.put(new Integer(0), "update0");
    region.put(new Integer(1), "update1");
    region.put(new Integer(2), "update2");
    region.put(new Integer(3), "update3");
    region.put(new Integer(4), "update0");
    region.put(new Integer(5), "update1");
    region.put(new Integer(6), "update2");
    region.put(new Integer(7), "update3");
    
    region.put(new Integer(0), "update00");
    region.put(new Integer(1), "update11");
    region.put(new Integer(2), "update22");
    region.put(new Integer(3), "update33");
    region.put(new Integer(4), "update00");
    region.put(new Integer(5), "update11");
    region.put(new Integer(6), "update22");
    region.put(new Integer(7), "update33");
  }
  
//  public static void putIntoPartitionedRegions2() {
//    for (int i = 801; i <= 1600; i++) {
//      CustId custid = new CustId(i);
//      Customer customer = new Customer("name" + i, "Address" + i);
//      customerRegion.put(custid, customer);
//    }
//    for (int j = 801; j <= 1600; j++) {
//      CustId custid = new CustId(j);
//      OrderId orderId = new OrderId(j, custid);
//      Order order = new Order("OREDR" + j);
//      orderRegion.put(orderId, order);
//    }
//    for (int k = 801; k <= 1600; k++) {
//      CustId custid = new CustId(k);
//      OrderId orderId = new OrderId(k, custid);
//      ShipmentId shipmentId = new ShipmentId(k, orderId);
//      Shipment shipment = new Shipment("Shipment" + k);
//      shipmentRegion.put(shipmentId, shipment);
//    }
//
//    region.put(new Integer(8), "create0");
//    region.put(new Integer(9), "create1");
//    region.put(new Integer(10), "create2");
//    region.put(new Integer(11), "create3");
//    region.put(new Integer(12), "create0");
//    region.put(new Integer(13), "create1");
//    region.put(new Integer(14), "create2");
//    region.put(new Integer(15), "create3");
//    
//    region.put(new Integer(8), "update0");
//    region.put(new Integer(9), "update1");
//    region.put(new Integer(10), "update2");
//    region.put(new Integer(11), "update3");
//    region.put(new Integer(12), "update0");
//    region.put(new Integer(13), "update1");
//    region.put(new Integer(14), "update2");
//    region.put(new Integer(15), "update3");
//    
//    region.put(new Integer(8), "update00");
//    region.put(new Integer(9), "update11");
//    region.put(new Integer(10), "update22");
//    region.put(new Integer(11), "update33");
//    region.put(new Integer(12), "update00");
//    region.put(new Integer(13), "update11");
//    region.put(new Integer(14), "update22");
//    region.put(new Integer(15), "update33");
//  }
  
  public static void putIntoPartitionedRegions2Client() {
    
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
      customerRegion2.put(custid, customer);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      Order order = new Order("OREDR" + j);
      orderRegion.put(orderId, order);
      orderRegion2.put(orderId, order);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      Shipment shipment = new Shipment("Shipment" + k);
      shipmentRegion.put(shipmentId, shipment);
      shipmentRegion2.put(shipmentId, shipment);
    }
    
    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    region.put(new Integer(4), "create0");
    region.put(new Integer(5), "create1");
    region.put(new Integer(6), "create2");
    region.put(new Integer(7), "create3");
    
    region.put(new Integer(0), "update0");
    region.put(new Integer(1), "update1");
    region.put(new Integer(2), "update2");
    region.put(new Integer(3), "update3");
    region.put(new Integer(4), "update0");
    region.put(new Integer(5), "update1");
    region.put(new Integer(6), "update2");
    region.put(new Integer(7), "update3");
    
    region.put(new Integer(0), "update00");
    region.put(new Integer(1), "update11");
    region.put(new Integer(2), "update22");
    region.put(new Integer(3), "update33");
    region.put(new Integer(4), "update00");
    region.put(new Integer(5), "update11");
    region.put(new Integer(6), "update22");
    region.put(new Integer(7), "update33");
    
    region2.put(new Integer(0), "create0");
    region2.put(new Integer(1), "create1");
    region2.put(new Integer(2), "create2");
    region2.put(new Integer(3), "create3");
    region2.put(new Integer(4), "create0");
    region2.put(new Integer(5), "create1");
    region2.put(new Integer(6), "create2");
    region2.put(new Integer(7), "create3");
    
    region2.put(new Integer(0), "update0");
    region2.put(new Integer(1), "update1");
    region2.put(new Integer(2), "update2");
    region2.put(new Integer(3), "update3");
    region2.put(new Integer(4), "update0");
    region2.put(new Integer(5), "update1");
    region2.put(new Integer(6), "update2");
    region2.put(new Integer(7), "update3");
    
    region2.put(new Integer(0), "update00");
    region2.put(new Integer(1), "update11");
    region2.put(new Integer(2), "update22");
    region2.put(new Integer(3), "update33");
    region2.put(new Integer(4), "update00");
    region2.put(new Integer(5), "update11");
    region2.put(new Integer(6), "update22");
    region2.put(new Integer(7), "update33");

  }

  public static void getFromPartitionedRegions() {
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      customerRegion.get(custid);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      orderRegion.get(orderId);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      shipmentRegion.get(shipmentId);
     }

    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));
    region.get(new Integer(4));
    region.get(new Integer(5));
    region.get(new Integer(6));
    region.get(new Integer(7));
  }
  
  
//  public static void getFromPartitionedRegions2() {
//    for (int i = 801; i <= 1600; i++) {
//      CustId custid = new CustId(i);
//      customerRegion.get(custid);
//    }
//    for (int j = 801; j <= 1600; j++) {
//      CustId custid = new CustId(j);
//      OrderId orderId = new OrderId(j, custid);
//      orderRegion.get(orderId);
//    }
//    for (int k = 801; k <= 1600; k++) {
//      CustId custid = new CustId(k);
//      OrderId orderId = new OrderId(k, custid);
//      ShipmentId shipmentId = new ShipmentId(k, orderId);
//      shipmentRegion.get(shipmentId);
//     }
//    region.get(new Integer(8));
//    region.get(new Integer(9));
//    region.get(new Integer(10));
//    region.get(new Integer(11));
//    region.get(new Integer(12));
//    region.get(new Integer(13));
//    region.get(new Integer(14));
//    region.get(new Integer(15));
//    
//  }
  
  public static void getFromPartitionedRegions2Client() {
    
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      customerRegion.get(custid);
      customerRegion2.get(custid);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      orderRegion.get(orderId);
      orderRegion2.get(orderId);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      shipmentRegion.get(shipmentId);
      shipmentRegion2.get(shipmentId);
     }

    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));
    region.get(new Integer(4));
    region.get(new Integer(5));
    region.get(new Integer(6));
    region.get(new Integer(7));
    
    region2.get(new Integer(0));
    region2.get(new Integer(1));
    region2.get(new Integer(2));
    region2.get(new Integer(3));
    region2.get(new Integer(4));
    region2.get(new Integer(5));
    region2.get(new Integer(6));
    region.get(new Integer(7));
    
  }
  public static void startLocatorInVM(final int locatorPort) {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    props.setProperty(DistributionConfig.LOG_FILE_NAME, "");

    try {
      locator = Locator.startLocatorAndDS(locatorPort, null, null, props);
    }
    catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void stopLocator() {
    locator.stop();
  }
  
  public static void resetHonourServerGroupsInPRSingleHop() {
    System.setProperty("gemfire.PoolImpl.honourServerGroupsInPRSingleHop", "False");
  }
  
  public static void setHonourServerGroupsInPRSingleHop() {
    System.setProperty("gemfire.PoolImpl.honourServerGroupsInPRSingleHop", "True");
  }
}
