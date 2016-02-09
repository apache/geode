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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Ignore;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.client.internal.ClientPartitionAdvisor;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.BucketAdvisor.ServerBucketProfile;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

public class PartitionedRegionSingleHopDUnitTest extends CacheTestCase {

  private static final long serialVersionUID = 1L;

  private static final String PR_NAME = "single_hop_pr";

  VM member0 = null;

  VM member1 = null;

  VM member2 = null;

  VM member3 = null;

  private static Region region = null;

  private static Region customerRegion = null;

  private static Region orderRegion = null;

  private static Region shipmentRegion = null;

  private static Region replicatedRegion = null;

  private static Cache cache = null;

  private static Locator locator = null;
  
  public PartitionedRegionSingleHopDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    IgnoredException.addIgnoredException("Connection refused");
    Host host = Host.getHost(0);
    member0 = host.getVM(0);
    member1 = host.getVM(1);
    member2 = host.getVM(2);
    member3 = host.getVM(3);
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    try {
      closeCache();
      
      member0 = null;
      member1 = null;
      member2 = null;
      member3 = null;

    }
    finally {
      DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    }
  }

  public static void closeCache() {
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
  
 public static void startLocatorInVM(final int locatorPort) {

    File logFile = new File("locator-" + locatorPort + ".log");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    try {
      locator = Locator.startLocatorAndDS(locatorPort, logFile, null, props);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void stopLocator() {
    locator.stop();
  }

  public static int createServerWithLocator(String locString,
      int redundantCopies, int totalNoofBuckets) {

    Properties props = new Properties();
    props = new Properties();
    props.setProperty("locators", locString);
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    return port;
  }

  public static void clearMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.getClientPartitionAttributesMap().clear();
    cms.getClientPRMetadata_TEST_ONLY().clear();
  }

  // 2 peers 2 servers 1 accessor.No client.Should work without any exceptions.
  public void test_NoClient() {
    member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });
    member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createPeer");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "createPeer");

    createAccessorServer();
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "clearMetadata");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "clearMetadata");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "clearMetadata");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "clearMetadata");
    clearMetadata();

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "putIntoPartitionedRegions");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "putIntoPartitionedRegions");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "putIntoPartitionedRegions");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "putIntoPartitionedRegions");
    putIntoPartitionedRegions();

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "getFromPartitionedRegions");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "getFromPartitionedRegions");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "getFromPartitionedRegions");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "getFromPartitionedRegions");
    getFromPartitionedRegions();

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyMetadata");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyMetadata");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyMetadata");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyMetadata");
    verifyEmptyMetadata();

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyStaticData");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyStaticData");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyStaticData");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "verifyEmptyStaticData");
    verifyEmptyStaticData();
  }

  // 2 AccessorServers, 2 Peers
  // 1 Client connected to 2 AccessorServers. Hence metadata should not be
  // fetched.
  public void test_ClientConnectedToAccessors() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createAccessorServer");
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createAccessorServer");

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createPeer");

    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "createPeer");

    createClient(port0, port1);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyEmptyMetadata();

    verifyEmptyStaticData();
  }

  // 1 server 2 accesorservers 2 peers.i client connected to the server
  // Since only 1 server hence Metadata should not be fetched.
  public void test_ClientConnectedTo1Server() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });

    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "createPeer");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createPeer");

    member3.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "createAccessorServer");

    createClient(port0);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyEmptyMetadata();

    verifyEmptyStaticData();
  }

  // 4 servers, 1 client connected to all 4 servers.
  // Put data, get data and make the metadata stable.
  // Now verify that metadata has all 8 buckets info.
  // Now update and ensure the fetch service is never called.
  public void test_MetadataContents() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });
    Integer port3 = (Integer)member3.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });
    createClient(port0, port1, port2, port3);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");

    verifyMetadata();
    updateIntoSinglePR();
  }

  // 2 servers, 2 clients.One client to one server.
  // Put from c1 to s1. Now put from c2. So since there will be a hop atleast
  // once,
  // fetchservice has to be triggered.
  // Now put again from c2.There should be no hop at all.
  public void test_MetadataServiceCallAccuracy() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createClient",
        new Object[] { port0 });
    createClient(port1);

    member2
        .invoke(PartitionedRegionSingleHopDUnitTest.class, "putIntoSinglePR");

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    final ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    
    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (cms.isRefreshMetadataTestOnly()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadataservice to be called atleast once, but it was not called";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    //pause(5000);
    wc = new WaitCriterion() {
      public boolean done() {
        return !cms.isRefreshMetadataTestOnly();
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);

  }

  public void test_MetadataServiceCallAccuracy_FromDestroyOp() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createClient",
        new Object[] { port0 });
    createClient(port1);

    member2
        .invoke(PartitionedRegionSingleHopDUnitTest.class, "putIntoSinglePR");

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    final ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    
    region.destroy(new Integer(0));
    region.destroy(new Integer(1));
    region.destroy(new Integer(2));
    region.destroy(new Integer(3));
    
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (cms.isRefreshMetadataTestOnly()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadataservice to be called atleast once, but it was not called";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
  }

  public void test_MetadataServiceCallAccuracy_FromGetOp() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createClient",
        new Object[] { port0 });
    createClient(port1);

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "putIntoSinglePR");

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    final ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    

    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));

    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (cms.isRefreshMetadataTestOnly()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadataservice to be called atleast once, but it was not called";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
    printMetadata();
    Wait.pause(5000);
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());

  }

  public void test_SingleHopWithHA() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 8 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 8 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 8 });
    Integer port3 = (Integer)member3.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 8 });
    createClient(port0, port1, port2, port3);
    final ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    // put
    for (int i = 1; i <= 16; i++) {
      region.put(new Integer(i), new Integer(i));
    }

    // update
    for (int i = 1; i <= 16; i++) {
      region.put(new Integer(i), new Integer(i + 1));
    }

    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (cms.isRefreshMetadataTestOnly()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadataservice to be called atleast once, but it was not called";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);

    // kill server
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopServer");

    // again update
    for (int i = 1; i <= 16; i++) {
      region.put(new Integer(i), new Integer(i + 10));
    }
  }

  public void test_SingleHopWithHAWithLocator() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "startLocatorInVM", new Object[] { port3 });
    try {

    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServerWithLocator",
        new Object[] { locator, 0, 8 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServerWithLocator",
        new Object[] { locator, 0, 8 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServerWithLocator",
        new Object[] { locator, 0, 8 });

    createClientWithLocator(host0, port3);

    // put
    for (int i = 1; i <= 16; i++) {
      region.put(new Integer(i), new Integer(i));
    }

    // update
    for (int i = 1; i <= 16; i++) {
      region.put(new Integer(i), new Integer(i + 1));
    }

    // kill server
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopServer");

    // again update
    for (int i = 1; i <= 16; i++) {
      region.put(new Integer(i), new Integer(i + 10));
    }

    } finally {
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopLocator");
    }
  }

  
  public void test_NoMetadataServiceCall_ForGetOp() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createClientWithoutPRSingleHopEnabled",
        new Object[] { port0 });
    createClientWithoutPRSingleHopEnabled(port1);
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class,
        "putIntoSinglePR");

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");

    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
    printMetadata();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
  }


 public void test_NoMetadataServiceCall() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 1, 4 });

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createClientWithoutPRSingleHopEnabled",
        new Object[] { port0 });
    createClientWithoutPRSingleHopEnabled(port1);

    member2
        .invoke(PartitionedRegionSingleHopDUnitTest.class, "putIntoSinglePR");
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");

 
    region.put(new Integer(0), "create0");
    final boolean metadataRefreshed_get1 = cms
        .isRefreshMetadataTestOnly();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(new Integer(1), "create1");
    final boolean metadataRefreshed_get2 = cms
        .isRefreshMetadataTestOnly();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(new Integer(2), "create2");
    final boolean metadataRefreshed_get3 = cms
        .isRefreshMetadataTestOnly();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(new Integer(3), "create3");
    final boolean metadataRefreshed_get4 = cms
        .isRefreshMetadataTestOnly();
    Wait.pause(5000);
    assertFalse(metadataRefreshed_get1 || metadataRefreshed_get2
            || metadataRefreshed_get3 || metadataRefreshed_get4);

    printMetadata();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());

  }

  public void test_NoMetadataServiceCall_ForDestroyOp() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 0, 4 });

    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "createClientWithoutPRSingleHopEnabled",
        new Object[] { port0 });
    createClientWithoutPRSingleHopEnabled(port1);

    member2
        .invoke(PartitionedRegionSingleHopDUnitTest.class, "putIntoSinglePR");

    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "printView");
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.destroy(new Integer(0));
    region.destroy(new Integer(1));
    region.destroy(new Integer(2));
    region.destroy(new Integer(3));
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
  }

  public void testServerLocationRemovalThroughPing() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port3 = (Integer)member3.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    createClient(port0, port1, port2, port3);
    putIntoPartitionedRegions();    
    getFromPartitionedRegions();
    Wait.pause(5000);
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
    assertEquals(4, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    assertTrue(regionMetaData.containsKey(customerRegion.getFullPath()));
    assertTrue(regionMetaData.containsKey(orderRegion.getFullPath()));
    assertTrue(regionMetaData.containsKey(shipmentRegion.getFullPath()));
    
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath()); 
    assertEquals(4/*numBuckets*/, prMetaData.getBucketServerLocationsMap_TEST_ONLY().size());
    
    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertEquals(4, ((List)entry.getValue()).size());
    }
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopServer");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopServer");
    Wait.pause(5000);//make sure that ping detects the dead servers
    getFromPartitionedRegions();
    verifyDeadServer(regionMetaData, customerRegion, port0, port1);
    verifyDeadServer(regionMetaData, region, port0, port1);
  }
  
  public void testMetadataFetchOnlyThroughFunctions() {
    //Workaround for 52004
    IgnoredException.addIgnoredException("InternalFunctionInvocationTargetException");
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port3 = (Integer)member3.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    createClient(port0, port1, port2, port3);
    executeFunctions();
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return (regionMetaData.size() == 1);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
    
    assertEquals(1, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    
    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath()); 
    
    //Fixes a race condition in GEODE-414 by retrying as 
    //region.clientMetaDataLock.tryLock() may prevent fetching the 
    //metadata through functional calls as only limited functions are executed in the test.
    long start = System.currentTimeMillis();
    do {
      if ((prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() !=4)) {
        //waiting if there is another thread holding the lock
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          //ignored.
        }
        cms.getClientPRMetadata((LocalRegion)region);
      } else {
        break;
      }
    } while (System.currentTimeMillis() - start < 60000);
    
    wc = new WaitCriterion() {
      public boolean done() {
        return (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == 4);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
//    assertEquals(4/*numBuckets*/, prMetaData.getBucketServerLocationsMap_TEST_ONLY().size());    
  }

  public void testMetadataFetchOnlyThroughputAll() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port3 = (Integer)member3.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    createClient(port0, port1, port2, port3);
    putAll();
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return (regionMetaData.size() == 1);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
    
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    
    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath()); 
    wc = new WaitCriterion() {
      public boolean done() {
        return (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == 4);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);  
  }
  
  
  
  @Ignore("Bug 47950")
  public void DISABLED_testMetadataIsSameOnAllServersAndClients() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    Integer port3 = (Integer)member3.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 3, 4 });
    createClient(port0, port1, port2, port3);
    put();
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    
    
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion)region);
    
    Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
    assertEquals(1, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap  = prMetaData.getBucketServerLocationsMap_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return (clientMap.size() == 4);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true); 
    for (Entry entry : clientMap.entrySet()) {
      assertEquals(4, ((List)entry.getValue()).size());
    }
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopServer");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopServer");
    
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "startServerOnPort",  new Object[] { port0 });
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "startServerOnPort",  new Object[] { port1 });
    put();
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    
    cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    
    wc = new WaitCriterion() {
      public boolean done() {
        ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();    
        Map<String, ClientPartitionAdvisor>  regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
        assertEquals(1, regionMetaData.size());
        assertTrue(regionMetaData.containsKey(region.getFullPath()));    
        ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
        Map<Integer, List<BucketServerLocation66>> clientMap  = prMetaData.getBucketServerLocationsMap_TEST_ONLY();
        assertEquals(4/*numBuckets*/, clientMap.size());
        boolean finished = true;
        for (Entry entry : clientMap.entrySet()) {
          List list = (List)entry.getValue();
          if(list.size()<4){
            LogWriterUtils.getLogWriter().info("still waiting for 4 bucket owners in " + entry.getKey() + ": " + list);
            finished = false;
            break;
          }
        }
        return finished;
      }
      public String description() {
        return "bucket copies are not created";
      }
    };
    Wait.waitForCriterion(wc, 60000, 400, true);
    cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion)region);
    
    regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
    assertEquals(1, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    
    prMetaData = regionMetaData.get(region.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap2  = prMetaData.getBucketServerLocationsMap_TEST_ONLY();
    wc = new WaitCriterion() {
      public boolean done() {
        return (clientMap2.size() == 4);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true); 
    for (Entry entry : clientMap.entrySet()) {
      assertEquals(4, ((List)entry.getValue()).size());
    }
    
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "closeCache");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "closeCache");
    
//    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "createServerOnPort",  new Object[] {3,4,port0 });
//    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "createServerOnPort",  new Object[] {3,4,port1 });
    put();
    member2.invoke(new CacheSerializableRunnable("aba") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion)region;
        ConcurrentHashMap<Integer, Set<ServerBucketProfile>> serverMap = pr
            .getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });
    
    member3.invoke(new CacheSerializableRunnable("aba") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion)region;
        ConcurrentHashMap<Integer, Set<ServerBucketProfile>> serverMap = pr
            .getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });
    
//    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
//    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    
    cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion)region);
    
    regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
    assertEquals(1, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    
    prMetaData = regionMetaData.get(region.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap3  = prMetaData.getBucketServerLocationsMap_TEST_ONLY();
    wc = new WaitCriterion() {
      public boolean done() {
        return (clientMap3.size() == 4);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true); 
    for (Entry entry : clientMap.entrySet()) {
      assertEquals(2, ((List)entry.getValue()).size());
    }
    final Map<Integer, List<BucketServerLocation66>> fclientMap = clientMap;
    Wait.waitForCriterion(new WaitCriterion() {

      public boolean done() {
        try {
          //member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{fclientMap});
          //member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{fclientMap});
          member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{fclientMap});
          member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{fclientMap});
        } catch (Exception e) {
          LogWriterUtils.getLogWriter().info("verification failed", e);
          return false;
        }
        return true;
      }

      public String description() {
        return "verification of metadata on all members";
      }
      
    }, 20000, 2000, true);
  }
  
  public void testMetadataIsSameOnAllServersAndClientsHA() {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 2, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createServer",
        new Object[] { 2, 4 });
    
    createClient(port0, port1, port0, port1);
    put();
    
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion)region);
    
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return (regionMetaData.size() == 1);
      }

      public String description() {
        return "expected metadata is ready";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
    
    assertEquals(1, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    
    member0.invoke(new CacheSerializableRunnable("aba") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion)region;
        ConcurrentHashMap<Integer, Set<ServerBucketProfile>> serverMap = pr
            .getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });
    
    member1.invoke(new CacheSerializableRunnable("aba") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion)region;
        ConcurrentHashMap<Integer, Set<ServerBucketProfile>> serverMap = pr
            .getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });
    
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap  = prMetaData.getBucketServerLocationsMap_TEST_ONLY();
    wc = new WaitCriterion() {
      public boolean done() {
        return (clientMap.size() == 4);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true); 
    for (Entry entry : clientMap.entrySet()) {
      assertEquals(2, ((List)entry.getValue()).size());
    }
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "stopServer");
    
    put();
    
    cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion)region);
    
    //member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "verifyMetadata", new Object[]{clientMap});
    
    assertEquals(4/*numBuckets*/, clientMap.size());
    for (Entry entry : clientMap.entrySet()) {
      assertEquals(1, ((List)entry.getValue()).size());
    }
    
    wc = new WaitCriterion() {
      private int bucketId;
      private int size;
      List globalList = new ArrayList();
      public boolean done() {
        ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();    
        Map<String, ClientPartitionAdvisor>  regionMetaData = cms.getClientPRMetadata_TEST_ONLY();    
        assertEquals(1, regionMetaData.size());
        assertTrue(regionMetaData.containsKey(region.getFullPath()));    
        ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
        Map<Integer, List<BucketServerLocation66>> clientMap  = prMetaData.getBucketServerLocationsMap_TEST_ONLY();
        assertEquals(4/*numBuckets*/, clientMap.size());
        boolean finished = true;
        for (Entry entry : clientMap.entrySet()) {
          List list = (List)entry.getValue();
          if(list.size() != 1){
            size = list.size();
            globalList = list;
            bucketId = (Integer)entry.getKey();
            finished = false;
          }
        }
        return finished;
      }
      public String description() {
        return "bucket copies are not created, the locations size for bucket id : "
            + bucketId + " size : " + size + " the list is " + globalList;
      }
    };
    Wait.waitForCriterion(wc, 60000, 400, true);
  }

  //TODO This is failing in WAN_Dev_Dec11 branch after downmerge from trunk revision 34709
  
  // This test is disabled due to intermittent failures in unit test runs
  // Please see ticket #52203
  public void disabledtestClientMetadataForPersistentPrs() throws Exception {
    Integer port0 = (Integer)member0.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServer",
        new Object[] { 3, 4 });
    Integer port1 = (Integer)member1.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServer",
        new Object[] { 3, 4 });
    Integer port2 = (Integer)member2.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServer",
        new Object[] { 3, 4 });
    Integer port3 = (Integer)member3.invoke(
        PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServer",
        new Object[] { 3, 4 });
    
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "putIntoPartitionedRegions");
    
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "waitForLocalBucketsCreation", new Object[]{4});
    
    createClient(port0, port1, port2, port3);
    fetchAndValidateMetadata();
    member0.invoke(PartitionedRegionSingleHopDUnitTest.class, "closeCache");
    member1.invoke(PartitionedRegionSingleHopDUnitTest.class, "closeCache");
    member2.invoke(PartitionedRegionSingleHopDUnitTest.class, "closeCache");
    member3.invoke(PartitionedRegionSingleHopDUnitTest.class, "closeCache");
    Wait.pause(1000); //let client detect that servers are dead through ping
    AsyncInvocation m3 = member3.invokeAsync(PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServerOnPort",new Object[] { 3, 4,port3 });
    AsyncInvocation m2 = member2.invokeAsync(PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServerOnPort",new Object[] { 3, 4,port2 });
    AsyncInvocation m1 = member1.invokeAsync(PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServerOnPort",new Object[] { 3, 4,port1 });
    AsyncInvocation m0 = member0.invokeAsync(PartitionedRegionSingleHopDUnitTest.class, "createPersistentPrsAndServerOnPort",new Object[] { 3, 4,port0 });
    m3.join();
    m2.join();
    m1.join();
    m0.join();
    fetchAndValidateMetadata();
  }
  
  
  private void fetchAndValidateMetadata() {
    ClientMetadataService service = ((GemFireCacheImpl)this.cache)
        .getClientMetadataService();
    service.getClientPRMetadata((LocalRegion)this.region);
    HashMap<ServerLocation, HashSet<Integer>> servers = service
        .groupByServerToAllBuckets(this.region, true);
    if (servers == null) {
      fail("The client metadata contains no servers for region "
          + this.region.getFullPath());
    }
    else {
      region.getCache().getLogger().fine("The client metadata contains the following "
          + servers.size() + " servers for region " + this.region.getFullPath()
          + ":");
      for (Map.Entry entry : servers.entrySet()) {
        region.getCache().getLogger().fine(entry.getKey() + "->" + entry.getValue());
      }
      if (servers.size() < 4) {
        fail("Servers size is " +servers.size() + " less than expected 4.");
      }
    }   
  }
  
  public static void verifyMetadata(Map<Integer, List<BucketServerLocation66>> clientMap){
    final PartitionedRegion pr = (PartitionedRegion)region;
    ConcurrentHashMap<Integer, Set<ServerBucketProfile>> serverMap = pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    assertEquals(clientMap.size(), serverMap.size());
    assertTrue(clientMap.keySet().containsAll(serverMap.keySet()));    
    for(Map.Entry<Integer, List<BucketServerLocation66>> entry : clientMap.entrySet()){
      int bucketId = entry.getKey();
      List<BucketServerLocation66> list = entry.getValue();
      BucketServerLocation66 primaryBSL = null;
      int primaryCnt = 0;
      for(BucketServerLocation66 bsl : list){
        if(bsl.isPrimary()){
          primaryBSL = bsl;
          primaryCnt++;
        }
      }
      assertTrue(primaryCnt==1);
      Set<ServerBucketProfile> set = serverMap.get(bucketId);
      assertEquals(list.size(), set.size());
      primaryCnt=0;
      for(ServerBucketProfile bp : set){
        ServerLocation sl = (ServerLocation)bp.bucketServerLocations.toArray()[0];
        assertTrue(list.contains(sl));
        // should be only one primary
        if(bp.isPrimary){
          primaryCnt++;
          assertTrue(primaryBSL.equals(sl));
        }
      }
      assertTrue(primaryCnt==1);
    }
  }
  
  public static void waitForLocalBucketsCreation(final int numBuckets){
    final PartitionedRegion pr = (PartitionedRegion)region;
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (pr.getDataStore().getAllLocalBuckets().size() == numBuckets) {
          return true;
        }
        return false;
      }

      public String description() {
        return "bucket copies are not created, the total number of buckets expected are "
            + numBuckets
            + " but the total num of buckets are "
            + pr.getDataStore().getAllLocalBuckets().size();
      }
    };
    Wait.waitForCriterion(wc, 60000, 400, true);
  }
  
  public static void waitForBucketsCreation(){
    final PartitionedRegion pr = (PartitionedRegion)region;
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (pr.getDataStore().getAllLocalBuckets().size() == 2) {
          return true;
        }
        return false;
      }
      public String description() {
        return "bucket copies are not created";
      }
    };
    Wait.waitForCriterion(wc, 60000, 400, true);
  }
  
  private void verifyDeadServer(Map<String, ClientPartitionAdvisor> regionMetaData, Region region, int port0, int port1) {
    
    ServerLocation sl0 = new ServerLocation("localhost", port0);
    ServerLocation sl1 = new ServerLocation("localhost", port1);
    
    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath()); 
    
    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      List servers = (List)entry.getValue();
      assertFalse( servers.contains(sl0) );
      assertFalse( servers.contains(sl1) );
    }
  }
  
  public static void createClientWithoutPRSingleHopEnabled(int port0) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0)
          .setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).setPRSingleHopEnabled(false).create(PR_NAME);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }
  
  public static int createAccessorServer() {
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4).setLocalMaxMemory(0);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    region = cache.createRegion(PR_NAME, attr.create());

    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4).setLocalMaxMemory(0)
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4).setLocalMaxMemory(0)
        .setColocatedWith("CUSTOMER").setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4).setLocalMaxMemory(0)
        .setColocatedWith("ORDER").setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    replicatedRegion = cache.createRegion("rr", new AttributesFactory().create());
    return port;
  }

  public static int createServer(int redundantCopies, int totalNoofBuckets) {
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());    
    
    replicatedRegion = cache.createRegion("rr", new AttributesFactory().create());

    return port;
  }
  
  public static int createPersistentPrsAndServer(int redundantCopies, int totalNoofBuckets) {
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    cache = test.getCache();
    DiskStore disk = cache.findDiskStore("disk");
    if(disk == null) {
      disk = cache.createDiskStoreFactory()
      .setDiskDirs(getDiskDirs()).create("disk");
    }
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());    
    
    replicatedRegion = cache.createRegion("rr", new AttributesFactory().create());
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }
    return port;
  }
  public static int createPersistentPrsAndServerOnPort(int redundantCopies, int totalNoofBuckets, int port) {
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    cache = test.getCache();
    DiskStore disk = cache.findDiskStore("disk");
    if(disk == null) {
      disk = cache.createDiskStoreFactory()
      .setDiskDirs(getDiskDirs()).create("disk");
    }
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attr.setDiskStoreName("disk");
    attr.setPartitionAttributes(paf.create());
//    attr.setConcurrencyChecksEnabled(true);
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());    
    
    replicatedRegion = cache.createRegion("rr", new AttributesFactory().create());
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }
    return port;
  }
  public static void createServerOnPort(int redundantCopies, int totalNoofBuckets, int port) {
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());    
    
    replicatedRegion = cache.createRegion("rr", new AttributesFactory().create());

  }
  
  public static void startServerOnPort(int port) {
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }
  }
  

  public static void createPeer() {
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    cache = test.getCache();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4).setPartitionResolver(
        new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4)
        .setColocatedWith("CUSTOMER").setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region ORDER created Successfully :"
            + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(4).setColocatedWith("ORDER")
        .setPartitionResolver(
            new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    replicatedRegion = cache.createRegion("rr", new AttributesFactory().create());
  }

  public static void createClient(int port0) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0)
          .setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  public static void createClient(int port0, int port1) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer(
          "localhost", port1).setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  public static void createClientWithLocator(String host, int port0) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create(PR_NAME);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  public static void createClient(int port0, int port1, int port2, int port3) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PartitionedRegionSingleHopDUnitTest(
        "PartitionedRegionSingleHopDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer(
          "localhost", port1).addServer("localhost", port2).addServer(
          "localhost", port3).setPingInterval(100).setSubscriptionEnabled(false).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }
  
  private static void createRegionsInClientCache(String poolName) {
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(poolName);
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(PR_NAME, attrs);
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Distributed Region " + PR_NAME + " created Successfully :"
            + region.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    attrs = factory.create();
    customerRegion = cache.createRegion("CUSTOMER", attrs);
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter().info(
        "Distributed Region CUSTOMER created Successfully :"
            + customerRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    attrs = factory.create();
    orderRegion = cache.createRegion("ORDER", attrs);
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter().info(
        "Distributed Region ORDER created Successfully :"
            + orderRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    attrs = factory.create();
    shipmentRegion = cache.createRegion("SHIPMENT", attrs);
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter().info(
        "Distributed Region SHIPMENT created Successfully :"
            + shipmentRegion.toString());
    factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    factory.setPoolName(poolName);
    replicatedRegion = cache.createRegion("rr", factory.create());
  }
  
  public static void putIntoPartitionedRegions() {
    for (int i = 0; i <= 3; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        orderRegion.put(orderId, order);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.put(shipmentId, shipment);
        }
      }
    }

    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");

    region.put(new Integer(0), "update0");
    region.put(new Integer(1), "update1");
    region.put(new Integer(2), "update2");
    region.put(new Integer(3), "update3");

    region.put(new Integer(0), "update00");
    region.put(new Integer(1), "update11");
    region.put(new Integer(2), "update22");
    region.put(new Integer(3), "update33");
    Map map = new HashMap();
    map.put(1, 1);
    replicatedRegion.putAll(map);
  }

  static class MyFunctionAdapter extends FunctionAdapter {
    public String getId() {
      return "fid";
    }

    public void execute(FunctionContext context) {
      System.out.println("YOGS function called");
      RegionFunctionContext rc = (RegionFunctionContext)context;
      Region r = rc.getDataSet();
      Set filter = rc.getFilter();
      if (rc.getFilter() == null) {
        for (int i = 0; i < 200; i++) {
          r.put(i, i);
        }
      }
      else {
        for (Object key : filter) {
          r.put(key, key);
        }
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }
  }
  
  public static void executeFunctions() {
    Set filter = new HashSet();
    filter.add(0);
    FunctionService.onRegion(region).withFilter(filter).execute(new MyFunctionAdapter()).getResult();
    filter.add(1);
    FunctionService.onRegion(region).withFilter(filter).execute(new MyFunctionAdapter()).getResult();
    filter.add(2);
    filter.add(3);
    FunctionService.onRegion(region).withFilter(filter).execute(new MyFunctionAdapter()).getResult();
    FunctionService.onRegion(region).execute(new MyFunctionAdapter()).getResult();
  }
  
  public static void putAll() {
    Map map = new HashMap();
    map.put(0, 0);
    map.put(1, 1);
    map.put(2, 2);
    map.put(3, 3);
    region.putAll(map, "putAllCallback");
    region.putAll(map);
    region.putAll(map);
    region.putAll(map);
  }  
  
  public static void put() {

    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    for(int i=0;i<40;i++){
      region.put(new Integer(i), "create"+i);
    }
/*    pause(2000);
    region.put(new Integer(0), "update0");
    region.put(new Integer(1), "update1");
    region.put(new Integer(2), "update2");
    region.put(new Integer(3), "update3");
    pause(2000);
    region.put(new Integer(0), "update00");
    region.put(new Integer(1), "update11");
    region.put(new Integer(2), "update22");
    region.put(new Integer(3), "update33");
*/  }  

  public static void getFromPartitionedRegions() {
    for (int i = 0; i <= 3; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.get(custid, customer);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        orderRegion.get(orderId, order);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.get(shipmentId, shipment);
        }
      }

    }

    region.get(new Integer(0), "create0");
    region.get(new Integer(1), "create1");
    region.get(new Integer(2), "create2");
    region.get(new Integer(3), "create3");

    region.get(new Integer(0), "update0");
    region.get(new Integer(1), "update1");
    region.get(new Integer(2), "update2");
    region.get(new Integer(3), "update3");

    region.get(new Integer(0), "update00");
    region.get(new Integer(1), "update11");
    region.get(new Integer(2), "update22");
    region.get(new Integer(3), "update33");
  }

  public static void putIntoSinglePR() {
    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
  }

  public static void getDataFromSinglePR() {
    for (int i = 0; i < 10; i++) {
      region.get(new Integer(0));
      region.get(new Integer(1));
      region.get(new Integer(2));
      region.get(new Integer(3));
    }
  }

  public static void updateIntoSinglePR() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(new Integer(0), "update0");
    assertEquals(false, cms.isRefreshMetadataTestOnly());
    

    region.put(new Integer(1), "update1");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(new Integer(2), "update2");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(new Integer(3), "update3");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(new Integer(0), "update00");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(new Integer(1), "update11");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(new Integer(2), "update22");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(new Integer(3), "update33");
    assertEquals(false, cms.isRefreshMetadataTestOnly());
  }

  public static void verifyEmptyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    assertTrue(cms.getClientPRMetadata_TEST_ONLY().isEmpty());
  }

  public static void verifyEmptyStaticData() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    assertTrue(cms
        .getClientPartitionAttributesMap().isEmpty());
  }

  public static void verifyNonEmptyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    assertTrue(!cms.getClientPRMetadata_TEST_ONLY().isEmpty());
    assertTrue(!cms.getClientPartitionAttributesMap().isEmpty());
  }

  public static void printMetadata() {
    if (cache != null) {
      ClientMetadataService cms = ((GemFireCacheImpl)cache)
          .getClientMetadataService();
      ((GemFireCacheImpl)cache).getLogger().info(
          "Metadata is " + cms.getClientPRMetadata_TEST_ONLY());
    }
  }

  public static void printView() {
    PartitionedRegion pr = (PartitionedRegion)region;
    if(pr.cache!=null )
    {
      ((GemFireCacheImpl)cache).getLogger().info(
          "Primary Bucket view of server0  "
          + pr.getDataStore().getLocalPrimaryBucketsListTestOnly());
      ((GemFireCacheImpl)cache).getLogger().info("Secondary Bucket view of server0  "
          + pr.getDataStore().getLocalNonPrimaryBucketsListTestOnly());
    }
  }

  private void verifyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms
        .getClientPRMetadata_TEST_ONLY();
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return (regionMetaData.size() == 4);
      }

      public String description() {
        return "expected PRAdvisor to be ready";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
    
    assertEquals(4, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    wc = new WaitCriterion() {
      public boolean done() {
        return (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == 4);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    Wait.waitForCriterion(wc, 60000, 1000, true);
  }
}

class Customer implements DataSerializable {
  String name;

  String address;

  public Customer() {

  }

  public Customer(String name, String address) {
    this.name = name;
    this.address = address;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.address = DataSerializer.readString(in);

  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeString(this.address, out);
  }

  public String toString() {
    return "Customer { name=" + this.name + " address=" + this.address + "}";
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof Customer))
      return false;

    Customer cust = (Customer)o;
    return (cust.name.equals(name) && cust.address.equals(address));
  }
}

class Order implements DataSerializable {
  String orderName;

  public Order() {

  }

  public Order(String orderName) {
    this.orderName = orderName;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.orderName = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.orderName, out);
  }

  public String toString() {
    return this.orderName;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;

    if (obj instanceof Order) {
      Order other = (Order)obj;
      if (other.orderName != null && other.orderName.equals(this.orderName)) {
        return true;
      }
    }
    return false;
  }
}

class Shipment implements DataSerializable {
  String shipmentName;

  public Shipment() {

  }

  public Shipment(String shipmentName) {
    this.shipmentName = shipmentName;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.shipmentName = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.shipmentName, out);
  }

  public String toString() {
    return this.shipmentName;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;

    if (obj instanceof Shipment) {
      Shipment other = (Shipment)obj;
      if (other.shipmentName != null
          && other.shipmentName.equals(this.shipmentName)) {
        return true;
      }
    }
    return false;
  }
}
