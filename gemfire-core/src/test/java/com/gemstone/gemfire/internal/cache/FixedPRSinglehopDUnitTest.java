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

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.client.internal.ClientPartitionAdvisor;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.QuarterPartitionResolver;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.FixedPartitioningTestBase.Q1_Months;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

public class FixedPRSinglehopDUnitTest extends CacheTestCase {

  private static final long serialVersionUID = 1L;

  private static final String PR_NAME = "fixed_single_hop_pr";

  private static Cache cache = null;

  private static Locator locator = null;

  private static Region region = null;

  private static final Date q1dateJan1 = new Date(2010, 0, 1);

  private static final Date q1dateFeb1 = new Date(2010, 1, 1);
  
  private static final Date q1dateMar1 = new Date(2010, 2, 1);
  
  private static final Date q2dateApr1 = new Date(2010, 3, 1);
  
  private static final Date q2dateMay1 = new Date(2010, 4, 1);
  
  private static final Date q2dateJun1 = new Date(2010, 5, 1);

  private static final Date q3dateJuly1 = new Date(2010, 6, 1);
  
  private static final Date q3dateAug1 = new Date(2010, 7, 1);

  private static final Date q3dateSep1 = new Date(2010, 8, 1);
  
  private static final Date q4dateOct1 = new Date(2010, 9, 1);
  
  private static final Date q4dateNov1 = new Date(2010, 10, 1);
  
  private static final Date q4dateDec1 = new Date(2010, 11, 1);
  
  public FixedPRSinglehopDUnitTest(String name) {
    super(name);
  }

  public void testNoClientConnected() {
    final Host host = Host.getHost(0);
    VM accessorServer = host.getVM(0);
    VM datastoreServer = host.getVM(1);
    VM peer1 = host.getVM(2);
    VM peer2 = host.getVM(3);

    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));

    datastoreServer.invoke(FixedPRSinglehopDUnitTest.class, "createServer",
        new Object[] { false, fpaList });

    fpaList.clear();
    accessorServer.invoke(FixedPRSinglehopDUnitTest.class, "createServer",
        new Object[] { true, fpaList });

    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", 3));

    peer1.invoke(FixedPRSinglehopDUnitTest.class, "createPeer", new Object[] {
        false, fpaList });

    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));

    peer2.invoke(FixedPRSinglehopDUnitTest.class, "createPeer", new Object[] {
        false, fpaList });

    datastoreServer.invoke(FixedPRSinglehopDUnitTest.class,
        "putIntoPartitionedRegions");
    accessorServer.invoke(FixedPRSinglehopDUnitTest.class,
        "putIntoPartitionedRegions");
    peer1.invoke(FixedPRSinglehopDUnitTest.class, "putIntoPartitionedRegions");
    peer2.invoke(FixedPRSinglehopDUnitTest.class, "putIntoPartitionedRegions");

    datastoreServer.invoke(FixedPRSinglehopDUnitTest.class,
        "getFromPartitionedRegions");
    accessorServer.invoke(FixedPRSinglehopDUnitTest.class,
        "getFromPartitionedRegions");
    peer1.invoke(FixedPRSinglehopDUnitTest.class, "getFromPartitionedRegions");
    peer2.invoke(FixedPRSinglehopDUnitTest.class, "getFromPartitionedRegions");

    datastoreServer.invoke(FixedPRSinglehopDUnitTest.class,
        "verifyEmptyMetadata");
    accessorServer.invoke(FixedPRSinglehopDUnitTest.class,
        "verifyEmptyMetadata");
    peer1.invoke(FixedPRSinglehopDUnitTest.class, "verifyEmptyMetadata");
    peer2.invoke(FixedPRSinglehopDUnitTest.class, "verifyEmptyMetadata");

    datastoreServer.invoke(FixedPRSinglehopDUnitTest.class,
        "verifyEmptyStaticData");
    accessorServer.invoke(FixedPRSinglehopDUnitTest.class,
        "verifyEmptyStaticData");
    peer1.invoke(FixedPRSinglehopDUnitTest.class, "verifyEmptyStaticData");
    peer2.invoke(FixedPRSinglehopDUnitTest.class, "verifyEmptyStaticData");
  }

  // 2 AccessorServers, 2 Peers
  // 1 Client connected to 2 AccessorServers. Hence metadata should not be
  // fetched.
  public void testClientConnectedToAccessors() {
    final Host host = Host.getHost(0);
    VM accessorServer1 = host.getVM(0);
    VM accessorServer2 = host.getVM(1);
    VM peer1 = host.getVM(2);
    VM peer2 = host.getVM(3);

    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();

    Integer port0 = (Integer)accessorServer1.invoke(
        FixedPRSinglehopDUnitTest.class, "createServer", new Object[] { true,
            fpaList });

    Integer port1 = (Integer)accessorServer2.invoke(
        FixedPRSinglehopDUnitTest.class, "createServer", new Object[] { true,
            fpaList });

    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));

    peer1.invoke(FixedPRSinglehopDUnitTest.class, "createPeer", new Object[] {
        false, fpaList });
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));

    peer2.invoke(FixedPRSinglehopDUnitTest.class, "createPeer", new Object[] {
        false, fpaList });

    createClient(port0, port1);

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
    
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);
    
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", false, 3));

    Integer port1 = (Integer)server1.invoke(
        FixedPRSinglehopDUnitTest.class, "createServer", new Object[] { false,
            fpaList });
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", false, 3));

    Integer port2 = (Integer)server2.invoke(
        FixedPRSinglehopDUnitTest.class, "createServer", new Object[] { false,
            fpaList });
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", false, 3));
    
    Integer port3 = (Integer)server3.invoke(
        FixedPRSinglehopDUnitTest.class, "createServer", new Object[] { false,
            fpaList });
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", false, 3));
    
    Integer port4 = (Integer)server4.invoke(
        FixedPRSinglehopDUnitTest.class, "createServer", new Object[] { false,
            fpaList });    

    createClient(port1, port2, port3, port4);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    server1.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    server2.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    server3.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    server4.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    
    int totalBucketOnServer = 0;
    totalBucketOnServer += (Integer)server1.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    totalBucketOnServer += (Integer)server2.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    totalBucketOnServer += (Integer)server3.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    totalBucketOnServer += (Integer)server4.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    
    verifyMetadata(totalBucketOnServer,2);
    updateIntoSinglePR();
  }  
  
  /**
   * This test will check to see if all the partitionAttributes are sent to the client.
   * In case one partition comes late, we should fetch that when there is a network hop because
   * of that partitioned region.
   * This test will create 3 servers with partition. Do some operations on them. Validate that
   * the metadata are fetched and then later up one more partition and do some operations on them. It should
   * fetch new fpa. 
   */
  public void test_FPAmetadataFetch() {
    
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);
    Boolean simpleFPR = false;
    final int portLocator = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String hostLocator = getServerHostName(server1.getHost());
    final String locator = hostLocator + "[" + portLocator + "]";
    server3.invoke(FixedPRSinglehopDUnitTest.class,
        "startLocatorInVM", new Object[] { portLocator });
    try {
    
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", false, 3));

    Integer port1 = (Integer)server1.invoke(
        FixedPRSinglehopDUnitTest.class, "createServerWithLocator", new Object[] { locator, false,
            fpaList,  simpleFPR});
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));

    Integer port2 = (Integer)server2.invoke(
        FixedPRSinglehopDUnitTest.class, "createServerWithLocator", new Object[] { locator, false,
            fpaList , simpleFPR});
    fpaList.clear();
    
    createClientWithLocator(hostLocator, portLocator);
    
    putIntoPartitionedRegionsThreeQs();

    getFromPartitionedRegionsFor3Qs();
    pause(2000);
    // TODO: Verify that all the fpa's are in the map
    server1.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    server2.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    
    int totalBucketOnServer = 0;
    totalBucketOnServer += (Integer)server1.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    totalBucketOnServer += (Integer)server2.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    int currentRedundancy = 1;
    verifyMetadata(totalBucketOnServer,currentRedundancy);
    updateIntoSinglePRFor3Qs();
    
    // now create one more partition
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", false, 3));
  
    Integer port4 = (Integer)server4.invoke(
        FixedPRSinglehopDUnitTest.class, "createServerWithLocator", new Object[] { locator, false,
            fpaList, simpleFPR });    
    
    pause(2000);
    putIntoPartitionedRegions();
    // Client should get the new partition
    // TODO: Verify that

    getFromPartitionedRegions();
    pause(2000);
    server1.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    server2.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    server4.invoke(FixedPRSinglehopDUnitTest.class, "printView");
    
    totalBucketOnServer = 0;
    totalBucketOnServer += (Integer)server1.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    totalBucketOnServer += (Integer)server2.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    totalBucketOnServer += (Integer)server4.invoke(FixedPRSinglehopDUnitTest.class, "totalNumBucketsCreated");
    
    updateIntoSinglePR();
    } finally {
    server3.invoke(FixedPRSinglehopDUnitTest.class, "stopLocator"); 
    }
  }
  
  public static int createServer(boolean isAccessor,
      List<FixedPartitionAttributes> fpaList) {

    CacheTestCase test = new FixedPRSinglehopDUnitTest(
        "FixedPRSinglehopDUnitTest");
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      fail("Failed to start server ", e);
    }
    
    if (!fpaList.isEmpty() || isAccessor) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setTotalNumBuckets(12);
      if (isAccessor) {
        paf.setLocalMaxMemory(0);
      }
      for (FixedPartitionAttributes fpa : fpaList) {
        paf.addFixedPartitionAttributes(fpa);
      }
      paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      AttributesFactory attr = new AttributesFactory();
      attr.setPartitionAttributes(paf.create());
      region = cache.createRegion(PR_NAME, attr.create());
      assertNotNull(region);
      getLogWriter().info(
          "Partitioned Region " + PR_NAME + " created Successfully :"
              + region.toString());
    }
    return port;
  }

  
  public static int createServerWithLocator(String locator, boolean isAccessor,
      List<FixedPartitionAttributes> fpaList, boolean simpleFPR) {

    CacheTestCase test = new FixedPRSinglehopDUnitTest(
    "FixedPRSinglehopDUnitTest");
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("locators", locator);
    DistributedSystem ds = test.getSystem(props);
    cache = new CacheFactory(props).create(ds);
    
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    }
    catch (IOException e) {
      fail("Failed to start server ", e);
    }
    
    if (!fpaList.isEmpty() || isAccessor) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setTotalNumBuckets(12);
      if (isAccessor) {
        paf.setLocalMaxMemory(0);
      }
      for (FixedPartitionAttributes fpa : fpaList) {
        paf.addFixedPartitionAttributes(fpa);
      }
      //paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());
      paf.setPartitionResolver(new QuarterPartitionResolver());

      AttributesFactory attr = new AttributesFactory();
      attr.setPartitionAttributes(paf.create());
      region = cache.createRegion(PR_NAME, attr.create());
      assertNotNull(region);
      getLogWriter().info(
          "Partitioned Region " + PR_NAME + " created Successfully :"
              + region.toString());
    }
    return port;
  }

  public static void startLocatorInVM(final int locatorPort) {

    File logFile = new File("locator-" + locatorPort + ".log");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    try {
      locator = Locator.startLocatorAndDS(locatorPort, logFile, null, props);
    }
    catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void stopLocator() {
    locator.stop();
  }
  
  public static int totalNumBucketsCreated () {
    CacheTestCase test = new FixedPRSinglehopDUnitTest(
    "FixedPRSinglehopDUnitTest");
    PartitionedRegion pr = (PartitionedRegion)cache.getRegion(PR_NAME);
    assertNotNull(pr);
    return pr.getLocalPrimaryBucketsListTestOnly().size();
  }
  
  public static void createPeer(boolean isAccessor,
      List<FixedPartitionAttributes> fpaList) {
    CacheTestCase test = new FixedPRSinglehopDUnitTest(
        "FixedPRSinglehopDUnitTest");
    cache = test.getCache();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(12);
    if(isAccessor){
      paf.setLocalMaxMemory(0);
    }
    for (FixedPartitionAttributes fpa : fpaList) {
      paf.addFixedPartitionAttributes(fpa);
    }
    paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());

    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    getLogWriter().info(
        "Partitioned Region " + PR_NAME + " created Successfully :"
            + region.toString());

  }

  public static void createClient(int port0) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new FixedPRSinglehopDUnitTest(
        "FixedPRSinglehopDUnitTest");
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
    CacheTestCase test = new FixedPRSinglehopDUnitTest(
        "FixedPRSinglehopDUnitTest");
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
    CacheTestCase test = new FixedPRSinglehopDUnitTest(
        "FixedPRSinglehopDUnitTest");
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
    CacheTestCase test = new FixedPRSinglehopDUnitTest(
        "FixedPRSinglehopDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer(
          "localhost", port1).addServer("localhost", port2).addServer(
          "localhost", port3).setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
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
    getLogWriter().info(
        "Distributed Region " + PR_NAME + " created Successfully :"
            + region.toString());
  }

  public static void putIntoPartitionedRegions() {
    region.put(q1dateJan1, "create0");
    region.put(q2dateApr1, "create1");
    region.put(q3dateJuly1, "create2");
    region.put(q4dateOct1, "create3");    
    region.put(q1dateFeb1, "create4");
    region.put(q2dateMay1, "create5");
    region.put(q3dateAug1, "create6");
    region.put(q4dateNov1, "create7");
    region.put(q1dateMar1, "create8");
    region.put(q2dateJun1, "create9");
    region.put(q3dateSep1, "create10");
    region.put(q4dateDec1, "create11");
    
    region.put(q1dateJan1, "update0");
    region.put(q2dateApr1, "update1");
    region.put(q3dateJuly1, "update2");
    region.put(q4dateOct1, "update3");    
    region.put(q1dateFeb1, "update4");
    region.put(q2dateMay1, "update5");
    region.put(q3dateAug1, "update6");
    region.put(q4dateNov1, "update7");
    region.put(q1dateMar1, "update8");
    region.put(q2dateJun1, "update9");
    region.put(q3dateSep1, "update10");
    region.put(q4dateDec1, "update11");
    
    
    region.put(q1dateJan1, "update00");
    region.put(q2dateApr1, "update11");
    region.put(q3dateJuly1, "update22");
    region.put(q4dateOct1, "update33");    
    region.put(q1dateFeb1, "update44");
    region.put(q2dateMay1, "update55");
    region.put(q3dateAug1, "update66");
    region.put(q4dateNov1, "update77");
    region.put(q1dateMar1, "update88");
    region.put(q2dateJun1, "update99");
    region.put(q3dateSep1, "update1010");
    region.put(q4dateDec1, "update1111");
    
  }

  public static void putIntoPartitionedRegionsThreeQs() {
    region.put(q1dateJan1, "create0");
    region.put(q2dateApr1, "create1");
    region.put(q3dateJuly1, "create2");
        
    region.put(q1dateFeb1, "create4");
    region.put(q2dateMay1, "create5");
    region.put(q3dateAug1, "create6");
    region.put(q1dateMar1, "create8");
    region.put(q2dateJun1, "create9");
    region.put(q3dateSep1, "create10");
    
    region.put(q1dateJan1, "update0");
    region.put(q2dateApr1, "update1");
    region.put(q3dateJuly1, "update2");
    region.put(q1dateFeb1, "update4");
    region.put(q2dateMay1, "update5");
    region.put(q3dateAug1, "update6");
    region.put(q1dateMar1, "update8");
    region.put(q2dateJun1, "update9");
    region.put(q3dateSep1, "update10");
    
    region.put(q1dateJan1, "update00");
    region.put(q2dateApr1, "update11");
    region.put(q3dateJuly1, "update22");
    region.put(q1dateFeb1, "update44");
    region.put(q2dateMay1, "update55");
    region.put(q3dateAug1, "update66");
    region.put(q1dateMar1, "update88");
    region.put(q2dateJun1, "update99");
    region.put(q3dateSep1, "update1010");
  }
  
  public static void getFromPartitionedRegions() {
    region.get(q1dateJan1, "create0");
    region.get(q2dateApr1, "create1");
    region.get(q3dateJuly1, "create2");
    region.get(q4dateOct1, "create3");

    region.get(q1dateJan1, "update0");
    region.get(q2dateApr1, "update1");
    region.get(q3dateJuly1, "update2");
    region.get(q4dateOct1, "update3");

    region.get(q1dateJan1, "update00");
    region.get(q2dateApr1, "update11");
    region.get(q3dateJuly1, "update22");
    region.get(q4dateOct1, "update33");
  }

  public static void getFromPartitionedRegionsFor3Qs() {
    region.get(q1dateJan1, "create0");
    region.get(q2dateApr1, "create1");
    region.get(q3dateJuly1, "create2");

    region.get(q1dateJan1, "update0");
    region.get(q2dateApr1, "update1");
    region.get(q3dateJuly1, "update2");

    region.get(q1dateJan1, "update00");
    region.get(q2dateApr1, "update11");
    region.get(q3dateJuly1, "update22");
  }
  
  public static void putIntoSinglePR() {
    region.put(q1dateJan1, "create0");
    region.put(q2dateApr1, "create1");
    region.put(q3dateJuly1, "create2");
    region.put(q4dateOct1, "create3");
  }

  public static void getDataFromSinglePR() {
    for (int i = 0; i < 10; i++) {
      region.get(q1dateJan1);
      region.get(q2dateApr1);
      region.get(q3dateJuly1);
      region.get(q4dateOct1);
    }
  }

  public static void updateIntoSinglePR() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache)
        .getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(q1dateJan1, "update0");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q1dateFeb1, "update00");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q1dateMar1, "update000");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q2dateApr1, "update1");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q2dateMay1, "update11");
    assertEquals(false, cms.isRefreshMetadataTestOnly());
    
    region.put(q2dateJun1, "update111");
    assertEquals(false, cms.isRefreshMetadataTestOnly());
    
    region.put(q3dateJuly1, "update2");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q3dateAug1, "update22");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q3dateSep1, "update2222");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q4dateOct1, "update3");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q4dateNov1, "update33");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q4dateDec1, "update3333");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

  }

  public static void updateIntoSinglePRFor3Qs() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache)
        .getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(q1dateJan1, "update0");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q1dateFeb1, "update00");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q1dateMar1, "update000");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q2dateApr1, "update1");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q2dateMay1, "update11");
    assertEquals(false, cms.isRefreshMetadataTestOnly());
    
    region.put(q2dateJun1, "update111");
    assertEquals(false, cms.isRefreshMetadataTestOnly());
    
    region.put(q3dateJuly1, "update2");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q3dateAug1, "update22");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q3dateSep1, "update2222");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

  }

  public static void verifyEmptyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache)
        .getClientMetadataService();
    assertTrue(cms.getClientPRMetadata_TEST_ONLY().isEmpty());
  }

  public static void verifyEmptyStaticData() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache)
        .getClientMetadataService();
    assertTrue(cms.getClientPartitionAttributesMap().isEmpty());
  }

  public static void verifyNonEmptyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache)
        .getClientMetadataService();
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
    if (pr.cache != null) {
      ((GemFireCacheImpl)cache).getLogger().info(
          "Primary Bucket view of server0  "
              + pr.getDataStore().getLocalPrimaryBucketsListTestOnly());
      ((GemFireCacheImpl)cache).getLogger().info(
          "Secondary Bucket view of server0  "
              + pr.getDataStore().getLocalNonPrimaryBucketsListTestOnly());
    }
  }

  private void verifyMetadata(final int totalBuckets, int currentRedundancy) {
    ClientMetadataService cms = ((GemFireCacheImpl)cache)
        .getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms
        .getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return (regionMetaData.size() == 1);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60000, 1000, true);
    
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    final ClientPartitionAdvisor prMetaData = regionMetaData
        .get(region.getFullPath());
    wc = new WaitCriterion() {
      public boolean done() {
        return (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == totalBuckets);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60000, 1000, true);
    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      assertEquals(currentRedundancy, ((List)entry.getValue()).size());
    }
  }

  public static void clearMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl)cache)
        .getClientMetadataService();
    cms.getClientPartitionAttributesMap().clear();
    cms.getClientPRMetadata_TEST_ONLY().clear();
  }

}
