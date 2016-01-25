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
package com.gemstone.gemfire.internal.cache.execute;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

public class PRClientServerTestBase extends CacheTestCase {

  protected static VM server1 = null;

  protected static VM server2 = null;

  protected static VM server3 = null;

  protected static VM client = null;

  protected static Cache cache = null;

  protected static String PartitionedRegionName = "TestPartitionedRegion"; //default name

  protected static String regionName = "TestRegion"; //default name
  
  private static Region metaDataRegion;
  
  static PartitionResolver partitionResolver = null; //default

  static Integer redundancy = new Integer(0); //default

  //static Integer localMaxmemory = new Integer(20); //default

  static Integer totalNumBuckets = new Integer(13);//default

  static String colocatedWith = null;//default

  protected static Integer serverPort1 = null;

  protected static Integer serverPort2 = null;

  protected static Integer serverPort3 = null;

  protected static PoolImpl pool = null;
  
  private boolean isSingleHop = false;
  
  private boolean isSelector = false;
  
  public PRClientServerTestBase(String name, boolean isSingleHop, boolean isSelector) {
    super(name);
    this.isSingleHop = isSingleHop;
    this.isSelector = isSelector;
  }

  public PRClientServerTestBase(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    client = host.getVM(3);
  }

  public ArrayList createCommonServerAttributes(String regionName,
      PartitionResolver pr, int red, int numBuckets, String colocatedWithRegion) {
    ArrayList commonAttributes = new ArrayList();
    commonAttributes.add(regionName); //0
    commonAttributes.add(pr); //1
    commonAttributes.add(new Integer(red)); //2
    commonAttributes.add(new Integer(totalNumBuckets)); //3
    commonAttributes.add(colocatedWithRegion); //4
    return commonAttributes;
  }

  public static Integer createCacheServer(ArrayList commonAttributes, Integer localMaxMemory) {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    
    paf.setPartitionResolver((PartitionResolver)commonAttributes.get(1));
    paf.setRedundantCopies(((Integer)commonAttributes.get(2)).intValue());
    paf.setTotalNumBuckets(((Integer)commonAttributes.get(3)).intValue());
    paf.setColocatedWith((String)commonAttributes.get(4));
    paf.setLocalMaxMemory(localMaxMemory.intValue());
    PartitionAttributes partitionAttributes = paf.create();
    factory.setDataPolicy(DataPolicy.PARTITION);
    factory.setPartitionAttributes(partitionAttributes);
    RegionAttributes attrs = factory.create();

    Region region = cache.createRegion((String)commonAttributes.get(0), attrs);    
    assertNotNull(region);
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    try {
      server1.start();
    }
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }
  
  public static Integer createSelectorCacheServer(ArrayList commonAttributes, Integer localMaxMemory) {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    
    paf.setPartitionResolver((PartitionResolver)commonAttributes.get(1));
    paf.setRedundantCopies(((Integer)commonAttributes.get(2)).intValue());
    paf.setTotalNumBuckets(((Integer)commonAttributes.get(3)).intValue());
    paf.setColocatedWith((String)commonAttributes.get(4));
    paf.setLocalMaxMemory(localMaxMemory.intValue());
    PartitionAttributes partitionAttributes = paf.create();
    factory.setDataPolicy(DataPolicy.PARTITION);
    factory.setPartitionAttributes(partitionAttributes);
    RegionAttributes attrs = factory.create();

    Region region = cache.createRegion((String)commonAttributes.get(0), attrs);    
    assertNotNull(region);
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.setMaxThreads(16);
    try {
      server1.start();
    }
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  public static Integer createCacheServerWith2Regions(ArrayList commonAttributes, Integer localMaxMemory) {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    
    paf.setPartitionResolver((PartitionResolver)commonAttributes.get(1));
    paf.setRedundantCopies(((Integer)commonAttributes.get(2)).intValue());
    paf.setTotalNumBuckets(((Integer)commonAttributes.get(3)).intValue());
    paf.setColocatedWith((String)commonAttributes.get(4));
    paf.setLocalMaxMemory(localMaxMemory.intValue());
    PartitionAttributes partitionAttributes = paf.create();
    factory.setDataPolicy(DataPolicy.PARTITION);
    factory.setPartitionAttributes(partitionAttributes);
    RegionAttributes attrs = factory.create();

    Region region1 = cache.createRegion(PartitionedRegionName+"1", attrs);   
    assertNotNull(region1);
    Region region2 = cache.createRegion(PartitionedRegionName+"2", attrs);
    assertNotNull(region2);
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    try {
      server1.start();
    }
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }
  public static Integer createCacheServer() {
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    try {
      server1.start();
    }
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }
  
  public static Integer createCacheServerWithDR() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    assertNotNull(cache);
    Region region = cache.createRegion(regionName, factory.create());
    assertNotNull(region);
    
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    try {
      server1.start();
    }       
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  public static void createCacheClient(String host, Integer port1,
      Integer port2, Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue())
          .setPingInterval(2000).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(2).setPRSingleHopEnabled(false).create("PRClientServerTestBase");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl)p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }
  
  public static void createCacheClient_SingleConnection(String host, Integer port1) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    serverPort1 = port1;
    
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .setPingInterval(2000).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(0).setMaxConnections(1)
          .setRetryAttempts(0).setPRSingleHopEnabled(false).create("PRClientServerTestBase");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl)p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }
  
  public static void createCacheClientWith2Regions(String host, Integer port1,
      Integer port2, Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue())
          .setPingInterval(2000).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(2).create("PRClientServerTestBase");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl)p;
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region1 = cache.createRegion(PartitionedRegionName+"1", attrs);
    assertNotNull(region1);
    
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    attrs = factory.create();
    Region region2 = cache.createRegion(PartitionedRegionName+"2", attrs);
    assertNotNull(region2);
  }
  
  public static void createSingleHopCacheClient(String host, Integer port1,
      Integer port2, Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue())
          .setPingInterval(2000).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(2).setPRSingleHopEnabled(true).create("PRClientServerTestBase");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl)p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }

  public static void createNoSingleHopCacheClient(String host, Integer port1,
      Integer port2, Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue())
          .setPingInterval(2000).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(2).setPRSingleHopEnabled(false).create("PRClientServerTestBase");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl)p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }
  public static void createCacheClientWithoutRegion(String host, Integer port1,
      Integer port2, Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    getLogWriter().info("PRClientServerTestBase#createCacheClientWithoutRegion : creating pool");
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue())
          .setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(1).create("PRClientServerTestBaseWithoutRegion");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl)p;    
  }
  
  public static void createCacheClientWithDistributedRegion(String host, Integer port1,
      Integer port2, Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    getLogWriter().info("PRClientServerTestBase#createCacheClientWithoutRegion : creating pool");
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue())
          .setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(0).create("PRClientServerTestBaseWithoutRegion");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl)p;   
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    assertNotNull(cache);
    Region region = cache.createRegion(regionName, factory.create());
    assertNotNull(region);
  }
  
  protected void createClientServerScenarion(ArrayList commonAttributes , int localMaxMemoryServer1,
      int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes ,new Integer(localMaxMemoryServer1) });
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes, new Integer(localMaxMemoryServer2) });
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] { commonAttributes, new Integer(localMaxMemoryServer3) });
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(PRClientServerTestBase.class, "createCacheClient",
        new Object[] { getServerHostName(server1.getHost()), port1, port2,
            port3 });
  }

  protected void createClientServerScenarion_SingleConnection(ArrayList commonAttributes , int localMaxMemoryServer1,
      int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes ,new Integer(localMaxMemoryServer1) });
    server2.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes ,new Integer(localMaxMemoryServer2) });
    serverPort1 = port1;
    client.invoke(PRClientServerTestBase.class, "createCacheClient_SingleConnection",
        new Object[] { getServerHostName(server1.getHost()), port1});
  }
  
  
  
  protected void createClientServerScenarionWith2Regions(ArrayList commonAttributes , int localMaxMemoryServer1,
      int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createCacheServerWith2Regions",
        new Object[] {commonAttributes ,new Integer(localMaxMemoryServer1) });
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createCacheServerWith2Regions",
        new Object[] {commonAttributes, new Integer(localMaxMemoryServer2) });
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createCacheServerWith2Regions",
        new Object[] { commonAttributes, new Integer(localMaxMemoryServer3) });
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(PRClientServerTestBase.class, "createCacheClientWith2Regions",
        new Object[] { getServerHostName(server1.getHost()), port1, port2,
            port3 });
  }

  protected void createClientServerScenarioSingleHop(ArrayList commonAttributes , int localMaxMemoryServer1,
      int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes ,new Integer(localMaxMemoryServer1) });
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes, new Integer(localMaxMemoryServer2) });
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] { commonAttributes, new Integer(localMaxMemoryServer3) });
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    //Workaround for the issue that hostnames returned by the client metadata may
    //not match those configured by the pool, leading to multiple copies 
    //of the endpoint in the client.
    String hostname = (String) server1.invoke(PRClientServerTestBase.class,
        "getHostname", new Object[] {});
    client.invoke(PRClientServerTestBase.class, "createSingleHopCacheClient",
        new Object[] { hostname, port1, port2,
            port3 });
  }
  
  public static String getHostname() {
    return cache.getDistributedSystem().getDistributedMember().getHost();
  }
  
  protected void createClientServerScenarioNoSingleHop(ArrayList commonAttributes , int localMaxMemoryServer1,
      int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes ,new Integer(localMaxMemoryServer1) });
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] {commonAttributes, new Integer(localMaxMemoryServer2) });
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createCacheServer",
        new Object[] { commonAttributes, new Integer(localMaxMemoryServer3) });
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(PRClientServerTestBase.class, "createNoSingleHopCacheClient",
        new Object[] { getServerHostName(server1.getHost()), port1, port2,
            port3 });
  }
  
  protected void createClientServerScenarioSelectorNoSingleHop(ArrayList commonAttributes , int localMaxMemoryServer1,
      int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createSelectorCacheServer",
        new Object[] {commonAttributes ,new Integer(localMaxMemoryServer1) });
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createSelectorCacheServer",
        new Object[] {commonAttributes, new Integer(localMaxMemoryServer2) });
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createSelectorCacheServer",
        new Object[] { commonAttributes, new Integer(localMaxMemoryServer3) });
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(PRClientServerTestBase.class, "createNoSingleHopCacheClient",
        new Object[] { getServerHostName(server1.getHost()), port1, port2,
            port3 });
  }

  
  protected void createClientServerScenarionWithoutRegion () {
    getLogWriter().info("PRClientServerTestBase#createClientServerScenarionWithoutRegion : creating client server");
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createCacheServer");
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createCacheServer");
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createCacheServer");
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    client.invoke(PRClientServerTestBase.class, "createCacheClientWithoutRegion",
        new Object[] { getServerHostName(server1.getHost()), port1, port2,
            port3 });    
  }
  
  protected void createClientServerScenarionWithDistributedtRegion () {
    getLogWriter().info("PRClientServerTestBase#createClientServerScenarionWithoutRegion : creating client server");
    createCacheInClientServer();
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
        "createCacheServerWithDR");
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createCacheServerWithDR");
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createCacheServerWithDR");
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    
    
    client.invoke(PRClientServerTestBase.class, "createCacheClientWithDistributedRegion",
        new Object[] { getServerHostName(server1.getHost()), port1, port2,
            port3 });    
  }

  protected void runOnAllServers(SerializableRunnable runnable) {
    server1.invoke(runnable);
    server2.invoke(runnable);
    server3.invoke(runnable);
  }

  protected void registerFunctionAtServer (Function function) {    
    server1.invoke(PRClientServerTestBase.class,
        "registerFunction", new Object []{function});
    
    server2.invoke(PRClientServerTestBase.class,
        "registerFunction", new Object []{function});
    
    server3.invoke(PRClientServerTestBase.class,
        "registerFunction", new Object []{function});
  }
  
  public static void registerFunction (Function function){
    FunctionService.registerFunction(function);    
  }
  
  private void createCacheInClientServer() {
    Properties props = new Properties();
    server1.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });

    server2.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });
    

    server3.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });

    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    client.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });
  }
  
  public static void createCacheInVm(Properties props) {
    new PRClientServerTestBase("temp").createCache(props);
  }

  private void createCache(Properties props) {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    }
    catch (Exception e) {
      fail("Failed while creating the cache", e);
    }
  }

  public static void startServerHA() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 2000, 500, false);
    Collection bridgeServers = cache.getCacheServers();
    getLogWriter().info(
        "Start Server Bridge Servers list : " + bridgeServers.size());
    Iterator bridgeIterator = bridgeServers.iterator();
    CacheServer bridgeServer = (CacheServer)bridgeIterator.next();
    getLogWriter().info("start Server Bridge Server" + bridgeServer);
    try {
      bridgeServer.start();
    }
    catch (IOException e) {
      fail("not able to start the server");
    }
  }  
  
  public static void stopServerHA() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 1000, 200, false);
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
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    closeCache();
    client.invoke(PRClientServerTestBase.class, "closeCache");
    server1.invoke(PRClientServerTestBase.class, "closeCache");
    server2.invoke(PRClientServerTestBase.class, "closeCache");
    server3.invoke(PRClientServerTestBase.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  public static void closeCacheHA() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 1000, 200, false);
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }
  
  public static void serverBucketFilterExecution(Set<Integer> bucketFilterSet) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = 150; i > 0; i--) {
      testKeysSet.add( i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    FunctionService.registerFunction(function);
    InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      
      List l = null;
      ResultCollector<Integer, List<Integer>> rc =  (ResultCollector<Integer, List<Integer>>)dataSet.
          withBucketFilter(bucketFilterSet).execute(function.getId());
      List<Integer> results = rc.getResult();
      assertEquals(bucketFilterSet.size(), results.size());
      for(Integer bucket: results) {
        bucketFilterSet.remove(bucket) ; 
      }
      assertTrue(bucketFilterSet.isEmpty());

     
      
    }catch(Exception e){
      fail("Test failed ", e);
      
    }
  }
  
  public static void serverBucketFilterOverrideExecution(Set<Integer> bucketFilterSet,
      Set<Integer> ketFilterSet) {

    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = 150; i > 0; i--) {
      testKeysSet.add( i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    FunctionService.registerFunction(function);
    InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(region);
    Set<Integer> expectedBucketSet = new HashSet<Integer>();
    for(Integer key : ketFilterSet) {
      expectedBucketSet.add(BucketFilterPRResolver.getBucketID(key));
    }
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      
      List l = null;
      ResultCollector<Integer, List<Integer>> rc =  (ResultCollector<Integer, List<Integer>>)dataSet.
          withBucketFilter(bucketFilterSet).withFilter(ketFilterSet).execute(function.getId());
      List<Integer> results = rc.getResult();
      assertEquals(expectedBucketSet.size(), results.size());
      for(Integer bucket: results) {
        expectedBucketSet.remove(bucket) ; 
      }
      assertTrue(expectedBucketSet.isEmpty());
    }catch(Exception e){
      fail("Test failed ", e);
      
    }
  
  }
  
  public static class BucketFilterPRResolver implements PartitionResolver, Serializable {    
    
    @Override
    public void close() {            
    }

    @Override
    public Object getRoutingObject(EntryOperation opDetails) {
      Object key = opDetails.getKey();
      return getBucketID(key);
    }
    
    static int getBucketID(Object key) {
      return key.hashCode()/10;
    }

    @Override
    public String getName() {
      return "bucketFilterPRResolver";
    }    
  }
}
