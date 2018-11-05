/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

public class PRClientServerTestBase extends JUnit4CacheTestCase {

  protected VM server1 = null;

  protected static VM server2 = null;

  protected static VM server3 = null;

  protected static VM client = null;

  protected static Cache cache = null;

  protected static String PartitionedRegionName = "TestPartitionedRegion"; // default name

  protected static String regionName = "TestRegion"; // default name

  private static Region metaDataRegion;

  static PartitionResolver partitionResolver = null; // default

  static Integer redundancy = new Integer(0); // default

  // static Integer localMaxmemory = new Integer(20); //default

  static Integer totalNumBuckets = new Integer(13);// default

  static String colocatedWith = null;// default

  protected static Integer serverPort1 = null;

  protected static Integer serverPort2 = null;

  protected static Integer serverPort3 = null;

  protected static PoolImpl pool = null;

  private boolean isSingleHop = false;

  private boolean isSelector = false;

  public PRClientServerTestBase(String name, boolean isSingleHop, boolean isSelector) {
    super();
    this.isSingleHop = isSingleHop;
    this.isSelector = isSelector;
  }

  public PRClientServerTestBase() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    client = host.getVM(3);
    postSetUpPRClientServerTestBase();
  }

  protected void postSetUpPRClientServerTestBase() throws Exception {}

  public ArrayList createCommonServerAttributes(String regionName, PartitionResolver pr, int red,
      int numBuckets, String colocatedWithRegion) {
    ArrayList commonAttributes = new ArrayList();
    commonAttributes.add(regionName); // 0
    commonAttributes.add(pr); // 1
    commonAttributes.add(new Integer(red)); // 2
    commonAttributes.add(new Integer(totalNumBuckets)); // 3
    commonAttributes.add(colocatedWithRegion); // 4
    return commonAttributes;
  }

  public static Integer createCacheServer(ArrayList commonAttributes, Integer localMaxMemory) {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    paf.setPartitionResolver((PartitionResolver) commonAttributes.get(1));
    paf.setRedundantCopies(((Integer) commonAttributes.get(2)).intValue());
    paf.setTotalNumBuckets(((Integer) commonAttributes.get(3)).intValue());
    paf.setColocatedWith((String) commonAttributes.get(4));
    paf.setLocalMaxMemory(localMaxMemory.intValue());
    PartitionAttributes partitionAttributes = paf.create();
    factory.setDataPolicy(DataPolicy.PARTITION);
    factory.setPartitionAttributes(partitionAttributes);
    RegionAttributes attrs = factory.create();

    Region region = cache.createRegion((String) commonAttributes.get(0), attrs);
    assertNotNull(region);
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    try {
      server1.start();
    } catch (IOException e) {
      Assert.fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  public static Integer createSelectorCacheServer(ArrayList commonAttributes,
      Integer localMaxMemory) throws Exception {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    paf.setPartitionResolver((PartitionResolver) commonAttributes.get(1));
    paf.setRedundantCopies(((Integer) commonAttributes.get(2)).intValue());
    paf.setTotalNumBuckets(((Integer) commonAttributes.get(3)).intValue());
    paf.setColocatedWith((String) commonAttributes.get(4));
    paf.setLocalMaxMemory(localMaxMemory.intValue());
    PartitionAttributes partitionAttributes = paf.create();
    factory.setDataPolicy(DataPolicy.PARTITION);
    factory.setPartitionAttributes(partitionAttributes);
    RegionAttributes attrs = factory.create();

    Region region = cache.createRegion((String) commonAttributes.get(0), attrs);
    assertNotNull(region);
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.setMaxThreads(16);
    server1.start();
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  public static Integer createCacheServerWith2Regions(ArrayList commonAttributes,
      Integer localMaxMemory) throws Exception {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    paf.setPartitionResolver((PartitionResolver) commonAttributes.get(1));
    paf.setRedundantCopies(((Integer) commonAttributes.get(2)).intValue());
    paf.setTotalNumBuckets(((Integer) commonAttributes.get(3)).intValue());
    paf.setColocatedWith((String) commonAttributes.get(4));
    paf.setLocalMaxMemory(localMaxMemory.intValue());
    PartitionAttributes partitionAttributes = paf.create();
    factory.setDataPolicy(DataPolicy.PARTITION);
    factory.setPartitionAttributes(partitionAttributes);
    RegionAttributes attrs = factory.create();

    Region region1 = cache.createRegion(PartitionedRegionName + "1", attrs);
    assertNotNull(region1);
    Region region2 = cache.createRegion(PartitionedRegionName + "2", attrs);
    assertNotNull(region2);
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.start();
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  public static Integer createCacheServer() throws Exception {
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.start();
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  public static Integer createCacheServerWithDR() throws Exception {
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
    server1.start();
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  public static void createCacheClient(String host, Integer port1, Integer port2, Integer port3)
      throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue()).setPingInterval(2000)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(2)
          .setPRSingleHopEnabled(false).create("PRClientServerTestBase");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl) p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }

  public static void createCacheClient_SingleConnection(String host, Integer port1)
      throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    serverPort1 = port1;

    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue()).setPingInterval(2000)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(0).setMaxConnections(1).setRetryAttempts(0)
          .setPRSingleHopEnabled(false).create("PRClientServerTestBase");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl) p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }

  public static void createCacheClientWith2Regions(String host, Integer port1, Integer port2,
      Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue()).setPingInterval(2000)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(2)
          .create("PRClientServerTestBase");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl) p;
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region1 = cache.createRegion(PartitionedRegionName + "1", attrs);
    assertNotNull(region1);

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    attrs = factory.create();
    Region region2 = cache.createRegion(PartitionedRegionName + "2", attrs);
    assertNotNull(region2);
  }

  public static void createSingleHopCacheClient(String host, Integer port1, Integer port2,
      Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue()).setPingInterval(2000)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(2)
          .setPRSingleHopEnabled(true).create("PRClientServerTestBase");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl) p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }

  public static void createNoSingleHopCacheClient(String host, Integer port1, Integer port2,
      Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue()).setPingInterval(2000)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(2)
          .setPRSingleHopEnabled(false).create("PRClientServerTestBase");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl) p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(PartitionedRegionName, attrs);
    assertNotNull(region);
  }

  public static void createCacheClientWithoutRegion(String host, Integer port1, Integer port2,
      Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    LogWriterUtils.getLogWriter()
        .info("PRClientServerTestBase#createCacheClientWithoutRegion : creating pool");
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue()).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(1)
          .create("PRClientServerTestBaseWithoutRegion");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl) p;
  }

  public static void createCacheClientWithDistributedRegion(String host, Integer port1,
      Integer port2, Integer port3) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    LogWriterUtils.getLogWriter()
        .info("PRClientServerTestBase#createCacheClientWithoutRegion : creating pool");
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, port1.intValue())
          .addServer(host, port2.intValue()).addServer(host, port3.intValue()).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(0)
          .create("PRClientServerTestBaseWithoutRegion");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    pool = (PoolImpl) p;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    assertNotNull(cache);
    Region region = cache.createRegion(regionName, factory.create());
    assertNotNull(region);
  }

  protected void createClientServerScenarion(ArrayList commonAttributes, int localMaxMemoryServer1,
      int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer) server1.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer1)));
    Integer port2 = (Integer) server2.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer2)));
    Integer port3 = (Integer) server3.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer3)));
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(() -> PRClientServerTestBase
        .createCacheClient(NetworkUtils.getServerHostName(server1.getHost()), port1, port2, port3));
  }

  protected void createClientServerScenarion_SingleConnection(ArrayList commonAttributes,
      int localMaxMemoryServer1, int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer) server1.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer1)));
    server2.invoke(() -> PRClientServerTestBase.createCacheServer(commonAttributes,
        new Integer(localMaxMemoryServer2)));
    serverPort1 = port1;
    client.invoke(() -> PRClientServerTestBase.createCacheClient_SingleConnection(
        NetworkUtils.getServerHostName(server1.getHost()), port1));
  }



  protected void createClientServerScenarionWith2Regions(ArrayList commonAttributes,
      int localMaxMemoryServer1, int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer) server1.invoke(() -> PRClientServerTestBase
        .createCacheServerWith2Regions(commonAttributes, new Integer(localMaxMemoryServer1)));
    Integer port2 = (Integer) server2.invoke(() -> PRClientServerTestBase
        .createCacheServerWith2Regions(commonAttributes, new Integer(localMaxMemoryServer2)));
    Integer port3 = (Integer) server3.invoke(() -> PRClientServerTestBase
        .createCacheServerWith2Regions(commonAttributes, new Integer(localMaxMemoryServer3)));
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(() -> PRClientServerTestBase.createCacheClientWith2Regions(
        NetworkUtils.getServerHostName(server1.getHost()), port1, port2, port3));
  }

  protected void createClientServerScenarioSingleHop(ArrayList commonAttributes,
      int localMaxMemoryServer1, int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer) server1.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer1)));
    Integer port2 = (Integer) server2.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer2)));
    Integer port3 = (Integer) server3.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer3)));
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    // Workaround for the issue that hostnames returned by the client metadata may
    // not match those configured by the pool, leading to multiple copies
    // of the endpoint in the client.
    String hostname = (String) server1.invoke(() -> PRClientServerTestBase.getHostname());
    client.invoke(
        () -> PRClientServerTestBase.createSingleHopCacheClient(hostname, port1, port2, port3));
  }

  public static String getHostname() {
    return cache.getDistributedSystem().getDistributedMember().getHost();
  }

  protected void createClientServerScenarioNoSingleHop(ArrayList commonAttributes,
      int localMaxMemoryServer1, int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer) server1.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer1)));
    Integer port2 = (Integer) server2.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer2)));
    Integer port3 = (Integer) server3.invoke(() -> PRClientServerTestBase
        .createCacheServer(commonAttributes, new Integer(localMaxMemoryServer3)));
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(() -> PRClientServerTestBase.createNoSingleHopCacheClient(
        NetworkUtils.getServerHostName(server1.getHost()), port1, port2, port3));
  }

  protected void createClientServerScenarioSelectorNoSingleHop(ArrayList commonAttributes,
      int localMaxMemoryServer1, int localMaxMemoryServer2, int localMaxMemoryServer3) {
    createCacheInClientServer();
    Integer port1 = (Integer) server1.invoke(() -> PRClientServerTestBase
        .createSelectorCacheServer(commonAttributes, new Integer(localMaxMemoryServer1)));
    Integer port2 = (Integer) server2.invoke(() -> PRClientServerTestBase
        .createSelectorCacheServer(commonAttributes, new Integer(localMaxMemoryServer2)));
    Integer port3 = (Integer) server3.invoke(() -> PRClientServerTestBase
        .createSelectorCacheServer(commonAttributes, new Integer(localMaxMemoryServer3)));
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;
    client.invoke(() -> PRClientServerTestBase.createNoSingleHopCacheClient(
        NetworkUtils.getServerHostName(server1.getHost()), port1, port2, port3));
  }


  protected void createClientServerScenarionWithoutRegion() {
    LogWriterUtils.getLogWriter().info(
        "PRClientServerTestBase#createClientServerScenarionWithoutRegion : creating client server");
    createCacheInClientServer();
    Integer port1 = (Integer) server1.invoke(() -> PRClientServerTestBase.createCacheServer());
    Integer port2 = (Integer) server2.invoke(() -> PRClientServerTestBase.createCacheServer());
    Integer port3 = (Integer) server3.invoke(() -> PRClientServerTestBase.createCacheServer());
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    client.invoke(() -> PRClientServerTestBase.createCacheClientWithoutRegion(
        NetworkUtils.getServerHostName(server1.getHost()), port1, port2, port3));
  }

  protected void createClientServerScenarionWithDistributedtRegion() {
    LogWriterUtils.getLogWriter().info(
        "PRClientServerTestBase#createClientServerScenarionWithoutRegion : creating client server");
    createCacheInClientServer();
    Integer port1 =
        (Integer) server1.invoke(() -> PRClientServerTestBase.createCacheServerWithDR());
    Integer port2 =
        (Integer) server2.invoke(() -> PRClientServerTestBase.createCacheServerWithDR());
    Integer port3 =
        (Integer) server3.invoke(() -> PRClientServerTestBase.createCacheServerWithDR());
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;


    client.invoke(() -> PRClientServerTestBase.createCacheClientWithDistributedRegion(
        NetworkUtils.getServerHostName(server1.getHost()), port1, port2, port3));
  }

  protected void runOnAllServers(SerializableRunnable runnable) {
    server1.invoke(runnable);
    server2.invoke(runnable);
    server3.invoke(runnable);
  }

  protected void registerFunctionAtServer(Function function) {
    server1.invoke(PRClientServerTestBase.class, "registerFunction", new Object[] {function});

    server2.invoke(PRClientServerTestBase.class, "registerFunction", new Object[] {function});

    server3.invoke(PRClientServerTestBase.class, "registerFunction", new Object[] {function});
  }

  public static void registerFunction(Function function) {
    FunctionService.registerFunction(function);
  }

  private void createCacheInClientServer() {
    Properties props = new Properties();
    props.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.functions.TestFunction;org.apache.geode.internal.cache.execute.**");
    server1.invoke(() -> PRClientServerTestBase.createCacheInVm(props));

    server2.invoke(() -> PRClientServerTestBase.createCacheInVm(props));


    server3.invoke(() -> PRClientServerTestBase.createCacheInVm(props));

    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    client.invoke(() -> PRClientServerTestBase.createCacheInVm(props));
  }

  public static void createCacheInVm(Properties props) {
    new PRClientServerTestBase().createCache(props);
  }

  private void createCache(Properties props) {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    } catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  public static void startServerHA() throws Exception {
    Wait.pause(2000);
    Collection bridgeServers = cache.getCacheServers();
    LogWriterUtils.getLogWriter()
        .info("Start Server cache servers list : " + bridgeServers.size());
    Iterator bridgeIterator = bridgeServers.iterator();
    CacheServer bridgeServer = (CacheServer) bridgeIterator.next();
    LogWriterUtils.getLogWriter().info("start Server cache server" + bridgeServer);
    bridgeServer.start();
  }

  public static void stopServerHA() throws Exception {
    Wait.pause(1000);
    Iterator iter = cache.getCacheServers().iterator();
    if (iter.hasNext()) {
      CacheServer server = (CacheServer) iter.next();
      server.stop();
    }
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    closeCacheAndDisconnect();
    client.invoke(() -> PRClientServerTestBase.closeCacheAndDisconnect());
    server1.invoke(() -> PRClientServerTestBase.closeCacheAndDisconnect());
    server2.invoke(() -> PRClientServerTestBase.closeCacheAndDisconnect());
    server3.invoke(() -> PRClientServerTestBase.closeCacheAndDisconnect());
  }

  public static void closeCacheAndDisconnect() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void closeCacheHA() {
    Wait.pause(1000);
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }

  public static void serverBucketFilterExecution(Set<Integer> bucketFilterSet) throws Exception {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = 150; i > 0; i--) {
      testKeysSet.add(i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    FunctionService.registerFunction(function);
    InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(region);
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }

    List l = null;
    ResultCollector<Integer, List<Integer>> rc =
        dataSet.withBucketFilter(bucketFilterSet).execute(function.getId());
    List<Integer> results = rc.getResult();
    assertEquals(bucketFilterSet.size(), results.size());
    for (Integer bucket : results) {
      bucketFilterSet.remove(bucket);
    }
    assertTrue(bucketFilterSet.isEmpty());
  }

  public static void serverBucketFilterOverrideExecution(Set<Integer> bucketFilterSet,
      Set<Integer> ketFilterSet) throws Exception {

    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = 150; i > 0; i--) {
      testKeysSet.add(i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    FunctionService.registerFunction(function);
    InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(region);
    Set<Integer> expectedBucketSet = new HashSet<Integer>();
    for (Integer key : ketFilterSet) {
      expectedBucketSet.add(BucketFilterPRResolver.getBucketID(key));
    }
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }

    List l = null;
    ResultCollector<Integer, List<Integer>> rc = dataSet.withBucketFilter(bucketFilterSet)
        .withFilter(ketFilterSet).execute(function.getId());
    List<Integer> results = rc.getResult();
    assertEquals(expectedBucketSet.size(), results.size());
    for (Integer bucket : results) {
      expectedBucketSet.remove(bucket);
    }
    assertTrue(expectedBucketSet.isEmpty());
  }

  public static class BucketFilterPRResolver implements PartitionResolver, Serializable {

    @Override
    public void close() {}

    @Override
    public Object getRoutingObject(EntryOperation opDetails) {
      Object key = opDetails.getKey();
      return getBucketID(key);
    }

    static int getBucketID(Object key) {
      return key.hashCode() / 10;
    }

    @Override
    public String getName() {
      return "bucketFilterPRResolver";
    }
  }
}
