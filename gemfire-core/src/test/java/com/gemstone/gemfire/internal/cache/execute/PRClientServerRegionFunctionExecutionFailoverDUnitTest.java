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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;

public class PRClientServerRegionFunctionExecutionFailoverDUnitTest extends
    PRClientServerTestBase {

  private static Locator locator = null;
  
  private static Region region = null;
  
  public PRClientServerRegionFunctionExecutionFailoverDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("SocketTimeoutException");
    IgnoredException.addIgnoredException("ServerConnectivityException");
    IgnoredException.addIgnoredException("Socket Closed");
  }
  
  public void testserverMultiKeyExecution_SocektTimeOut() {
    createScenario();
    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecutionSocketTimeOut", new Object[] { new Boolean(true) });
  }

  /*
   * Ensure that the while executing the function if the servers is down then
   * the execution is failover to other available server
   */
  public void testServerFailoverWithTwoServerAliveHA()
      throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    ArrayList commonAttributes = createCommonServerAttributes(
        "TestPartitionedRegion", null, 1, 13, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "stopServerHA");
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "stopServerHA");
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "putOperation");
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = client.invokeAsync(
        PRClientServerRegionFunctionExecutionDUnitTest.class,
        "executeFunctionHA");
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "startServerHA");
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "startServerHA");
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "stopServerHA");
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "verifyDeadAndLiveServers", new Object[] { new Integer(1),
            new Integer(2) });
    ThreadUtils.join(async[0], 6 * 60 * 1000);
    if (async[0].getException() != null) {
      Assert.fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List)async[0].getReturnValue();
    assertEquals(2, l.size());
  }

  /*
   * Ensure that the while executing the function if the servers is down then
   * the execution is failover to other available server
   */
  public void testServerCacheClosedFailoverWithTwoServerAliveHA()
      throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    ArrayList commonAttributes = createCommonServerAttributes(
        "TestPartitionedRegion", null, 1, 13, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "stopServerHA");
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "stopServerHA");
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "putOperation");
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = client.invokeAsync(
        PRClientServerRegionFunctionExecutionDUnitTest.class,
        "executeFunctionHA");
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "startServerHA");
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "startServerHA");
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "closeCacheHA");
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "verifyDeadAndLiveServers", new Object[] { new Integer(1),
            new Integer(2) });
    ThreadUtils.join(async[0], 5 * 60 * 1000);
    if (async[0].getException() != null) {
      Assert.fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List)async[0].getReturnValue();
    assertEquals(2, l.size());
  }

  public void testBug40714() {
    createScenario();
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "FunctionExecution_Inline_Bug40714");
  }

  public void testOnRegionFailoverWithTwoServerDownHA()
      throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    createScenario();

    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createProxyRegion",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()) });

    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_HA_REGION);
    registerFunctionAtServer(function);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "regionExecutionHATwoServerDown", new Object[] { Boolean.FALSE,
            function, Boolean.FALSE });

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "verifyMetaData", new Object[] { new Integer(2), new Integer(1) });
  }

  // retry attempts is 2
  public void testOnRegionFailoverWithOneServerDownHA()
      throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    createScenario();

    server1.invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server2.invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server3.invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createProxyRegion",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()) });

    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_HA_REGION);
    registerFunctionAtServer(function);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "regionExecutionHAOneServerDown", new Object[] { Boolean.FALSE,
            function, Boolean.FALSE });

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "verifyMetaData", new Object[] { new Integer(1), new Integer(1) });
  }

  /*
   * Ensure that the while executing the function if the servers are down then
   * the execution shouldn't failover to other available server
   */
  public void testOnRegionFailoverNonHA() throws InterruptedException { // See #47489 before enabling it
    createScenario();
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createProxyRegion",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()) });

    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_NONHA_REGION);
    registerFunctionAtServer(function);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "regionSingleKeyExecutionNonHA", new Object[] { Boolean.FALSE,
            function, Boolean.FALSE });
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "verifyMetaData", new Object[] { new Integer(1), new Integer(0) });
  }
  
  /*
   * Ensure that the while executing the function if the servers are down then
   * the execution shouldn't failover to other available server
   */
  public void testOnRegionFailoverNonHASingleHop() throws InterruptedException { // See #47489 before enabling it
    ArrayList commonAttributes = createCommonServerAttributes(
        "TestPartitionedRegion", null, 0, 13, null);
    createClientServerScenarioSingleHop(commonAttributes, 20, 20, 20);
    
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "createProxyRegion",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()) });

    //Make sure the buckets are created.
    client.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        region = (LocalRegion) cache.getRegion(PRClientServerTestBase.PartitionedRegionName);
        for(int i=0 ; i< 13; i++) {
          region.put(i, i);
        }
      }
    });
    
    //Make sure the client metadata is up to date.
    client.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "fetchMetaData");
    
    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_NONHA_REGION);
    registerFunctionAtServer(function);
    final Function function2 = new TestFunction(true,
        TestFunction.TEST_FUNCTION_NONHA_NOP);
    registerFunctionAtServer(function);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "regionSingleKeyExecutionNonHA", new Object[] { Boolean.FALSE,
            function, Boolean.FALSE });
    
    
    //This validation doesn't work because the client may
    //still be asynchronously recording the departure of the
    //failed server
//    System.err.println("Trying the second function");
//    //Make sure the client can now execute a function
//    //on the server
//    client.invoke(new SerializableRunnable() {
//      @Override
//      public void run() {
//        ResultCollector rs = FunctionService.onRegion(region).execute(function2);
//        rs.getResult();
//      }
//    });
    
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "verifyMetaData", new Object[] { new Integer(1), new Integer(0) });
  }

  public void testServerBucketMovedException() throws InterruptedException {

    IgnoredException.addIgnoredException("BucketMovedException");
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);

    disconnectAllFromDS();
    
    ArrayList commonAttributes = createCommonServerAttributes(
        "TestPartitionedRegion", null, 1, 113, null);

    final int portLocator = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    final String hostLocator = NetworkUtils.getServerHostName(server1.getHost());
    final String locator = hostLocator + "[" + portLocator + "]";

    startLocatorInVM(portLocator);
    try {

    Integer port1 = (Integer)server1.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "createServerWithLocator", new Object[] { locator, false,
            commonAttributes });

    Integer port2 = (Integer)server2.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "createServerWithLocator", new Object[] { locator, false,
            commonAttributes });

    server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "createClientWithLocator", new Object[] { hostLocator, portLocator });
    server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "putIntoRegion");

    server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "fetchMetaData");
    
    Integer port3 = (Integer)server3.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "createServerWithLocator", new Object[] { locator, false,
            commonAttributes });

    Object result = server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "executeFunction");
    List l = (List)result;
    assertEquals(2, l.size());

    } finally {
    stopLocator();
    }
  }

  public void testServerBucketMovedException_LocalServer()
      throws InterruptedException {
    IgnoredException.addIgnoredException("BucketMovedException");

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);

    ArrayList commonAttributes = createCommonServerAttributes(
        "TestPartitionedRegion", null, 0, 113, null);

    final int portLocator = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    final String hostLocator = NetworkUtils.getServerHostName(server1.getHost());
    final String locator = hostLocator + "[" + portLocator + "]";

    startLocatorInVM(portLocator);
    try {

    Integer port1 = (Integer)server1.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "createServerWithLocator", new Object[] { locator, false,
            commonAttributes });

    server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "createClientWithLocator", new Object[] { hostLocator, portLocator });
    server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "putIntoRegion");

    server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "fetchMetaData");
    
    Integer port2 = (Integer)server2.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "createServerWithLocator", new Object[] { locator, false,
            commonAttributes });

    Object result = server4.invoke(
        PRClientServerRegionFunctionExecutionFailoverDUnitTest.class,
        "executeFunction");
    List l = (List)result;
    assertEquals(2, l.size());

    } finally {
    stopLocator();
    }
  }
  
  public static void fetchMetaData(){
    ((GemFireCacheImpl)cache).getClientMetadataService().getClientPRMetadata((LocalRegion)region);
  }
  
  public void startLocatorInVM(final int locatorPort) {

    File logFile = new File("locator-" + locatorPort + ".log");

    Properties props = new Properties();
    props = DistributedTestUtils.getAllDistributedSystemProperties(props);
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    try {
      locator = Locator.startLocatorAndDS(locatorPort, logFile, null, props);
    }
    catch (IOException e) {
      Assert.fail("Unable to start locator ", e);
    }
  }
  
  public static void stopLocator() {
    locator.stop();
  }
  
  public static int createServerWithLocator(String locator, boolean isAccessor, ArrayList commonAttrs) {
    CacheTestCase test = new PRClientServerRegionFunctionExecutionFailoverDUnitTest(
    "PRClientServerRegionFunctionExecutionFailoverDUnitTest");
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
      Assert.fail("Failed to start server ", e);
    }
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    if (isAccessor) {
      paf.setLocalMaxMemory(0);
    }
    paf.setTotalNumBuckets(((Integer)commonAttrs.get(3)).intValue()).setRedundantCopies(((Integer)commonAttrs.get(2)).intValue());
    

    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(regionName, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + regionName + " created Successfully :"
            + region.toString());
    return port;
  }
  
  public static void createClientWithLocator(String host, int port0) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    CacheTestCase test = new PRClientServerRegionFunctionExecutionFailoverDUnitTest(
        "PRClientServerRegionFunctionExecutionFailoverDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(
          250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
          .setMaxConnections(10).setRetryAttempts(3).create("Pool_"+regionName);
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(p.getName());
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(regionName, attrs);
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Distributed Region " + regionName + " created Successfully :"
            + region.toString());
  }
  
  public static void putIntoRegion() {
    for(int i = 0 ; i < 113; i++){
      region.put(i, "KB_"+i);
    }
    LogWriterUtils.getLogWriter().info(
        "Distributed Region " + regionName + " Have size :"
            + region.size());
  }
  
  public static Object executeFunction(){
    Execution execute = FunctionService.onRegion(region);
    ResultCollector rc = execute.withArgs(Boolean.TRUE).execute(
        new TestFunction(true, TestFunction.TEST_FUNCTION_LASTRESULT));
    LogWriterUtils.getLogWriter().info(
        "Exeuction Result :"
            + rc.getResult());
    List l = ((List)rc.getResult());
    return l;
  }
  
  public static void checkSize(){
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + regionName + " Have size :"
            + region.size());
  }
  
  protected void createScenario() {
    ArrayList commonAttributes = createCommonServerAttributes(
        "TestPartitionedRegion", null, 0, 13, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
  }
  

}
