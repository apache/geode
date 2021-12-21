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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ClientServerTest.class, FunctionServiceTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PRClientServerRegionFunctionExecutionFailoverDUnitTest extends PRClientServerTestBase {

  private static final Logger logger = LogService.getLogger();

  private static Locator locator = null;

  private static Region<Integer, Object> region = null;

  @Override
  protected void postSetUpPRClientServerTestBase() {
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("SocketTimeoutException");
    IgnoredException.addIgnoredException("ServerConnectivityException");
    IgnoredException.addIgnoredException("Socket Closed");
  }

  @Test
  public void testserverMultiKeyExecution_SocektTimeOut() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverMultiKeyExecutionSocketTimeOut(Boolean.TRUE));
  }

  /*
   * Ensure that the while executing the function if the servers is down then the execution is
   * failover to other available server
   */
  @Test
  public void testServerFailoverWithTwoServerAliveHA() {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 1, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::stopServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::stopServerHA);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::putOperation);
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = client
        .invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest::executeFunctionHA);
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::startServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::startServerHA);
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest::stopServerHA);
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .verifyDeadAndLiveServers(2));
    ThreadUtils.join(async[0], 6 * 60 * 1000);
    if (async[0].getException() != null) {
      Assert.fail("UnExpected Exception Occurred : ", async[0].getException());
    }
    List l = (List) async[0].getReturnValue();
    assertEquals(2, l.size());
  }

  /*
   * Ensure that the while executing the function if the servers is down then the execution is
   * failover to other available server
   */
  @Test
  public void testServerCacheClosedFailoverWithTwoServerAliveHA() {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 1, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::stopServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::stopServerHA);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::putOperation);
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = client
        .invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest::executeFunctionHA);
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::startServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::startServerHA);
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest::closeCacheHA);
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .verifyDeadAndLiveServers(2));
    ThreadUtils.join(async[0], 5 * 60 * 1000);
    if (async[0].getException() != null) {
      Assert.fail("UnExpected Exception Occurred : ", async[0].getException());
    }
    List l = (List) async[0].getReturnValue();
    assertEquals(2, l.size());
  }

  @Test
  public void testBug40714() {
    createScenario();
    server1.invoke(
        (SerializableRunnableIF) PRClientServerRegionFunctionExecutionDUnitTest::registerFunction);
    server1.invoke(
        (SerializableRunnableIF) PRClientServerRegionFunctionExecutionDUnitTest::registerFunction);
    server1.invoke(
        (SerializableRunnableIF) PRClientServerRegionFunctionExecutionDUnitTest::registerFunction);
    client.invoke(
        (SerializableRunnableIF) PRClientServerRegionFunctionExecutionDUnitTest::registerFunction);
    client.invoke(
        PRClientServerRegionFunctionExecutionDUnitTest::FunctionExecution_Inline_Bug40714);
  }

  @Test
  public void testOnRegionFailoverWithTwoServerDownHA() {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    createScenario();

    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createProxyRegion);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_REGION);
    registerFunctionAtServer(function);

    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .regionExecutionHATwoServerDown(Boolean.FALSE, function, Boolean.FALSE));

    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest.verifyMetaData(2, 1));
  }

  // retry attempts is 2
  @Test
  public void testOnRegionFailoverWithOneServerDownHA() {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    createScenario();

    server1
        .invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server2
        .invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server3
        .invokeAsync(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createProxyRegion);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_REGION);
    registerFunctionAtServer(function);

    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .regionExecutionHAOneServerDown(Boolean.FALSE, function, Boolean.FALSE));

    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .verifyMetaData(1, 1));
  }

  /*
   * Ensure that the while executing the function if the servers are down then the execution
   * shouldn't failover to other available server
   */
  @Test
  public void testOnRegionFailoverNonHA() {
    createScenario();
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createProxyRegion);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NONHA_REGION);
    registerFunctionAtServer(function);

    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .regionSingleKeyExecutionNonHA(Boolean.FALSE, function, Boolean.FALSE));
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .verifyMetaData(1, 0));
  }

  /*
   * Ensure that the while executing the function if the servers are down then the execution
   * shouldn't failover to other available server
   */
  @Test
  public void testOnRegionFailoverNonHASingleHop() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarioSingleHop(commonAttributes, 20, 20, 20);

    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::createProxyRegion);

    // Make sure the buckets are created.
    client.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        region = cache.getRegion(PRClientServerTestBase.PartitionedRegionName);
        for (int i = 0; i < 13; i++) {
          region.put(i, i);
        }
      }
    });

    // Make sure the client metadata is up to date.
    client.invoke(PRClientServerRegionFunctionExecutionFailoverDUnitTest::fetchMetaData);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NONHA_REGION);
    registerFunctionAtServer(function);
    final Function function2 = new TestFunction(true, TestFunction.TEST_FUNCTION_NONHA_NOP);
    registerFunctionAtServer(function2);

    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .regionSingleKeyExecutionNonHA(Boolean.FALSE, function, Boolean.FALSE));


    // This validation doesn't work because the client may
    // still be asynchronously recording the departure of the
    // failed server
    // System.err.println("Trying the second function");
    // //Make sure the client can now execute a function
    // //on the server
    // client.invoke(new SerializableRunnable() {
    // @Override
    // public void run() {
    // ResultCollector rs = FunctionService.onRegion(region).execute(function2);
    // rs.getResult();
    // }
    // });

    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .verifyMetaData(1, 0));
  }

  @Test
  public void testServerBucketMovedException() {

    IgnoredException.addIgnoredException("BucketMovedException");
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);

    disconnectAllFromDS();

    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 1, null);

    final int portLocator = getRandomAvailableTCPPort();
    final String hostLocator = NetworkUtils.getServerHostName(server1.getHost());
    final String locator = hostLocator + "[" + portLocator + "]";

    startLocatorInVM(portLocator);
    try {

      server1.invoke(() -> createServerWithLocator(locator, false, commonAttributes));

      server2.invoke(() -> createServerWithLocator(locator, false, commonAttributes));

      server4.invoke(() -> createClientWithLocator(hostLocator, portLocator));
      server4.invoke(PRClientServerRegionFunctionExecutionFailoverDUnitTest::putIntoRegion);

      server4.invoke(PRClientServerRegionFunctionExecutionFailoverDUnitTest::fetchMetaData);

      server3.invoke(() -> createServerWithLocator(locator, false, commonAttributes));

      Object result = server4
          .invoke(PRClientServerRegionFunctionExecutionFailoverDUnitTest::executeFunction);
      List l = (List) result;
      assertEquals(2, l.size());

    } finally {
      stopLocator();
    }
  }

  @Test
  public void testServerBucketMovedException_LocalServer() {
    IgnoredException.addIgnoredException("BucketMovedException");

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server4 = host.getVM(3);

    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);

    final int portLocator = getRandomAvailableTCPPort();
    final String hostLocator = NetworkUtils.getServerHostName(server1.getHost());
    final String locator = hostLocator + "[" + portLocator + "]";

    startLocatorInVM(portLocator);
    try {

      server1.invoke(() -> createServerWithLocator(locator, false, commonAttributes));

      server4.invoke(() -> createClientWithLocator(hostLocator, portLocator));
      server4.invoke(PRClientServerRegionFunctionExecutionFailoverDUnitTest::putIntoRegion);

      server4.invoke(PRClientServerRegionFunctionExecutionFailoverDUnitTest::fetchMetaData);

      server2.invoke(() -> createServerWithLocator(locator, false, commonAttributes));

      Object result = server4
          .invoke(PRClientServerRegionFunctionExecutionFailoverDUnitTest::executeFunction);
      List l = (List) result;
      assertEquals(2, l.size());

    } finally {
      stopLocator();
    }
  }

  private static void fetchMetaData() {
    ((GemFireCacheImpl) cache).getClientMetadataService().getClientPRMetadata((LocalRegion) region);
  }

  private void startLocatorInVM(final int locatorPort) {

    File logFile = new File("locator-" + locatorPort + ".log");

    Properties props = new Properties();
    props = DistributedTestUtils.getAllDistributedSystemProperties(props);
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    try {
      locator = Locator.startLocatorAndDS(locatorPort, logFile, null, props);
    } catch (IOException e) {
      Assert.fail("Unable to start locator ", e);
    }
  }

  public static void stopLocator() {
    locator.stop();
  }

  private int createServerWithLocator(String locator, boolean isAccessor, ArrayList commonAttrs) {
    Properties props = new Properties();
    props.setProperty(LOCATORS, locator);
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);

    CacheServer server = cache.addCacheServer();
    int port = getRandomAvailableTCPPort();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    if (isAccessor) {
      paf.setLocalMaxMemory(0);
    }
    paf.setTotalNumBuckets((Integer) commonAttrs.get(3))
        .setRedundantCopies((Integer) commonAttrs.get(2));


    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(regionName, attr.create());
    assertNotNull(region);
    logger
        .info("Partitioned Region " + regionName + " created Successfully :" + region);
    return port;
  }

  private void createClientWithLocator(String host, int port0) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create("Pool_" + regionName);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(p.getName());
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(regionName, attrs);
    assertNotNull(region);
    logger
        .info("Distributed Region " + regionName + " created Successfully :" + region);
  }

  public static void putIntoRegion() {
    for (int i = 0; i < 113; i++) {
      region.put(i, "KB_" + i);
    }
    logger
        .info("Distributed Region " + regionName + " Have size :" + region.size());
  }

  public static Object executeFunction() {
    Execution execute = FunctionService.onRegion(region);
    ResultCollector rc = execute.setArguments(Boolean.TRUE)
        .execute(new TestFunction(true, TestFunction.TEST_FUNCTION_LASTRESULT));
    logger.info("Exeuction Result :" + rc.getResult());
    List l = ((List) rc.getResult());
    return l;
  }

  protected void createScenario() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
  }


}
