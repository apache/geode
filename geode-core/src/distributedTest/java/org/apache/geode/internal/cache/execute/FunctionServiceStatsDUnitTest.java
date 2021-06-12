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
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionTestHelper;
import org.apache.geode.internal.cache.execute.metrics.FunctionServiceStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * This is DUnite Test to test the Function Execution stats under various scenarios like
 * Client-Server with Region/without Region, P2P with partitioned Region/Distributed Region,member
 * Execution
 */
@Category({FunctionServiceTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class FunctionServiceStatsDUnitTest extends PRClientServerTestBase {

  private static final Logger logger = LogService.getLogger();

  static InternalDistributedSystem ds = null;

  private static int noOfExecutionCalls_Aggregate = 0;
  private static int noOfExecutionsCompleted_Aggregate = 0;
  private static int resultReceived_Aggregate = 0;

  private static int noOfExecutionCalls_TESTFUNCTION1 = 0;
  private static int noOfExecutionsCompleted_TESTFUNCTION1 = 0;
  private static int resultReceived_TESTFUNCTION1 = 0;

  private static int noOfExecutionCalls_TESTFUNCTION2 = 0;
  private static int noOfExecutionsCompleted_TESTFUNCTION2 = 0;
  private static int resultReceived_TESTFUNCTION2 = 0;

  private static int noOfExecutionCalls_TESTFUNCTION3 = 0;
  private static int noOfExecutionsCompleted_TESTFUNCTION3 = 0;
  private static int resultReceived_TESTFUNCTION3 = 0;

  private static int noOfExecutionCalls_TESTFUNCTION5 = 0;
  private static int noOfExecutionsCompleted_TESTFUNCTION5 = 0;
  private static int resultReceived_TESTFUNCTION5 = 0;

  private static int noOfExecutionCalls_Inline = 0;
  private static int noOfExecutionsCompleted_Inline = 0;
  private static int resultReceived_Inline = 0;

  @Override
  protected final void postSetUpPRClientServerTestBase() {
    // Make sure stats to linger from a previous test
    disconnectAllFromDS();
  }

  private final transient SerializableRunnableIF initializeStats = () -> {
    noOfExecutionCalls_Aggregate = 0;
    noOfExecutionsCompleted_Aggregate = 0;
    resultReceived_Aggregate = 0;

    noOfExecutionCalls_TESTFUNCTION1 = 0;
    noOfExecutionsCompleted_TESTFUNCTION1 = 0;
    resultReceived_TESTFUNCTION1 = 0;

    noOfExecutionCalls_TESTFUNCTION2 = 0;
    noOfExecutionsCompleted_TESTFUNCTION2 = 0;
    resultReceived_TESTFUNCTION2 = 0;

    noOfExecutionCalls_TESTFUNCTION3 = 0;
    noOfExecutionsCompleted_TESTFUNCTION3 = 0;
    resultReceived_TESTFUNCTION3 = 0;

    noOfExecutionCalls_TESTFUNCTION5 = 0;
    noOfExecutionsCompleted_TESTFUNCTION5 = 0;
    resultReceived_TESTFUNCTION5 = 0;

    noOfExecutionCalls_Inline = 0;
    noOfExecutionsCompleted_Inline = 0;
    resultReceived_Inline = 0;
  };

  private final transient SerializableRunnableIF closeDistributedSystem = () -> {
    if (getCache() != null && !getCache().isClosed()) {
      getCache().close();
      getCache().getDistributedSystem().disconnect();
    }
  };

  /*
   * This helper method prevents race conditions in local functions. Typically, when calling
   * ResultCollector.getResult() one might expect the function to have completed. For local
   * functions this is true, however, at this point the function stats may not have been updated yet
   * thus any code which checks stats after calling getResult() may get wrong data.
   */
  private void waitNoFunctionsRunning(FunctionServiceStats stats) {
    int count = 100;
    while (stats.getFunctionExecutionsRunning() > 0 && count > 0) {
      count--;
      try {
        Thread.sleep(50);
      } catch (InterruptedException ex) {
        // Ignored
      }
    }
  }

  /**
   * 1-client 3-Servers Function : TEST_FUNCTION2 Function : TEST_FUNCTION3 Execution of the
   * function on serverRegion with set multiple keys as the routing object and using the name of the
   * function
   *
   * On server side, function execution calls should be equal to the no of function executions
   * completed.
   */
  @Test
  public void testClientServerPartitonedRegionFunctionExecutionStats() {
    createScenario();
    registerFunctionAtServer(new TestFunction(true, TestFunction.TEST_FUNCTION2));
    registerFunctionAtServer(new TestFunction(true, TestFunction.TEST_FUNCTION3));

    client.invoke(initializeStats);
    server1.invoke(initializeStats);
    server2.invoke(initializeStats);
    server3.invoke(initializeStats);

    client.invoke(() -> {
      Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
      assertNotNull(region);
      final HashSet<String> testKeysSet = new HashSet<>();
      for (int i = (totalNumBuckets * 2); i > 0; i--) {
        testKeysSet.add("execKey-" + i);
      }
      DistributedSystem.setThreadsSocketPolicy(false);
      Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
      if (shouldRegisterFunctionsOnClient()) {
        FunctionService.registerFunction(function);
      }
      Execution dataSet = FunctionService.onRegion(region);
      try {
        int j = 0;
        for (String s : testKeysSet) {
          Integer val = j++;
          region.put(s, val);
        }
        ResultCollector rc =
            dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(function.getId());
        int resultSize = ((List) rc.getResult()).size();
        resultReceived_Aggregate += resultSize;
        resultReceived_TESTFUNCTION2 += resultSize;
        noOfExecutionCalls_Aggregate++;
        noOfExecutionCalls_TESTFUNCTION2++;
        noOfExecutionsCompleted_Aggregate++;
        noOfExecutionsCompleted_TESTFUNCTION2++;

        rc = dataSet.withFilter(testKeysSet).setArguments(testKeysSet).execute(function.getId());
        resultSize = ((List) rc.getResult()).size();
        resultReceived_Aggregate += resultSize;
        resultReceived_TESTFUNCTION2 += resultSize;
        noOfExecutionCalls_Aggregate++;
        noOfExecutionCalls_TESTFUNCTION2++;
        noOfExecutionsCompleted_Aggregate++;
        noOfExecutionsCompleted_TESTFUNCTION2++;

        function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
        FunctionService.registerFunction(function);
        rc = dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(function.getId());
        resultSize = ((List) rc.getResult()).size();
        resultReceived_Aggregate += resultSize;
        resultReceived_TESTFUNCTION3 += resultSize;
        noOfExecutionCalls_Aggregate++;
        noOfExecutionCalls_TESTFUNCTION3++;
        noOfExecutionsCompleted_Aggregate++;
        noOfExecutionsCompleted_TESTFUNCTION3++;

      } catch (Exception e) {
        logger.info("Exception : " + e.getMessage());
        e.printStackTrace();
        fail("Test failed after the put operation");
      }
    });

    client.invoke(() -> {
      // checks for the aggregate stats
      InternalDistributedSystem iDS = (InternalDistributedSystem) cache.getDistributedSystem();
      FunctionServiceStats functionServiceStats =
          iDS.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Aggregate,
          functionServiceStats.getFunctionExecutionsCompleted());
      assertTrue(functionServiceStats.getResultsReceived() >= resultReceived_Aggregate);

      logger.info("Calling FunctionStats for  TEST_FUNCTION2 :");
      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
      logger.info("Called FunctionStats for  TEST_FUNCTION2 :");
      assertEquals(noOfExecutionCalls_TESTFUNCTION2, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION2,
          functionStats.getFunctionExecutionsCompleted());
      assertTrue(functionStats.getResultsReceived() >= resultReceived_TESTFUNCTION2);

      functionStats = FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
      assertEquals(noOfExecutionCalls_TESTFUNCTION3, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION3,
          functionStats.getFunctionExecutionsCompleted());
      assertTrue(functionStats.getResultsReceived() >= resultReceived_TESTFUNCTION3);
    });

    SerializableRunnableIF checkStatsOnServer = () -> {
      // checks for the aggregate stats
      InternalDistributedSystem iDS = (InternalDistributedSystem) cache.getDistributedSystem();
      FunctionServiceStats functionServiceStats =
          iDS.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      // functions are executed 3 times
      noOfExecutionCalls_Aggregate += 3;
      assertTrue(
          functionServiceStats.getFunctionExecutionCalls() >= noOfExecutionCalls_Aggregate);
      noOfExecutionsCompleted_Aggregate += 3;
      assertTrue(functionServiceStats
          .getFunctionExecutionsCompleted() >= noOfExecutionsCompleted_Aggregate);

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
      // TEST_FUNCTION2 is executed twice
      noOfExecutionCalls_TESTFUNCTION2 += 2;
      assertTrue(functionStats.getFunctionExecutionCalls() >= noOfExecutionCalls_TESTFUNCTION2);
      noOfExecutionsCompleted_TESTFUNCTION2 += 2;
      assertTrue(functionStats
          .getFunctionExecutionsCompleted() >= noOfExecutionsCompleted_TESTFUNCTION2);

      functionStats = FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
      // TEST_FUNCTION3 is executed once
      noOfExecutionCalls_TESTFUNCTION3 += 1;
      assertTrue(functionStats.getFunctionExecutionCalls() >= noOfExecutionCalls_TESTFUNCTION3);
      noOfExecutionsCompleted_TESTFUNCTION3 += 1;
      assertTrue(functionStats
          .getFunctionExecutionsCompleted() >= noOfExecutionsCompleted_TESTFUNCTION3);
    };

    server1.invoke(checkStatsOnServer);
    server2.invoke(checkStatsOnServer);
    server3.invoke(checkStatsOnServer);
  }

  /**
   * 1-client 3-Servers server1 : Replicate server2 : Replicate server3 : Replicate client : Empty
   * Function : TEST_FUNCTION2 Execution of the function on serverRegion with set multiple keys as
   * the routing object and using the name of the function
   *
   * On server side, function execution calls should be equal to the no of function executions
   * completed.
   */
  @Test
  public void testClientServerDistributedRegionFunctionExecutionStats() {

    final String regionName = "FunctionServiceStatsDUnitTest";
    SerializableCallableIF<Integer> createCahenServer = () -> {
      try {
        Properties props = new Properties();
        DistributedSystem ds = getSystem(props);
        assertNotNull(ds);
        ds.disconnect();
        ds = getSystem(props);
        cache = CacheFactory.create(ds);
        logger.info("Created Cache on Server");
        assertNotNull(cache);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        assertNotNull(cache);
        Region<String, Integer> region = cache.createRegion(regionName, factory.create());
        logger.info("Region Created :" + region);
        assertNotNull(region);
        for (int i = 1; i <= 200; i++) {
          region.put("execKey-" + i, i);
        }
        CacheServer server = cache.addCacheServer();
        assertNotNull(server);
        int port = getRandomAvailableTCPPort();
        server.setPort(port);
        try {
          server.start();
        } catch (IOException e) {
          Assert.fail("Failed to start the Server", e);
        }
        assertTrue(server.isRunning());
        return server.getPort();
      } catch (Exception e) {
        Assert.fail("FunctionServiceStatsDUnitTest#createCache() Failed while creating the cache",
            e);
        throw e;
      }
    };
    final Integer port1 = server1.invoke(createCahenServer);
    final Integer port2 = server2.invoke(createCahenServer);
    final Integer port3 = server3.invoke(createCahenServer);

    client.invoke(() -> {
      try {
        Properties props = new Properties();
        props.put(MCAST_PORT, "0");
        props.put(LOCATORS, "");
        DistributedSystem ds = getSystem(props);
        assertNotNull(ds);
        ds.disconnect();
        ds = getSystem(props);
        cache = CacheFactory.create(ds);
        logger.info("Created Cache on Client");
        assertNotNull(cache);


        CacheServerTestUtil.disableShufflingOfEndpoints();
        Pool p;
        try {
          p = PoolManager.createFactory().addServer("localhost", port1)
              .addServer("localhost", port2).addServer("localhost", port3)
              .setPingInterval(250).setSubscriptionEnabled(false).setSubscriptionRedundancy(-1)
              .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6)
              .setMaxConnections(10).setRetryAttempts(3)
              .create("FunctionServiceStatsDUnitTest_pool");
        } finally {
          CacheServerTestUtil.enableShufflingOfEndpoints();
        }
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setPoolName(p.getName());
        assertNotNull(cache);
        Region<String, Integer> region = cache.createRegion(regionName, factory.create());
        logger.info("Client Region Created :" + region);
        assertNotNull(region);
        for (int i = 1; i <= 200; i++) {
          region.put("execKey-" + i, i);
        }
      } catch (Exception e) {
        Assert.fail("FunctionServiceStatsDUnitTest#createCache() Failed while creating the cache",
            e);
        throw e;
      }
    });

    client.invoke(initializeStats);
    server1.invoke(initializeStats);
    server2.invoke(initializeStats);
    server3.invoke(initializeStats);

    registerFunctionAtServer(new TestFunction(true, TestFunction.TEST_FUNCTION2));
    registerFunctionAtServer(new TestFunction(true, TestFunction.TEST_FUNCTION3));

    client.invoke(() -> {
      Function function2 = new TestFunction(true, TestFunction.TEST_FUNCTION2);
      Function function3 = new TestFunction(true, TestFunction.TEST_FUNCTION3);
      if (shouldRegisterFunctionsOnClient()) {
        FunctionService.registerFunction(function2);
        FunctionService.registerFunction(function3);
      }
      Region region = cache.getRegion(regionName);
      Set<String> filter = new HashSet<>();
      for (int i = 100; i < 120; i++) {
        filter.add("execKey-" + i);
      }

      try {
        noOfExecutionCalls_Aggregate++;
        noOfExecutionCalls_TESTFUNCTION2++;
        List list = (List) FunctionService.onRegion(region).withFilter(filter)
            .execute(function2).getResult();
        noOfExecutionsCompleted_Aggregate++;
        noOfExecutionsCompleted_TESTFUNCTION2++;
        int size = list.size();
        resultReceived_Aggregate += size;
        resultReceived_TESTFUNCTION2 += size;

        noOfExecutionCalls_Aggregate++;
        noOfExecutionCalls_TESTFUNCTION2++;
        list = (List) FunctionService.onRegion(region).withFilter(filter).execute(function2)
            .getResult();
        noOfExecutionsCompleted_Aggregate++;
        noOfExecutionsCompleted_TESTFUNCTION2++;
        size = list.size();
        resultReceived_Aggregate += size;
        resultReceived_TESTFUNCTION2 += size;
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("test failed due to", e);
        throw e;
      }

    });

    client.invoke(() -> {
      // checks for the aggregate stats
      InternalDistributedSystem iDS = (InternalDistributedSystem) cache.getDistributedSystem();
      FunctionServiceStats functionServiceStats =
          iDS.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Aggregate,
          functionServiceStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_Aggregate, functionServiceStats.getResultsReceived());

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
      assertEquals(noOfExecutionCalls_TESTFUNCTION2, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION2,
          functionStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_TESTFUNCTION2, functionStats.getResultsReceived());
    });
  }

  /**
   * Execution of the function on server using the name of the function TEST_FUNCTION1
   * TEST_FUNCTION5 On client side, the no of result received should equal to the no of function
   * execution calls. On server side, function execution calls should be equal to the no of function
   * executions completed.
   */
  @Test
  public void testClientServerwithoutRegion() {
    createClientServerScenarionWithoutRegion();
    registerFunctionAtServer(new TestFunction(true, TestFunction.TEST_FUNCTION1));
    registerFunctionAtServer(new TestFunction(true, TestFunction.TEST_FUNCTION5));

    client.invoke(initializeStats);
    server1.invoke(initializeStats);
    server2.invoke(initializeStats);
    server3.invoke(initializeStats);

    client.invoke(() -> {
      DistributedSystem.setThreadsSocketPolicy(false);
      Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
      if (shouldRegisterFunctionsOnClient()) {
        FunctionService.registerFunction(function);
      }
      Execution member = FunctionService.onServers(pool);

      try {
        ResultCollector rs = member.setArguments(Boolean.TRUE).execute(function.getId());
        int size = ((List) rs.getResult()).size();
        resultReceived_Aggregate += size;
        noOfExecutionCalls_Aggregate++;
        noOfExecutionsCompleted_Aggregate++;
        resultReceived_TESTFUNCTION1 += size;
        noOfExecutionCalls_TESTFUNCTION1++;
        noOfExecutionsCompleted_TESTFUNCTION1++;
      } catch (Exception ex) {
        ex.printStackTrace();
        logger.info("Exception : ", ex);
        fail("Test failed after the execute operation nn TRUE");
      }
      function = new TestFunction(true, TestFunction.TEST_FUNCTION5);
      if (shouldRegisterFunctionsOnClient()) {
        FunctionService.registerFunction(function);
      }
      try {
        ResultCollector rs = member.setArguments("Success").execute(function.getId());
        int size = ((List) rs.getResult()).size();
        resultReceived_Aggregate += size;
        noOfExecutionCalls_Aggregate++;
        noOfExecutionsCompleted_Aggregate++;
        resultReceived_TESTFUNCTION5 += size;
        noOfExecutionCalls_TESTFUNCTION5++;
        noOfExecutionsCompleted_TESTFUNCTION5++;
      } catch (Exception ex) {
        ex.printStackTrace();
        logger.info("Exception : ", ex);
        fail("Test failed after the execute operationssssss");
      }
    });


    client.invoke(() -> {
      // checks for the aggregate stats
      InternalDistributedSystem iDS = (InternalDistributedSystem) cache.getDistributedSystem();
      FunctionServiceStats functionServiceStats =
          iDS.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Aggregate,
          functionServiceStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_Aggregate, functionServiceStats.getResultsReceived());

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION1, iDS);
      assertEquals(noOfExecutionCalls_TESTFUNCTION1, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION1,
          functionStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_TESTFUNCTION1, functionStats.getResultsReceived());

      functionStats = FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION5, iDS);
      assertEquals(noOfExecutionCalls_TESTFUNCTION5, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION5,
          functionStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_TESTFUNCTION5, functionStats.getResultsReceived());

    });

    SerializableRunnableIF checkStatsOnServer = () -> {
      // checks for the aggregate stats
      InternalDistributedSystem iDS = (InternalDistributedSystem) cache.getDistributedSystem();
      FunctionServiceStats functionServiceStats =
          iDS.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      // functions are executed 2 times
      noOfExecutionCalls_Aggregate += 2;
      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      noOfExecutionsCompleted_Aggregate += 2;
      // this check is time sensitive, so allow it to fail a few times
      // before giving up
      for (int i = 0; i < 10; i++) {
        try {
          assertEquals(noOfExecutionsCompleted_Aggregate,
              functionServiceStats.getFunctionExecutionsCompleted());
        } catch (RuntimeException r) {
          if (i == 9) {
            throw r;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw r;
          }
        }
      }

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION1, iDS);
      // TEST_FUNCTION1 is executed once
      noOfExecutionCalls_TESTFUNCTION1 += 1;
      assertEquals(noOfExecutionCalls_TESTFUNCTION1, functionStats.getFunctionExecutionCalls());
      noOfExecutionsCompleted_TESTFUNCTION1 += 1;
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION1,
          functionStats.getFunctionExecutionsCompleted());

      functionStats = FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION5, iDS);
      // TEST_FUNCTION5 is executed once
      noOfExecutionCalls_TESTFUNCTION5 += 1;
      assertEquals(noOfExecutionCalls_TESTFUNCTION5, functionStats.getFunctionExecutionCalls());
      noOfExecutionsCompleted_TESTFUNCTION5 += 1;
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION5,
          functionStats.getFunctionExecutionsCompleted());

    };

    server1.invoke(checkStatsOnServer);
    server2.invoke(checkStatsOnServer);
    server3.invoke(checkStatsOnServer);
  }

  @Test
  public void testP2PDummyExecutionStats() {
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM accessor = host.getVM(3);
    accessor.invoke(closeDistributedSystem);
    datastore0.invoke(closeDistributedSystem);
    datastore1.invoke(closeDistributedSystem);
    datastore2.invoke(closeDistributedSystem);
  }

  /**
   * Ensure that the execution is happening all the PR as a whole
   *
   * Function Execution will not take place on accessor, accessor will onlu receive the
   * resultsReceived. On datastore, no of function execution calls should be equal to the no of
   * function execution calls from the accessor.
   */
  @Test
  public void testP2PPartitionedRegionsFunctionExecutionStats() {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM accessor = host.getVM(3);

    datastore0.invoke(initializeStats);
    datastore1.invoke(initializeStats);
    datastore2.invoke(initializeStats);
    accessor.invoke(initializeStats);

    accessor.invoke(() -> {
      RegionAttributes ra = PartitionedRegionTestHelper.createRegionAttrsForPR(0, 0);
      AttributesFactory raf = new AttributesFactory(ra);
      PartitionAttributesImpl pa = new PartitionAttributesImpl();
      pa.setAll(ra.getPartitionAttributes());
      pa.setTotalNumBuckets(17);
      raf.setPartitionAttributes(pa);

      getCache().createRegion(rName, raf.create());
    });

    SerializableRunnableIF dataStoreCreate = () -> {
      RegionAttributes ra = PartitionedRegionTestHelper.createRegionAttrsForPR(0, 10);
      AttributesFactory raf = new AttributesFactory(ra);
      PartitionAttributesImpl pa = new PartitionAttributesImpl();
      pa.setAll(ra.getPartitionAttributes());
      pa.setTotalNumBuckets(17);
      raf.setPartitionAttributes(pa);
      getCache().createRegion(rName, raf.create());
      Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
      FunctionService.registerFunction(function);
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    accessor.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(rName);
      DistributedSystem.setThreadsSocketPolicy(false);
      final HashSet<String> testKeys = new HashSet<>();
      for (int i = (pr.getTotalNumberOfBuckets() * 3); i > 0; i--) {
        testKeys.add("execKey-" + i);
      }
      int j = 0;
      for (Object testKey : testKeys) {
        Integer val = j++;
        pr.put(testKey, val);
      }
      Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution dataSet = FunctionService.onRegion(pr);
      ResultCollector rc1 = dataSet.setArguments(Boolean.TRUE).execute(function);
      int size = ((List) rc1.getResult()).size();
      resultReceived_Aggregate += size;
      resultReceived_TESTFUNCTION2 += size;

      rc1 = dataSet.setArguments(testKeys).execute(function);
      size = ((List) rc1.getResult()).size();
      resultReceived_Aggregate += size;
      resultReceived_TESTFUNCTION2 += size;

      function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
      FunctionService.registerFunction(function);
      rc1 = dataSet.setArguments(Boolean.TRUE).execute(function);
      size = ((List) rc1.getResult()).size();
      resultReceived_Aggregate += size;
      resultReceived_TESTFUNCTION3 += size;
    });

    accessor.invoke(() -> {
      InternalDistributedSystem iDS =
          ((InternalDistributedSystem) getCache().getDistributedSystem());
      FunctionServiceStats functionServiceStats =
          iDS.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Aggregate,
          functionServiceStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_Aggregate, functionServiceStats.getResultsReceived());

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
      assertEquals(noOfExecutionCalls_TESTFUNCTION2, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION2,
          functionStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_TESTFUNCTION2, functionStats.getResultsReceived());

      functionStats = FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
      assertEquals(noOfExecutionCalls_TESTFUNCTION3, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION3,
          functionStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_TESTFUNCTION3, functionStats.getResultsReceived());
    });

    SerializableRunnableIF checkFunctionExecutionStatsForDataStore = () -> {
      InternalDistributedSystem iDS =
          ((InternalDistributedSystem) getCache().getDistributedSystem());
      // 3 Function Executions took place
      FunctionServiceStats functionServiceStats =
          iDS.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      noOfExecutionCalls_Aggregate += 3;
      noOfExecutionsCompleted_Aggregate += 3;
      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Aggregate,
          functionServiceStats.getFunctionExecutionsCompleted());

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
      // TEST_FUNCTION2 is executed twice
      noOfExecutionCalls_TESTFUNCTION2 += 2;
      assertEquals(noOfExecutionCalls_TESTFUNCTION2,
          functionStats.getFunctionExecutionCalls());
      noOfExecutionsCompleted_TESTFUNCTION2 += 2;
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION2,
          functionStats.getFunctionExecutionsCompleted());

      functionStats = FunctionStatsManager.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
      // TEST_FUNCTION3 is executed once
      noOfExecutionCalls_TESTFUNCTION3 += 1;
      assertEquals(noOfExecutionCalls_TESTFUNCTION3,
          functionStats.getFunctionExecutionCalls());
      noOfExecutionsCompleted_TESTFUNCTION3 += 1;
      assertEquals(noOfExecutionsCompleted_TESTFUNCTION3,
          functionStats.getFunctionExecutionsCompleted());
    };
    datastore0.invoke(checkFunctionExecutionStatsForDataStore);
    datastore1.invoke(checkFunctionExecutionStatsForDataStore);
    datastore2.invoke(checkFunctionExecutionStatsForDataStore);

    accessor.invoke(closeDistributedSystem);
    datastore0.invoke(closeDistributedSystem);
    datastore1.invoke(closeDistributedSystem);
    datastore2.invoke(closeDistributedSystem);
  }

  /**
   * Test the function execution statistics in case of the distributed Region P2P DataStore0 is with
   * Empty datapolicy
   */
  @Test
  public void testP2PDistributedRegionFunctionExecutionStats() {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);

    datastore0.invoke(initializeStats);
    datastore1.invoke(initializeStats);
    datastore2.invoke(initializeStats);
    datastore3.invoke(initializeStats);

    datastore0.invoke(() -> {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.EMPTY);
      Region<String, Integer> region = getCache().createRegion(rName, factory.create());
      logger.info("Region Created :" + region);
      assertNotNull(region);
      FunctionService.registerFunction(new TestFunction(true, TestFunction.TEST_FUNCTION2));
      for (int i = 1; i <= 200; i++) {
        region.put("execKey-" + i, i);
      }
    });

    SerializableRunnableIF createAndPopulateRegionWithReplicate = () -> {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      Region<String, Integer> region = getCache().createRegion(rName, factory.create());
      logger.info("Region Created :" + region);
      assertNotNull(region);
      FunctionService.registerFunction(new TestFunction(true, TestFunction.TEST_FUNCTION2));
      for (int i = 1; i <= 200; i++) {
        region.put("execKey-" + i, i);
      }
    };

    datastore1.invoke(createAndPopulateRegionWithReplicate);
    datastore2.invoke(createAndPopulateRegionWithReplicate);
    datastore3.invoke(createAndPopulateRegionWithReplicate);

    datastore0.invoke(() -> {
      Region region = getCache().getRegion(rName);
      try {
        List list = (List) FunctionService.onRegion(region).setArguments(Boolean.TRUE)
            .execute(TestFunction.TEST_FUNCTION2).getResult();
        // this is the Distributed Region with Empty Data policy.
        // therefore no function execution takes place here. it only receives the results.
        resultReceived_Aggregate += list.size();
        assertEquals(resultReceived_Aggregate,
            ((InternalDistributedSystem) getCache().getDistributedSystem())
                .getFunctionStatsManager().getFunctionServiceStats().getResultsReceived());

        resultReceived_TESTFUNCTION2 += list.size();
        assertEquals(resultReceived_TESTFUNCTION2,
            ((InternalDistributedSystem) getCache().getDistributedSystem())
                .getFunctionStatsManager().getFunctionServiceStats().getResultsReceived());

      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("test failed due to", e);
      }
    });

    // there is a replicated region on 3 nodes so we cannot predict on which
    // node the function execution will take place
    // so i have avoided that check.

    datastore0.invoke(closeDistributedSystem);
    datastore1.invoke(closeDistributedSystem);
    datastore2.invoke(closeDistributedSystem);
    datastore3.invoke(closeDistributedSystem);
  }


  /**
   * Test the execution of function on all memebers haveResults = true
   *
   * member1 calls for the function executions sp the results received on memeber 1 should be equal
   * to the no of function execution calls. Function Execution should happen on all other members
   * too. so the no of function execution calls and no of function executions completed should be
   * equal tio the no of functions from member 1
   */
  @Test
  public void testP2PMembersFunctionExecutionStats() {
    Host host = Host.getHost(0);
    VM member1 = host.getVM(0);
    VM member2 = host.getVM(1);
    VM member3 = host.getVM(2);
    VM member4 = host.getVM(3);

    SerializableRunnableIF connectToDistributedSystem = () -> {
      Properties props = new Properties();
      try {
        ds = getSystem(props);
        assertNotNull(ds);
      } catch (Exception e) {
        Assert.fail("Failed while creating the Distribued System", e);
      }
    };
    member1.invoke(connectToDistributedSystem);
    member2.invoke(connectToDistributedSystem);
    member3.invoke(connectToDistributedSystem);
    member4.invoke(connectToDistributedSystem);

    member1.invoke(initializeStats);
    member2.invoke(initializeStats);
    member3.invoke(initializeStats);
    member4.invoke(initializeStats);

    final Function inlineFunction = new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        @SuppressWarnings("unchecked")
        final ResultSender<String> resultSender = context.getResultSender();
        if (context.getArguments() instanceof String) {
          resultSender.lastResult("Success");
        } else {
          resultSender.lastResult("Failure");
        }
      }

      @Override
      public String getId() {
        return getClass().getName();
      }

      @Override
      public boolean hasResult() {
        return true;
      }
    };

    member1.invoke(() -> {

      assertNotNull(ds);
      DistributedMember localmember = ds.getDistributedMember();
      Execution memberExecution = FunctionService.onMember(localmember);

      memberExecution.setArguments("Key");
      try {
        ResultCollector rc = memberExecution.execute(inlineFunction);
        int size = ((List) rc.getResult()).size();
        resultReceived_Aggregate += size;
        noOfExecutionCalls_Aggregate++;
        noOfExecutionsCompleted_Aggregate++;
        resultReceived_Inline += size;
        noOfExecutionCalls_Inline++;
        noOfExecutionsCompleted_Inline++;

      } catch (Exception e) {
        logger.info("Exception Occurred : " + e.getMessage());
        e.printStackTrace();
        Assert.fail("Test failed", e);
      }
    });

    member1.invoke(() -> {
      FunctionServiceStats functionServiceStats =
          ds.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Aggregate,
          functionServiceStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_Aggregate, functionServiceStats.getResultsReceived());

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(inlineFunction.getId(), ds);
      assertEquals(noOfExecutionCalls_Inline, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Inline,
          functionStats.getFunctionExecutionsCompleted());
      assertEquals(resultReceived_Inline, functionStats.getResultsReceived());
    });

    SerializableRunnableIF checkFunctionExecutionStatsForOtherMember = () -> {
      FunctionServiceStats functionServiceStats =
          ds.getFunctionStatsManager().getFunctionServiceStats();
      waitNoFunctionsRunning(functionServiceStats);

      // One function Execution took place on there members
      // noOfExecutionCalls_Aggregate++;
      // noOfExecutionsCompleted_Aggregate++;
      assertEquals(noOfExecutionCalls_Aggregate,
          functionServiceStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Aggregate,
          functionServiceStats.getFunctionExecutionsCompleted());

      FunctionStats functionStats =
          FunctionStatsManager.getFunctionStats(inlineFunction.getId(), ds);
      // noOfExecutionCalls_Inline++;
      // noOfExecutionsCompleted_Inline++;
      assertEquals(noOfExecutionCalls_Inline, functionStats.getFunctionExecutionCalls());
      assertEquals(noOfExecutionsCompleted_Inline,
          functionStats.getFunctionExecutionsCompleted());
    };
    member2.invoke(checkFunctionExecutionStatsForOtherMember);
    member3.invoke(checkFunctionExecutionStatsForOtherMember);
    member4.invoke(checkFunctionExecutionStatsForOtherMember);

    member1.invoke(closeDistributedSystem);
    member2.invoke(closeDistributedSystem);
    member3.invoke(closeDistributedSystem);
    member4.invoke(closeDistributedSystem);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.MyFunctionExecutionException");
    return properties;
  }

  /**
   * Test the exception occurred while invoking the function execution on all members of DS
   *
   * Function throws the Exception, The check is added to for the no of function execution execption
   * in datatostore1
   */
  @Test
  public void testFunctionExecutionExceptionStatsOnAllNodesPRegion() {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);

    datastore0.invoke(initializeStats);
    datastore1.invoke(initializeStats);
    datastore2.invoke(initializeStats);
    datastore3.invoke(initializeStats);

    SerializableRunnableIF dataStoreCreate = () -> {
      RegionAttributes ra = PartitionedRegionTestHelper.createRegionAttrsForPR(0, 10);
      AttributesFactory raf = new AttributesFactory(ra);
      PartitionAttributesImpl pa = new PartitionAttributesImpl();
      pa.setAll(ra.getPartitionAttributes());
      pa.setTotalNumBuckets(17);
      raf.setPartitionAttributes(pa);
      getCache().createRegion(rName, raf.create());
      Function function = new TestFunction(true, "TestFunctionException");
      FunctionService.registerFunction(function);
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);
    datastore3.invoke(dataStoreCreate);

    datastore3.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(rName);
      DistributedSystem.setThreadsSocketPolicy(false);
      final HashSet<String> testKeys = new HashSet<>();
      for (int i = (pr.getTotalNumberOfBuckets() * 3); i > 0; i--) {
        testKeys.add("execKey-" + i);
      }
      int j = 0;
      for (Object testKey : testKeys) {
        Integer key = j++;
        pr.put(key, testKey);
      }
      try {
        Function function = new TestFunction(true, "TestFunctionException");
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc = dataSet.setArguments(Boolean.TRUE).execute(function.getId());
        // Wait Criterion is added to make sure that the function execution
        // happens on all nodes and all nodes get the FunctionException so that the stats will get
        // incremented,
        Wait.pause(2000);
        rc.getResult();
        fail("No exception Occurred");
      } catch (Exception ignored) {
      }
    });

    datastore0.invoke(closeDistributedSystem);
    datastore1.invoke(closeDistributedSystem);
    datastore2.invoke(closeDistributedSystem);
    datastore3.invoke(closeDistributedSystem);
  }

  private void createScenario() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
  }
}
