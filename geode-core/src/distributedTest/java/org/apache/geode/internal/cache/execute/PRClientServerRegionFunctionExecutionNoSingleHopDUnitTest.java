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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ClientServerTest.class, FunctionServiceTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
    extends PRClientServerTestBase {

  private static final Logger logger = LogService.getLogger();

  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;

  private static final String TEST_FUNCTION2 = TestFunction.TEST_FUNCTION2;

  private Boolean isByName = null;

  private static final int retryCount = 0;

  public PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest() {
    super();
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerAllKeyExecution_byInstance() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverAllKeyExecution(isByName));
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerGetAllFunction() {
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::getAll);
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerPutAllFunction() {
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::putAll);
  }

  /*
   * Execution of the function on server with single key as the routing object and using the name of
   * the function
   */
  @Test
  public void testServerSingleKeyExecution_byName() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverSingleKeyExecution(isByName));
  }

  /*
   * Execution of the function on server with single key as the routing. Function throws the
   * FunctionInvocationTargetException. As this is the case of HA then system should retry the
   * function execution. After 5th attempt function will send Boolean as last result.
   */
  @Test
  public void testserverSingleKeyExecution_FunctionInvocationTargetException() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::serverSingleKeyExecution_FunctionInvocationTargetException);
  }

  /*
   * Execution of the function on server with bucket as filter
   */
  @Test
  public void testBucketFilter() {
    createScenarioForBucketFilter();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    registerFunctionAtServer(function);

    Set<Integer> bucketFilterSet = new HashSet<>();
    bucketFilterSet.add(3);
    bucketFilterSet.add(6);
    bucketFilterSet.add(8);
    client.invoke(() -> serverBucketFilterExecution(bucketFilterSet));
    bucketFilterSet.clear();
    // Test single filter
    bucketFilterSet.add(7);
    client.invoke(() -> serverBucketFilterExecution(bucketFilterSet));
  }

  @Test
  public void testBucketFilterOverride() {
    createScenarioForBucketFilter();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    registerFunctionAtServer(function);
    // test multi key filter
    Set<Integer> bucketFilterSet = new HashSet<>();
    bucketFilterSet.add(3);
    bucketFilterSet.add(6);
    bucketFilterSet.add(8);

    Set<Integer> keyFilterSet = new HashSet<>();
    keyFilterSet.add(75);
    keyFilterSet.add(25);

    client.invoke(() -> serverBucketFilterOverrideExecution(bucketFilterSet,
        keyFilterSet));

  }

  @Test
  public void testServerSingleKeyExecution_SocketTimeOut() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverSingleKeyExecutionSocketTimeOut(isByName));
  }

  /*
   * Execution of the function on server with single key as the routing object and using the
   * instance of the function
   */
  @Test
  public void testServerSingleKeyExecution_byInstance() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverSingleKeyExecution(isByName));
  }

  /*
   * Execution of the inline function on server with single key as the routing object
   */
  @Test
  public void testServerSingleKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::serverSingleKeyExecution_Inline);
  }

  /*
   * Execution of the function on server with set multiple keys as the routing object and using the
   * name of the function
   */
  @Test
  public void testserverMultiKeyExecution_byName() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverMultiKeyExecution(isByName));
    server1.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::checkBucketsOnServer);
    server2.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::checkBucketsOnServer);
    server3.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::checkBucketsOnServer);
  }

  @Test
  public void testserverMultiKeyExecution_SocektTimeOut() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverMultiKeyExecutionSocketTimeOut(isByName));
  }

  /*
   * Execution of the inline function on server with set multiple keys as the routing object
   */
  @Test
  public void testserverMultiKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::serverMultiKeyExecution_Inline);
  }

  /*
   * Execution of the inline function on server with set multiple keys as the routing object
   * Function throws the FunctionInvocationTargetException. As this is the case of HA then system
   * should retry the function execution. After 5th attempt function will send Boolean as last
   * result.
   */
  @Test
  public void testserverMultiKeyExecution_FunctionInvocationTargetException() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::serverMultiKeyExecution_FunctionInvocationTargetException);
  }

  /*
   * Execution of the function on server with set multiple keys as the routing object and using the
   * name of the function
   */
  @Test
  public void testserverMultiKeyExecutionNoResult_byName() {
    createScenario();
    Function function = new TestFunction(false, TEST_FUNCTION7);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverMultiKeyExecutionNoResult(isByName));
  }

  /*
   * Execution of the function on server with set multiple keys as the routing object and using the
   * instance of the function
   */
  @Test
  public void testserverMultiKeyExecution_byInstance() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverMultiKeyExecution(isByName));
  }

  /*
   * Ensure that the execution is limited to a single bucket put another way, that the routing logic
   * works correctly such that there is not extra execution
   */
  @Test
  public void testserverMultiKeyExecutionOnASingleBucket_byName() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverMultiKeyExecutionOnASingleBucket(isByName));
  }

  /*
   * Ensure that the execution is limited to a single bucket put another way, that the routing logic
   * works correctly such that there is not extra execution
   */
  @Test
  public void testserverMultiKeyExecutionOnASingleBucket_byInstance() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
        .serverMultiKeyExecutionOnASingleBucket(isByName));
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
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::stopServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::stopServerHA);
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::putOperation);

    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = client.invokeAsync(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::executeFunctionHA);
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::startServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::startServerHA);
    server1.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::stopServerHA);
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
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::stopServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::stopServerHA);
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::putOperation);
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = client.invokeAsync(
        PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::executeFunctionHA);
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::startServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::startServerHA);
    server1.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::closeCacheHA);
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest
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
    server1
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::registerFunction);
    server1
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::registerFunction);
    server1
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::registerFunction);
    client
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDUnitTest::registerFunction);
    client.invoke(
        PRClientServerRegionFunctionExecutionDUnitTest::FunctionExecution_Inline_Bug40714);
  }

  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        @SuppressWarnings("unchecked")
        final ResultSender<Object> resultSender = context.getResultSender();
        if (context.getArguments() instanceof String) {
          resultSender.lastResult("Failure");
        } else if (context.getArguments() instanceof Boolean) {
          resultSender.lastResult(Boolean.FALSE);
        }
      }

      @Override
      public String getId() {
        return "Function";
      }

      @Override
      public boolean hasResult() {
        return true;
      }
    });
  }

  public static void verifyDeadAndLiveServers(final Integer expectedLiveServers) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      @Override
      public boolean done() {
        int sz = pool.getConnectedServerCount();
        logger.info("Checking for the Live Servers : Expected  : " + expectedLiveServers
            + " Available :" + sz);
        if (sz == expectedLiveServers) {
          return true;
        }
        excuse = "Expected " + expectedLiveServers + " but found " + sz;
        return false;
      }

      @Override
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);
  }

  public static void executeFunction() {

    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      ResultCollector rc1 =
          dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(function.getId());

      HashMap resultMap = ((HashMap) rc1.getResult());
      assertEquals(3, resultMap.size());

      for (Object o : resultMap.entrySet()) {
        Map.Entry entry = (Map.Entry) o;
        ArrayList resultListForMember = (ArrayList) entry.getValue();

        for (Object result : resultListForMember) {
          assertEquals(Boolean.TRUE, result);
        }
      }
    } catch (Exception e) {
      logger.info("Got an exception : " + e.getMessage());
      assertTrue(e instanceof CacheClosedException);
    }
  }

  private static Object executeFunctionHA() {
    Region region = cache.getRegion(PartitionedRegionName);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    ResultCollector rc1 =
        dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(function.getId());
    List l = ((List) rc1.getResult());
    logger.info("Result size : " + l.size());
    return l;
  }

  private static void putOperation() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    for (String s : testKeysSet) {
      Integer val = j++;
      region.put(s, val);
    }
  }

  private void createScenario() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarioNoSingleHop(commonAttributes, 20, 20, 20);
  }

  private void createScenarioForBucketFilter() {
    ArrayList commonAttributes = createCommonServerAttributes("TestPartitionedRegion",
        new BucketFilterPRResolver(), 0, null);
    createClientServerScenarioNoSingleHop(commonAttributes, 20, 20, 20);
  }

  private static void checkBucketsOnServer() {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(PartitionedRegionName);
    HashMap localBucket2RegionMap = (HashMap) region.getDataStore().getSizeLocally();
    logger.info(
        "Size of the " + PartitionedRegionName + " in this VM :- " + localBucket2RegionMap.size());
    Set entrySet = localBucket2RegionMap.entrySet();
    assertNotNull(entrySet);
  }

  private static void serverAllKeyExecution(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets / 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet<Integer> origVals = new HashSet<>();
      for (String item : testKeysSet) {
        Integer val = j++;
        origVals.add(val);
        region.put(item, val);
      }
      ResultCollector rc1 = executeOnAll(dataSet, Boolean.TRUE, function, isByName);
      List resultList = (List) rc1.getResult();
      logger.info("Result size : " + resultList.size());
      logger.info("Result are SSSS : " + resultList);
      assertEquals(3, resultList.size());

      for (Object result : resultList) {
        assertEquals(Boolean.TRUE, result);
      }
      ResultCollector rc2 = executeOnAll(dataSet, testKeysSet, function, isByName);
      List l2 = ((List) rc2.getResult());
      assertEquals(3, l2.size());
      HashSet<Integer> foundVals = new HashSet<>();
      for (Object value : l2) {
        ArrayList subL = (ArrayList) (value);
        assertTrue(subL.size() > 0);
        for (Object o : subL) {
          assertTrue(foundVals.add((Integer) o));
        }
      }
      assertEquals(origVals, foundVals);

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }

  public static void getAll() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final List<String> testKeysList = new ArrayList<>();
    for (int i = (totalNumBuckets * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      int j = 0;
      Map<String, Integer> origVals = new HashMap<>();
      for (String key : testKeysList) {
        Integer val = j++;
        origVals.put(key, val);
        region.put(key, val);
      }
      Map resultMap = region.getAll(testKeysList);
      assertEquals(resultMap, origVals);
      Wait.pause(2000);
      Map secondResultMap = region.getAll(testKeysList);
      assertEquals(secondResultMap, origVals);

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }

  public static void putAll() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final List<String> testKeysList = new ArrayList<>();
    for (int i = (totalNumBuckets * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      int j = 0;
      Map<String, Integer> origVals = new HashMap<>();
      for (String key : testKeysList) {
        Integer val = j++;
        origVals.put(key, val);
        region.put(key, val);
      }
      Map resultMap = region.getAll(testKeysList);
      assertEquals(resultMap, origVals);
      Wait.pause(2000);
      Map secondResultMap = region.getAll(testKeysList);
      assertEquals(secondResultMap, origVals);

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }

  private static void serverMultiKeyExecutionOnASingleBucket(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    for (String value : testKeysSet) {
      Integer val = j++;
      region.put(value, val);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    for (String o : testKeysSet) {
      try {
        Set<String> singleKeySet = Collections.singleton(o);
        Function function = new TestFunction(true, TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(region);
        ResultCollector rc1 = execute(dataSet, singleKeySet, Boolean.TRUE, function, isByName);
        List l = ((List) rc1.getResult());
        assertEquals(1, l.size());

        ResultCollector rc2 =
            execute(dataSet, singleKeySet, new HashSet<>(singleKeySet), function, isByName);
        List l2 = ((List) rc2.getResult());

        assertEquals(1, l2.size());
        List subList = (List) l2.iterator().next();
        assertEquals(1, subList.size());
        assertEquals(region.get(singleKeySet.iterator().next()), subList.iterator().next());
      } catch (Exception expected) {
        logger.info("Exception : " + expected.getMessage());
        expected.printStackTrace();
        fail("Test failed after the put operation");
      }
    }
  }

  private static void serverMultiKeyExecution(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet<Integer> origVals = new HashSet<>();
      for (String element : testKeysSet) {
        Integer val = j++;
        origVals.add(val);
        region.put(element, val);
      }
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      List l = ((List) rc1.getResult());
      logger.info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Object item : l) {
        assertEquals(Boolean.TRUE, item);
      }

      ResultCollector rc2 = execute(dataSet, testKeysSet, testKeysSet, function, isByName);
      List l2 = ((List) rc2.getResult());
      assertEquals(3, l2.size());
      HashSet<Integer> foundVals = new HashSet<>();
      for (Object value : l2) {
        ArrayList subL = (ArrayList) value;
        assertTrue(subL.size() > 0);
        for (Object o : subL) {
          assertTrue(foundVals.add((Integer) o));
        }
      }
      assertEquals(origVals, foundVals);

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }


  private static void serverMultiKeyExecutionSocketTimeOut(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      for (String value : testKeysSet) {
        Integer val = j++;
        region.put(value, val);
      }
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      List l = ((List) rc1.getResult());
      logger.info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Object o : l) {
        assertEquals(Boolean.TRUE, o);
      }

    } catch (Exception e) {
      Assert.fail("Test failed after the function execution", e);

    }
  }

  private static void serverSingleKeyExecutionSocketTimeOut(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, 1);
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

      ResultCollector rs2 = execute(dataSet, testKeysSet, testKey, function, isByName);
      assertEquals(testKey, ((List) rs2.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      Assert.fail("Test failed after the put operation", ex);
    }
  }

  private static void serverMultiKeyExecution_Inline() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      for (String value : testKeysSet) {
        Integer val = j++;
        region.put(value, val);
      }
      ResultCollector rc1 =
          dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
            @Override
            public void execute(FunctionContext context) {
              @SuppressWarnings("unchecked")
              final ResultSender<Object> resultSender = context.getResultSender();
              if (context.getArguments() instanceof String) {
                resultSender.lastResult("Success");
              } else if (context.getArguments() instanceof Boolean) {
                resultSender.lastResult(Boolean.TRUE);
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
          });
      List l = ((List) rc1.getResult());
      logger.info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Object o : l) {
        assertEquals(Boolean.TRUE, o);
      }
    } catch (Exception e) {
      logger.info("Exception : " + e.getMessage());
      e.printStackTrace();
      fail("Test failed after the put operation");

    }
  }

  private static void serverMultiKeyExecution_FunctionInvocationTargetException() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution dataSet = FunctionService.onRegion(region);
    int j = 0;
    for (String o : testKeysSet) {
      Integer val = j++;
      region.put(o, val);
    }
    try {
      ResultCollector rc1 =
          dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
            @Override
            public void execute(FunctionContext context) {
              if (context.isPossibleDuplicate()) {
                context.getResultSender().lastResult(retryCount);
                return;
              }
              if (context.getArguments() instanceof Boolean) {
                throw new FunctionInvocationTargetException("I have been thrown from TestFunction");
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
          });

      List list = (ArrayList) rc1.getResult();
      assertEquals(list.get(0), 0);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail("This is not expected Exception", e);
    }

  }

  private static void serverMultiKeyExecutionNoResult(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(false, TEST_FUNCTION7);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      String msg = "<ExpectedException action=add>" + "FunctionException" + "</ExpectedException>";
      cache.getLogger().info(msg);
      int j = 0;
      for (String o : testKeysSet) {
        Integer val = j++;
        region.put(o, val);
      }
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      rc1.getResult();
      Thread.sleep(20000);
      fail("Test failed after the put operation");
    } catch (FunctionException expected) {
      expected.printStackTrace();
      logger.info("Exception : " + expected.getMessage());
      assertTrue(expected.getMessage()
          .startsWith((String.format("Cannot %s result as the Function#hasResult() is false",
              "return any"))));
    } catch (Exception notexpected) {
      Assert.fail("Test failed during execute or sleeping", notexpected);
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "FunctionException" + "</ExpectedException>");
    }
  }

  private static void serverSingleKeyExecution(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    } catch (Exception expected) {
      assertTrue(expected.getMessage().contains("No target node found for KEY = " + testKey)
          || expected.getMessage().startsWith("Server could not send the reply")
          || expected.getMessage().startsWith("Unexpected exception during"));
    }

    region.put(testKey, 1);
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

      ResultCollector rs2 = execute(dataSet, testKeysSet, testKey, function, isByName);
      assertEquals(1, ((List) rs2.getResult()).get(0));

      HashMap<String, Integer> putData = new HashMap<>();
      putData.put(testKey + "1", 2);
      putData.put(testKey + "2", 3);

      ResultCollector rs1 = execute(dataSet, testKeysSet, putData, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs1.getResult()).get(0));

      assertEquals((Integer) 2, region.get(testKey + "1"));
      assertEquals((Integer) 3, region.get(testKey + "2"));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      Assert.fail("Test failed after the put operation", ex);
    }
  }

  private static void serverSingleKeyExecution_FunctionInvocationTargetException() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, 1);
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, false);
      ArrayList list = (ArrayList) rs.getResult();
      assertTrue(((Integer) list.get(0)) >= 5);
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("This is not expected Exception", ex);
    }
  }

  private static void serverSingleKeyExecution_Inline() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Execution dataSet = FunctionService.onRegion(region);
    try {
      cache.getLogger()
          .info("<ExpectedException action=add>" + "No target node found for KEY = "
              + "|Server could not send the reply" + "|Unexpected exception during"
              + "</ExpectedException>");
      dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext context) {
          @SuppressWarnings("unchecked")
          final ResultSender<Object> resultSender = context.getResultSender();
          if (context.getArguments() instanceof String) {
            resultSender.lastResult("Success");
          }
          resultSender.lastResult("Failure");
        }

        @Override
        public String getId() {
          return getClass().getName();
        }

        @Override
        public boolean hasResult() {
          return true;
        }
      });
    } catch (Exception expected) {
      logger.debug("Exception occurred : " + expected.getMessage());
      assertTrue(expected.getMessage().contains("No target node found for KEY = " + testKey)
          || expected.getMessage().startsWith("Server could not send the reply")
          || expected.getMessage().startsWith("Unexpected exception during"));
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "No target node found for KEY = "
              + "|Server could not send the reply" + "|Unexpected exception during"
              + "</ExpectedException>");
    }

    region.put(testKey, 1);
    try {
      ResultCollector rs =
          dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
            @Override
            public void execute(FunctionContext context) {
              @SuppressWarnings("unchecked")
              final ResultSender<Object> resultSender = context.getResultSender();
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
          });
      assertEquals("Failure", ((List) rs.getResult()).get(0));

      ResultCollector rs2 =
          dataSet.withFilter(testKeysSet).setArguments(testKey).execute(new FunctionAdapter() {
            @Override
            public void execute(FunctionContext context) {
              @SuppressWarnings("unchecked")
              final ResultSender<Object> resultSender = context.getResultSender();
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
          });
      assertEquals("Success", ((List) rs2.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      Assert.fail("Test failed after the put operation", ex);
    }
  }

  private static ResultCollector execute(Execution dataSet, Set testKeysSet, Serializable args,
      Function function, Boolean isByName) {
    if (isByName) {// by name
      return dataSet.withFilter(testKeysSet).setArguments(args).execute(function.getId());
    } else { // By Instance
      return dataSet.withFilter(testKeysSet).setArguments(args).execute(function);
    }
  }

  private static ResultCollector executeOnAll(Execution dataSet, Serializable args,
      Function function, Boolean isByName) {
    if (isByName) {// by name
      return dataSet.setArguments(args).execute(function.getId());
    } else { // By Instance
      return dataSet.setArguments(args).execute(function);
    }
  }
}
