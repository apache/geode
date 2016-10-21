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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.rmi.ServerException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

@Category(DistributedTest.class)
public class PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
    extends PRClientServerTestBase {
  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;

  private static final String TEST_FUNCTION2 = TestFunction.TEST_FUNCTION2;

  Boolean isByName = null;

  private static int retryCount = 0;

  public PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest() {
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
    isByName = new Boolean(false);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverAllKeyExecution(isByName));
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerGetAllFunction() {
    createScenario();
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.getAll());
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerPutAllFunction() {
    createScenario();
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.putAll());
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
    isByName = new Boolean(true);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
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
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverSingleKeyExecution_FunctionInvocationTargetException());
  }

  @Test
  public void testServerSingleKeyExecution_SocketTimeOut() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
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
    isByName = new Boolean(false);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverSingleKeyExecution(isByName));
  }

  /*
   * Execution of the inline function on server with single key as the routing object
   */
  @Test
  public void testServerSingleKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverSingleKeyExecution_Inline());
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
    isByName = new Boolean(true);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverMultiKeyExecution(isByName));
    server1.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .checkBucketsOnServer());
    server2.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .checkBucketsOnServer());
    server3.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .checkBucketsOnServer());
  }

  @Test
  public void testserverMultiKeyExecution_SocektTimeOut() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverMultiKeyExecutionSocketTimeOut(isByName));
  }

  /*
   * Execution of the inline function on server with set multiple keys as the routing object
   */
  @Test
  public void testserverMultiKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverMultiKeyExecution_Inline());
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
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverMultiKeyExecution_FunctionInvocationTargetException());
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
    isByName = new Boolean(true);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
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
    isByName = new Boolean(false);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
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
    isByName = new Boolean(true);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
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
    isByName = new Boolean(false);
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .serverMultiKeyExecutionOnASingleBucket(isByName));
  }

  /*
   * Ensure that the while executing the function if the servers is down then the execution is
   * failover to other available server
   */
  @Category(FlakyTest.class) // GEODE-1497
  @Test
  public void testServerFailoverWithTwoServerAliveHA() throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("SocketTimeoutException");
    IgnoredException.addIgnoredException("ServerConnectivityException");
    IgnoredException.addIgnoredException("Socket Closed");
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 1, 13, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.stopServerHA());
    server3.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.stopServerHA());
    client.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.putOperation());

    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] =
        client.invokeAsync(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
            .executeFunctionHA());
    server2.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.startServerHA());
    server3.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.startServerHA());
    server1.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.stopServerHA());
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .verifyDeadAndLiveServers(new Integer(1), new Integer(2)));
    ThreadUtils.join(async[0], 6 * 60 * 1000);
    if (async[0].getException() != null) {
      Assert.fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List) async[0].getReturnValue();

    assertEquals(2, l.size());
  }

  /*
   * Ensure that the while executing the function if the servers is down then the execution is
   * failover to other available server
   */
  @Test
  public void testServerCacheClosedFailoverWithTwoServerAliveHA() throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("SocketTimeoutException");
    IgnoredException.addIgnoredException("ServerConnectivityException");
    IgnoredException.addIgnoredException("Socket Closed");
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 1, 13, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.stopServerHA());
    server3.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.stopServerHA());
    client.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.putOperation());
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] =
        client.invokeAsync(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
            .executeFunctionHA());
    server2.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.startServerHA());
    server3.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.startServerHA());
    server1.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.closeCacheHA());
    client.invoke(() -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest
        .verifyDeadAndLiveServers(new Integer(1), new Integer(2)));
    ThreadUtils.join(async[0], 5 * 60 * 1000);
    if (async[0].getException() != null) {
      Assert.fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List) async[0].getReturnValue();
    assertEquals(2, l.size());
  }

  @Test
  public void testBug40714() {
    createScenario();
    server1.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.registerFunction());
    server1.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.registerFunction());
    server1.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.registerFunction());
    client.invoke(
        () -> PRClientServerRegionFunctionExecutionSelectorNoSingleHopDUnitTest.registerFunction());
    client.invoke(
        () -> PRClientServerRegionFunctionExecutionDUnitTest.FunctionExecution_Inline_Bug40714());
  }

  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
      public void execute(FunctionContext context) {
        if (context.getArguments() instanceof String) {
          context.getResultSender().lastResult("Failure");
        } else if (context.getArguments() instanceof Boolean) {
          context.getResultSender().lastResult(Boolean.FALSE);
        }
      }

      public String getId() {
        return "Function";
      }

      public boolean hasResult() {
        return true;
      }
    });
  }

  public static void FunctionExecution_Inline_Bug40714() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      region.put(i.next(), val);
    }
    HashMap resultMap = (HashMap) FunctionService.onRegion(region).withArgs(Boolean.TRUE)
        .execute(new FunctionAdapter() {
          public void execute(FunctionContext context) {
            if (context.getArguments() instanceof String) {
              context.getResultSender().lastResult("Success");
            } else if (context.getArguments() instanceof Boolean) {
              context.getResultSender().lastResult(Boolean.TRUE);
            }
          }

          public String getId() {
            return "Function";
          }

          public boolean hasResult() {
            return true;
          }
        }).getResult();

    assertEquals(3, resultMap.size());

    Iterator mapIterator = resultMap.entrySet().iterator();
    Map.Entry entry = null;
    DistributedMember key = null;
    ArrayList resultListForMember = null;

    while (mapIterator.hasNext()) {
      entry = (Map.Entry) mapIterator.next();
      key = (DistributedMember) entry.getKey();
      resultListForMember = (ArrayList) entry.getValue();

      for (Object result : resultListForMember) {
        assertEquals(Boolean.TRUE, result);
      }
    }

  }

  public static void verifyDeadAndLiveServers(final Integer expectedDeadServers,
      final Integer expectedLiveServers) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        int sz = pool.getConnectedServerCount();
        LogWriterUtils.getLogWriter().info("Checking for the Live Servers : Expected  : "
            + expectedLiveServers + " Available :" + sz);
        if (sz == expectedLiveServers.intValue()) {
          return true;
        }
        excuse = "Expected " + expectedLiveServers.intValue() + " but found " + sz;
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);
  }

  public static void executeFunction() throws ServerException, InterruptedException {

    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      ResultCollector rc1 =
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());

      HashMap resultMap = ((HashMap) rc1.getResult());
      assertEquals(3, resultMap.size());

      Iterator mapIterator = resultMap.entrySet().iterator();
      Map.Entry entry = null;
      DistributedMember key = null;
      ArrayList resultListForMember = null;

      while (mapIterator.hasNext()) {
        entry = (Map.Entry) mapIterator.next();
        key = (DistributedMember) entry.getKey();
        resultListForMember = (ArrayList) entry.getValue();

        for (Object result : resultListForMember) {
          assertEquals(Boolean.TRUE, result);
        }
      }
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Got an exception : " + e.getMessage());
      assertTrue(e instanceof EOFException || e instanceof SocketException
          || e instanceof SocketTimeoutException || e instanceof ServerException
          || e instanceof IOException || e instanceof CacheClosedException);
    }
  }

  public static Object executeFunctionHA() throws Exception {
    Region region = cache.getRegion(PartitionedRegionName);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    ResultCollector rc1 =
        dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());
    List l = ((List) rc1.getResult());
    LogWriterUtils.getLogWriter().info("Result size : " + l.size());
    return l;
  }

  public static void putOperation() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }
  }

  private void createScenario() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, 13, null);
    createClientServerScenarioSelectorNoSingleHop(commonAttributes, 20, 20, 20);
  }

  public static void checkBucketsOnServer() {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(PartitionedRegionName);
    HashMap localBucket2RegionMap = (HashMap) region.getDataStore().getSizeLocally();
    LogWriterUtils.getLogWriter().info(
        "Size of the " + PartitionedRegionName + " in this VM :- " + localBucket2RegionMap.size());
    Set entrySet = localBucket2RegionMap.entrySet();
    assertNotNull(entrySet);
  }

  public static void serverAllKeyExecution(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() / 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      ResultCollector rc1 = executeOnAll(dataSet, Boolean.TRUE, function, isByName);
      List resultList = (List) ((List) rc1.getResult());
      LogWriterUtils.getLogWriter().info("Result size : " + resultList.size());
      LogWriterUtils.getLogWriter().info("Result are SSSS : " + resultList);
      assertEquals(3, resultList.size());

      Iterator resultIterator = resultList.iterator();
      Map.Entry entry = null;
      DistributedMember key = null;
      List resultListForMember = new ArrayList();

      // while (resultIterator.hasNext()) {
      // resultListForMember.add(resultIterator.next());
      //
      // for (Object result : resultListForMember) {
      // assertIndexDetailsEquals(Boolean.TRUE, result);
      // }
      // }
      for (Object result : resultList) {
        assertEquals(Boolean.TRUE, result);
      }
      List l2 = null;
      ResultCollector rc2 = executeOnAll(dataSet, testKeysSet, function, isByName);
      l2 = ((List) rc2.getResult());
      assertEquals(3, l2.size());
      HashSet foundVals = new HashSet();
      for (Iterator i = l2.iterator(); i.hasNext();) {
        ArrayList subL = (ArrayList) (i.next());
        assertTrue(subL.size() > 0);
        for (Iterator subI = subL.iterator(); subI.hasNext();) {
          assertTrue(foundVals.add(subI.next()));
        }
      }
      assertEquals(origVals, foundVals);

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }

  public static void getAll() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final List testKeysList = new ArrayList();
    for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      int j = 0;
      Map origVals = new HashMap();
      for (Iterator i = testKeysList.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        Object key = i.next();
        origVals.put(key, val);
        region.put(key, val);
      }
      Map resultMap = region.getAll(testKeysList);
      assertTrue(resultMap.equals(origVals));
      Wait.pause(2000);
      Map secondResultMap = region.getAll(testKeysList);
      assertTrue(secondResultMap.equals(origVals));

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }

  public static void putAll() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final List testKeysList = new ArrayList();
    for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      int j = 0;
      Map origVals = new HashMap();
      for (Iterator i = testKeysList.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        Object key = i.next();
        origVals.put(key, val);
        region.put(key, val);
      }
      Map resultMap = region.getAll(testKeysList);
      assertTrue(resultMap.equals(origVals));
      Wait.pause(2000);
      Map secondResultMap = region.getAll(testKeysList);
      assertTrue(secondResultMap.equals(origVals));

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }

  public static void serverMultiKeyExecutionOnASingleBucket(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      region.put(i.next(), val);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    for (Iterator kiter = testKeysSet.iterator(); kiter.hasNext();) {
      try {
        Set singleKeySet = Collections.singleton(kiter.next());
        Function function = new TestFunction(true, TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(region);
        ResultCollector rc1 = execute(dataSet, singleKeySet, Boolean.TRUE, function, isByName);
        List l = null;
        l = ((List) rc1.getResult());
        assertEquals(1, l.size());

        ResultCollector rc2 =
            execute(dataSet, singleKeySet, new HashSet(singleKeySet), function, isByName);
        List l2 = null;
        l2 = ((List) rc2.getResult());

        assertEquals(1, l2.size());
        List subList = (List) l2.iterator().next();
        assertEquals(1, subList.size());
        assertEquals(region.get(singleKeySet.iterator().next()), subList.iterator().next());
      } catch (Exception expected) {
        LogWriterUtils.getLogWriter().info("Exception : " + expected.getMessage());
        expected.printStackTrace();
        fail("Test failed after the put operation");
      }
    }
  }

  public static void serverMultiKeyExecution(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      List l = null;
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      l = ((List) rc1.getResult());
      LogWriterUtils.getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Iterator i = l.iterator(); i.hasNext();) {
        assertEquals(Boolean.TRUE, i.next());
      }

      List l2 = null;
      ResultCollector rc2 = execute(dataSet, testKeysSet, testKeysSet, function, isByName);
      l2 = ((List) rc2.getResult());
      assertEquals(3, l2.size());
      HashSet foundVals = new HashSet();
      for (Iterator i = l2.iterator(); i.hasNext();) {
        ArrayList subL = (ArrayList) i.next();
        assertTrue(subL.size() > 0);
        for (Iterator subI = subL.iterator(); subI.hasNext();) {
          assertTrue(foundVals.add(subI.next()));
        }
      }
      assertEquals(origVals, foundVals);

    } catch (Exception e) {
      Assert.fail("Test failed after the put operation", e);

    }
  }

  public static void serverMultiKeyExecutionSocketTimeOut(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      List l = null;
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      l = ((List) rc1.getResult());
      LogWriterUtils.getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Iterator i = l.iterator(); i.hasNext();) {
        assertEquals(Boolean.TRUE, i.next());
      }

    } catch (Exception e) {
      Assert.fail("Test failed after the function execution", e);

    }
  }

  public static void serverSingleKeyExecutionSocketTimeOut(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

      ResultCollector rs2 = execute(dataSet, testKeysSet, testKey, function, isByName);
      assertEquals(testKey, ((List) rs2.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      Assert.fail("Test failed after the put operation", ex);
    }
  }

  public static void serverMultiKeyExecution_Inline() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      List l = null;
      ResultCollector rc1 =
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter() {
            public void execute(FunctionContext context) {
              if (context.getArguments() instanceof String) {
                context.getResultSender().lastResult("Success");
              } else if (context.getArguments() instanceof Boolean) {
                context.getResultSender().lastResult(Boolean.TRUE);
              }
            }

            public String getId() {
              return getClass().getName();
            }

            public boolean hasResult() {
              return true;
            }
          });
      l = ((List) rc1.getResult());
      LogWriterUtils.getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Iterator i = l.iterator(); i.hasNext();) {
        assertEquals(Boolean.TRUE, i.next());
      }
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception : " + e.getMessage());
      e.printStackTrace();
      fail("Test failed after the put operation");

    }
  }

  public static void serverMultiKeyExecution_FunctionInvocationTargetException() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution dataSet = FunctionService.onRegion(region);
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }
    ResultCollector rc1 = null;
    try {
      rc1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter() {
        public void execute(FunctionContext context) {
          if (((RegionFunctionContext) context).isPossibleDuplicate()) {
            context.getResultSender().lastResult(new Integer(retryCount));
            return;
          }
          if (context.getArguments() instanceof Boolean) {
            throw new FunctionInvocationTargetException("I have been thrown from TestFunction");
          }
        }

        public String getId() {
          return getClass().getName();
        }

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

  public static void serverMultiKeyExecutionNoResult(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
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
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      rc1.getResult();
      Thread.sleep(20000);
      fail("Test failed after the put operation");
    } catch (FunctionException expected) {
      expected.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : " + expected.getMessage());
      assertTrue(expected.getMessage()
          .startsWith((LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("return any"))));
    } catch (Exception notexpected) {
      Assert.fail("Test failed during execute or sleeping", notexpected);
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "FunctionException" + "</ExpectedException>");
    }
  }

  public static void serverSingleKeyExecution(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
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

    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

      ResultCollector rs2 = execute(dataSet, testKeysSet, testKey, function, isByName);
      assertEquals(new Integer(1), ((List) rs2.getResult()).get(0));

      HashMap putData = new HashMap();
      putData.put(testKey + "1", new Integer(2));
      putData.put(testKey + "2", new Integer(3));

      ResultCollector rs1 = execute(dataSet, testKeysSet, putData, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs1.getResult()).get(0));

      assertEquals(new Integer(2), region.get(testKey + "1"));
      assertEquals(new Integer(3), region.get(testKey + "2"));

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      Assert.fail("Test failed after the put operation", ex);
    }
  }

  public static void serverSingleKeyExecution_FunctionInvocationTargetException() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, false);
      ArrayList list = (ArrayList) rs.getResult();
      assertTrue(((Integer) list.get(0)) >= 5);
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("This is not expected Exception", ex);
    }
  }

  public static void serverSingleKeyExecution_Inline() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Execution dataSet = FunctionService.onRegion(region);
    try {
      cache.getLogger()
          .info("<ExpectedException action=add>" + "No target node found for KEY = "
              + "|Server could not send the reply" + "|Unexpected exception during"
              + "</ExpectedException>");
      dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter() {
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          }
          context.getResultSender().lastResult("Failure");
        }

        public String getId() {
          return getClass().getName();
        }

        public boolean hasResult() {
          return true;
        }
      });
    } catch (Exception expected) {
      LogWriterUtils.getLogWriter().fine("Exception occured : " + expected.getMessage());
      assertTrue(expected.getMessage().contains("No target node found for KEY = " + testKey)
          || expected.getMessage().startsWith("Server could not send the reply")
          || expected.getMessage().startsWith("Unexpected exception during"));
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "No target node found for KEY = "
              + "|Server could not send the reply" + "|Unexpected exception during"
              + "</ExpectedException>");
    }

    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs =
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter() {
            public void execute(FunctionContext context) {
              if (context.getArguments() instanceof String) {
                context.getResultSender().lastResult("Success");
              } else {
                context.getResultSender().lastResult("Failure");
              }
            }

            public String getId() {
              return getClass().getName();
            }

            public boolean hasResult() {
              return true;
            }
          });
      assertEquals("Failure", ((List) rs.getResult()).get(0));

      ResultCollector rs2 =
          dataSet.withFilter(testKeysSet).withArgs(testKey).execute(new FunctionAdapter() {
            public void execute(FunctionContext context) {
              if (context.getArguments() instanceof String) {
                context.getResultSender().lastResult("Success");
              } else {
                context.getResultSender().lastResult("Failure");
              }
            }

            public String getId() {
              return getClass().getName();
            }

            public boolean hasResult() {
              return true;
            }
          });
      assertEquals("Success", ((List) rs2.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      Assert.fail("Test failed after the put operation", ex);
    }
  }

  private static ResultCollector execute(Execution dataSet, Set testKeysSet, Serializable args,
      Function function, Boolean isByName) throws Exception {
    if (isByName.booleanValue()) {// by name
      return dataSet.withFilter(testKeysSet).withArgs(args).execute(function.getId());
    } else { // By Instance
      return dataSet.withFilter(testKeysSet).withArgs(args).execute(function);
    }
  }

  private static ResultCollector executeOnAll(Execution dataSet, Serializable args,
      Function function, Boolean isByName) throws Exception {
    if (isByName.booleanValue()) {// by name
      return dataSet.withArgs(args).execute(function.getId());
    } else { // By Instance
      return dataSet.withArgs(args).execute(function);
    }
  }
}
