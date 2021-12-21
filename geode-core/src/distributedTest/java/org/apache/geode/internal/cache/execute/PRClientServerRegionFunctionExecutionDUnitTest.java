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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
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
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ClientServerTest.class, FunctionServiceTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PRClientServerRegionFunctionExecutionDUnitTest extends PRClientServerTestBase {

  private static final Logger logger = LogService.getLogger();

  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;

  private static final String TEST_FUNCTION2 = TestFunction.TEST_FUNCTION2;
  private Boolean isByName = null;

  private static final int retryCount = 0;
  private Boolean toRegister = null;

  private static Region metaDataRegion;

  private static final String retryRegionName = "RetryDataRegion";

  @Test
  public void test_Bug_43126_Function_Not_Registered() {
    createScenario();
    try {
      client
          .invoke(PRClientServerRegionFunctionExecutionDUnitTest::executeRegisteredFunction);
    } catch (Exception e) {
      assertTrue((e.getCause() instanceof ServerOperationException));
      assertTrue(
          e.getCause().getMessage().contains("The function is not registered for function id"));
    }
  }

  @Test
  public void test_Bug43126() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::executeRegisteredFunction);
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
    toRegister = Boolean.TRUE;
    SerializableRunnable suspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=add>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverSingleKeyExecution(isByName, toRegister));
    SerializableRunnable endSuspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=remove>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
  }

  @Test
  public void testServerSingleKeyExecution_Bug43513_OnRegion() {
    createScenario_SingleConnection();
    client.invoke(
        PRClientServerRegionFunctionExecutionDUnitTest::serverSingleKeyExecutionOnRegion_SingleConnection);
  }

  @Test
  public void testServerSingleKeyExecution_SendException() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverSingleKeyExecution_SendException(isByName, toRegister));
  }

  @Test
  public void testServerSingleKeyExecution_ThrowException() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverSingleKeyExecution_ThrowException(isByName, toRegister));
  }

  @Test
  public void testClientWithoutPool_Bug41832() {
    createScenarioWith2Regions();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;
    SerializableRunnable suspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=add>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverSingleKeyExecutionWith2Regions(toRegister));
    SerializableRunnable endSuspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=remove>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
  }

  /*
   * Execution of the function on server with single key as the routing object and using the name of
   * the function
   */
  @Test
  public void testServerExecution_NoLastResult() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;

    final IgnoredException ex = IgnoredException.addIgnoredException("did not send last result");
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverSingleKeyExecution_NoLastResult(isByName, toRegister));
    ex.remove();
  }

  @Test
  public void testServerSingleKeyExecution_byName_WithoutRegister() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.FALSE;
    SerializableRunnable suspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=add>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverSingleKeyExecution(isByName, toRegister));
    SerializableRunnable endSuspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=remove>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
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
        PRClientServerRegionFunctionExecutionDUnitTest::serverSingleKeyExecution_FunctionInvocationTargetException);
  }

  @Test
  public void testServerSingleKeyExecution_SocketTimeOut() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
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
    toRegister = true;
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverSingleKeyExecution(isByName, toRegister));
  }

  /*
   * Execution of the inline function on server with single key as the routing object
   */
  @Test
  public void testServerSingleKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionDUnitTest::serverSingleKeyExecution_Inline);
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
    client.invoke(
        () -> PRClientServerRegionFunctionExecutionDUnitTest.serverMultiKeyExecution(isByName));
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest::checkBucketsOnServer);
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest::checkBucketsOnServer);
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest::checkBucketsOnServer);
  }

  /*
   * Execution of the function on server with bucket as filter
   */
  @Test
  public void testBucketFilter() {
    createScenarioForBucketFilter();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    registerFunctionAtServer(function);
    // test multi key filter
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

    client.invoke(() -> serverBucketFilterOverrideExecution(bucketFilterSet, keyFilterSet));

  }

  @Test
  public void testserverMultiKeyExecution_SendException() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverMultiKeyExecution_SendException(isByName));
  }

  @Test
  public void testserverMultiKeyExecution_ThrowException() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverMultiKeyExecution_ThrowException(isByName));
  }



  /*
   * Execution of the inline function on server with set multiple keys as the routing object
   */
  @Test
  public void testserverMultiKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionDUnitTest::serverMultiKeyExecution_Inline);
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
        PRClientServerRegionFunctionExecutionDUnitTest::serverMultiKeyExecution_FunctionInvocationTargetException);
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
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
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
    client.invoke(
        () -> PRClientServerRegionFunctionExecutionDUnitTest.serverMultiKeyExecution(isByName));
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
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
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
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .serverMultiKeyExecutionOnASingleBucket(isByName));
  }


  static void regionSingleKeyExecutionNonHA(Boolean isByName, Function function,
      Boolean toRegister) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, 1);
    try {
      ArrayList<String> args = new ArrayList<>();
      args.add(retryRegionName);
      args.add("regionSingleKeyExecutionNonHA");

      execute(dataSet, testKeysSet, args, function, isByName);
      fail("Expected ServerConnectivityException not thrown!");
    } catch (Exception ex) {
      if (!(ex.getCause() instanceof ServerConnectivityException)
          && !(ex.getCause() instanceof FunctionInvocationTargetException)) {
        throw ex;
      }
    }
  }

  static void regionExecutionHAOneServerDown(Boolean isByName, Function function,
      Boolean toRegister) {

    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, 1);

    ArrayList<String> args = new ArrayList<>();
    args.add(retryRegionName);
    args.add("regionExecutionHAOneServerDown");

    ResultCollector rs = execute(dataSet, testKeysSet, args, function, isByName);
    assertEquals(1, ((List) rs.getResult()).size());
  }

  static void regionExecutionHATwoServerDown(Boolean isByName, Function function,
      Boolean toRegister) {

    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, 1);

    ArrayList<String> args = new ArrayList<>();
    args.add(retryRegionName);
    args.add("regionExecutionHATwoServerDown");

    ResultCollector rs = execute(dataSet, testKeysSet, args, function, isByName);
    assertEquals(1, ((List) rs.getResult()).size());
  }

  static void createReplicatedRegion() {
    metaDataRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).create(retryRegionName);
  }

  public static void createProxyRegion() {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(pool.getName());
    RegionAttributes attrs = factory.create();
    metaDataRegion = cache.createRegion(retryRegionName, attrs);
    assertNotNull(metaDataRegion);
  }

  static void verifyMetaData(Integer arg1, Integer arg2) {

    if (arg1 == 0) {
      assertNull(metaDataRegion.get("stopped"));
    } else {
      assertEquals(metaDataRegion.get("stopped"), arg1);
    }

    if (arg2 == 0) {
      assertNull(metaDataRegion.get("sentresult"));
    } else {
      assertEquals(metaDataRegion.get("sentresult"), arg2);
    }
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

  static void FunctionExecution_Inline_Bug40714() {
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
    List list = (List) FunctionService.onRegion(region).setArguments(Boolean.TRUE)
        .execute(new FunctionAdapter() {
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
            return "Function";
          }

          @Override
          public boolean hasResult() {
            return true;
          }
        }).getResult();
    assertEquals(3, list.size());
    Iterator iterator = list.iterator();
    for (int i = 0; i < 3; i++) {
      Boolean res = (Boolean) iterator.next();
      assertEquals(Boolean.TRUE, res);
    }
  }

  public static void verifyDeadAndLiveServers(final Integer expectedLiveServers) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      @Override
      public boolean done() {
        int sz = pool.getConnectedServerCount();
        logger.info("Checking for the Live Servers : Expected  : "
            + expectedLiveServers + " Available :" + sz);
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
      List l = ((List) rc1.getResult());
      logger.info("Result size : " + l.size());
      assertEquals(3, l.size());

      for (Object o : l) {
        assertEquals(Boolean.TRUE, o);
      }
    } catch (CacheClosedException e) {
      // okay - ignore
    }
  }

  static Object executeFunctionHA() {
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

  static void putOperation() {
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

  protected void createScenario() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
  }

  private void createScenarioForBucketFilter() {
    ArrayList commonAttributes = createCommonServerAttributes("TestPartitionedRegion",
        new BucketFilterPRResolver(), 0, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
  }

  private void createScenario_SingleConnection() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarion_SingleConnection(commonAttributes, 0, 20);
  }



  private void createScenarioWith2Regions() {
    ArrayList commonAttributes =
        createCommonServerAttributes(PartitionedRegionName, null, 0, null);
    createClientServerScenarionWith2Regions(commonAttributes, 20, 20, 20);

  }

  private static void checkBucketsOnServer() {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(PartitionedRegionName);
    HashMap localBucket2RegionMap = (HashMap) region.getDataStore().getSizeLocally();
    logger.info(
        "Size of the " + PartitionedRegionName + " in this VM :- " + localBucket2RegionMap.size());
    Set entrySet = localBucket2RegionMap.entrySet();
    assertNotNull(entrySet);
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



  private static void serverMultiKeyExecution_SendException(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    int j = 0;
    for (String value : testKeysSet) {
      Integer val = j++;
      region.put(value, val);
    }
    try {
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      List l = ((List) rc1.getResult());
      logger.info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Object o : l) {
        assertTrue(o instanceof MyFunctionExecutionException);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("No Exception Expected");
    }

    try {
      ResultCollector rc1 = execute(dataSet, testKeysSet, testKeysSet, function, isByName);
      List resultList = (List) rc1.getResult();
      assertEquals(((testKeysSet.size() * 3) + 3), resultList.size());
      Iterator resultIterator = resultList.iterator();
      int exceptionCount = 0;
      while (resultIterator.hasNext()) {
        Object o = resultIterator.next();
        if (o instanceof MyFunctionExecutionException) {
          exceptionCount++;
        }
      }
      assertEquals(3, exceptionCount);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("No Exception Expected");
    }
  }

  private static void serverMultiKeyExecution_ThrowException(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    int j = 0;
    for (String o : testKeysSet) {
      Integer val = j++;
      region.put(o, val);
    }
    try {
      execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      fail("Exception Expected");
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  static void serverMultiKeyExecutionSocketTimeOut(Boolean isByName) {
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
      Assert.fail("Test failed after the put operation", e);

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

    ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

    ResultCollector rs2 = execute(dataSet, testKeysSet, testKey, function, isByName);
    assertEquals(testKey, ((List) rs2.getResult()).get(0));

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

  private static void serverMultiKeyExecutionNoResult(Boolean isByName) throws Exception {
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
      assertTrue(expected.getMessage()
          .startsWith((String.format("Cannot %s result as the Function#hasResult() is false",
              "return any"))));
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "FunctionException" + "</ExpectedException>");
    }
  }

  private static void serverSingleKeyExecutionOnRegion_SingleConnection() {
    Region<Integer, String> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    for (int i = 0; i < 13; i++) {
      region.put(i, "KB_" + i);
    }
    Function function = new TestFunction(false, TEST_FUNCTION2);
    Execution dataSet = FunctionService.onRegion(region);
    dataSet.setArguments(Boolean.TRUE).execute(function);
    region.put(2, "KB_2");
    assertEquals("KB_2", region.get(2));
  }

  private static void serverSingleKeyExecution(Boolean isByName, Boolean toRegister)
      throws Exception {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TEST_FUNCTION2);

    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution dataSet = FunctionService.onRegion(region);
    try {
      execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    } catch (Exception ex) {
      if (!(ex.getMessage().contains("No target node found for KEY = " + testKey)
          || ex.getMessage().startsWith("Server could not send the reply")
          || ex.getMessage().startsWith("Unexpected exception during"))) {
        throw ex;
      }
    }
    region.put(testKey, 1);

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
  }

  private static void executeRegisteredFunction() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, 1);
    ((AbstractExecution) dataSet).removeFunctionAttributes(TestFunction.TEST_FUNCTION2);
    ResultCollector rs = dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE)
        .execute(TestFunction.TEST_FUNCTION2);
    assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));
    byte[] functionAttributes =
        ((AbstractExecution) dataSet).getFunctionAttributes(TestFunction.TEST_FUNCTION2);
    assertNotNull(functionAttributes);

    rs = dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE)
        .execute(TestFunction.TEST_FUNCTION2);
    assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));
    assertNotNull(functionAttributes);
  }

  private static void serverSingleKeyExecution_SendException(Boolean isByName, Boolean toRegister) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SEND_EXCEPTION);

    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, 1);

    ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    assertTrue(((List) rs.getResult()).get(0) instanceof MyFunctionExecutionException);

    rs = execute(dataSet, testKeysSet, (Serializable) testKeysSet, function, isByName);
    List resultList = (List) rs.getResult();
    assertEquals((testKeysSet.size() + 1), resultList.size());
    Iterator resultIterator = resultList.iterator();
    int exceptionCount = 0;
    while (resultIterator.hasNext()) {
      Object o = resultIterator.next();
      if (o instanceof MyFunctionExecutionException) {
        exceptionCount++;
      }
    }
    assertEquals(1, exceptionCount);
  }

  private static void serverSingleKeyExecution_ThrowException(Boolean isByName,
      Boolean toRegister) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_THROW_EXCEPTION);

    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, 1);
    try {
      execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      fail("Exception Expected");
    } catch (Exception ignored) {
    }
  }

  private static void serverSingleKeyExecutionWith2Regions(Boolean toRegister) {
    Region<String, Integer> region1 = cache.getRegion(PartitionedRegionName + "1");
    assertNotNull(region1);
    final String testKey = "execKey";
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TEST_FUNCTION2);
    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet1 = FunctionService.onRegion(region1);
    region1.put(testKey, 1);

    ResultCollector rs = dataSet1.execute(function.getId());
    assertEquals(Boolean.FALSE, ((List) rs.getResult()).get(0));

    Region<String, Integer> region2 = cache.getRegion(PartitionedRegionName + "2");
    assertNotNull(region2);

    Execution dataSet2 = FunctionService.onRegion(region2);
    region2.put(testKey, 1);
    try {
      rs = dataSet2.execute(function.getId());
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));
      fail("Expected FunctionException");
    } catch (Exception ex) {
      if (!ex.getMessage().startsWith("No Replicated Region found for executing function")) {
        throw ex;
      }
    }
  }

  private static void serverSingleKeyExecution_NoLastResult(Boolean isByName, Boolean toRegister) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);

    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, 1);
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));
      fail("Expected FunctionException : Function did not send last result");
    } catch (Exception ex) {
      if (!ex.getMessage().contains("did not send last result")) {
        throw ex;
      }

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

    ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, false);
    ArrayList list = (ArrayList) rs.getResult();
    assertTrue(((Integer) list.get(0)) >= 5);
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
    } catch (Exception ex) {
      if (!(ex.getMessage().contains("No target node found for KEY = " + testKey)
          || ex.getMessage().startsWith("Server could not send the reply")
          || ex.getMessage().startsWith("Unexpected exception during"))) {
        throw ex;
      }
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "No target node found for KEY = "
              + "|Server could not send the reply" + "|Unexpected exception during"
              + "</ExpectedException>");
    }

    region.put(testKey, 1);

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

  }



  /**
   * This class can be serialized but its deserialization will always fail
   *
   */
  private static class UnDeserializable implements DataSerializable {
    @Override
    public void toData(DataOutput out) throws IOException {}

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      throw new RuntimeException("deserialization is not allowed on this class");
    }

  }

  private static void serverBug43430() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, 1);
    try {
      cache.getLogger()
          .info("<ExpectedException action=add>"
              + "Could not create an instance of org.apache.geode.internal.cache.execute.PRClientServerRegionFunctionExecutionDUnitTest$UnDeserializable"
              + "</ExpectedException>");
      dataSet.withFilter(testKeysSet).setArguments(new UnDeserializable())
          .execute(new FunctionAdapter() {
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
      if (!expected.getCause().getMessage().contains(
          "Could not create an instance of org.apache.geode.internal.cache.execute.PRClientServerRegionFunctionExecutionDUnitTest$UnDeserializable")) {
        throw expected;
      }
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>"
              + "Could not create an instance of  org.apache.geode.internal.cache.execute.PRClientServerRegionFunctionExecutionDUnitTest$UnDeserializable"
              + "</ExpectedException>");
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

  /**
   * Attempt to do a client server function execution with an arg that fail deserialization on the
   * server. The client should see an exception instead of a hang if bug 43430 is fixed.
   */
  @Test
  public void testBug43430() {
    createScenario();
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    SerializableRunnable suspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=add>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest::serverBug43430);
    SerializableRunnable endSuspect = new SerializableRunnable() {
      @Override
      public void run() {
        cache.getLogger()
            .info("<ExpectedException action=remove>" + "No target node found for KEY = "
                + "|Server could not send the reply" + "|Unexpected exception during"
                + "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
  }

}
