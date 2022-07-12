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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerConnectivityException;
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
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ClientServerTest.class, FunctionServiceTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
    extends PRClientServerTestBase {

  private static final Logger logger = LogService.getLogger();

  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;

  private static final String TEST_FUNCTION2 = TestFunction.TEST_FUNCTION2;

  private Boolean isByName = null;

  private static final int retryCount = 0;

  public PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest() {
    super();
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerAllKeyExecution_byInstance() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverAllKeyExecution(isByName));
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerGetAllFunction() {
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::getAll);
  }

  /*
   * Execution of the function on server with
   */
  @Test
  public void testServerPutAllFunction() {
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::putAll);
  }

  /*
   * Execution of the function on server with single key as the routing object and using the name of
   * the function
   */
  @Test
  public void testServerSingleKeyExecution_byName() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
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
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::serverSingleKeyExecution_FunctionInvocationTargetException);
  }

  /*
   * Execution of the function on server with bucket as filter
   */
  @Test
  public void testBucketFilter() {
    createScenarioForBucketFilter();
    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_BUCKET_FILTER);
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
    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_BUCKET_FILTER);
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
    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverSingleKeyExecutionSocketTimeOut(isByName));
  }

  /*
   * Execution of the function on server with single key as the routing object and using the
   * instance of the function
   */
  @Test
  public void testServerSingleKeyExecution_byInstance() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverSingleKeyExecution(isByName));
  }

  /*
   * Execution of the inline function on server with single key as the routing object
   */
  @Test
  public void testServerSingleKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::serverSingleKeyExecution_Inline);
  }

  /*
   * Execution of the function on server with set multiple keys as the routing object and using the
   * name of the function
   */
  @Test
  public void testserverMultiKeyExecution_byName() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverMultiKeyExecution(isByName));
    server1.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::checkBucketsOnServer);
    server2.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::checkBucketsOnServer);
    server3.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::checkBucketsOnServer);
  }

  @Test
  public void testserverMultiKeyExecution_SocketTimeOut() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverMultiKeyExecutionSocketTimeOut(isByName));
  }

  /*
   * Execution of the inline function on server with set multiple keys as the routing object
   */
  @Test
  public void testserverMultiKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::serverMultiKeyExecution_Inline);
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
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::serverMultiKeyExecution_FunctionInvocationTargetException);
  }

  /*
   * Execution of the function on server with set multiple keys as the routing object and using the
   * name of the function
   */
  @Test
  public void testserverMultiKeyExecutionNoResult_byName() {
    createScenario();
    Function<Object> function = new TestFunction<>(false, TEST_FUNCTION7);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverMultiKeyExecutionNoResult(isByName));
  }

  /*
   * Execution of the function on server with set multiple keys as the routing object and using the
   * instance of the function
   */
  @Test
  public void testserverMultiKeyExecution_byInstance() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverMultiKeyExecution(isByName));
  }

  /*
   * Ensure that the execution is limited to a single bucket put another way, that the routing logic
   * works correctly such that there is not extra execution
   */
  @Test
  public void testserverMultiKeyExecutionOnASingleBucket_byName() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverMultiKeyExecutionOnASingleBucket(isByName));
  }

  /*
   * Ensure that the execution is limited to a single bucket put another way, that the routing logic
   * works correctly such that there is not extra execution
   */
  @Test
  public void testserverMultiKeyExecutionOnASingleBucket_byInstance() {
    createScenario();
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = Boolean.FALSE;
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .serverMultiKeyExecutionOnASingleBucket(isByName));
  }

  /*
   * Ensure that the while executing the function if the servers is down then the execution is
   * failover to other available server
   */
  @Test
  public void testServerFailoverWithTwoServerAliveHA() throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    ArrayList<Object> commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 1, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::stopServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::stopServerHA);
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::putOperation);

    AsyncInvocation<List<Boolean>> async = client.invokeAsync(
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::executeFunctionHA);
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::startServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::startServerHA);
    server1.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::stopServerHA);
    client.invoke(() -> PRClientServerRegionFunctionExecutionDUnitTest
        .verifyDeadAndLiveServers(2));

    List<?> l = async.get();

    assertThat(l).hasSize(2);
  }

  /*
   * Ensure that the while executing the function if the servers is down then the execution is
   * failover to other available server
   */
  @Test
  public void testServerCacheClosedFailoverWithTwoServerAliveHA() throws InterruptedException {
    IgnoredException.addIgnoredException("FunctionInvocationTargetException");
    ArrayList<Object> commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 1, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_HA);
    registerFunctionAtServer(function);
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::stopServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::stopServerHA);
    client.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::putOperation);

    AsyncInvocation<List<Boolean>> async = client.invokeAsync(
        PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::executeFunctionHA);
    server2.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::startServerHA);
    server3.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::startServerHA);
    server1.invoke(PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::closeCacheHA);
    client.invoke(() -> PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest
        .verifyDeadAndLiveServers(2));

    List<?> l = async.get();
    assertThat(l).hasSize(2);
  }

  @Test
  public void testBug40714() {
    createScenario();
    server1
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::registerFunction);
    server1
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::registerFunction);
    server1
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::registerFunction);
    client
        .invoke(
            (SerializableRunnableIF) PRClientServerRegionFunctionExecutionNoSingleHopDistributedTest::registerFunction);
    client.invoke(
        PRClientServerRegionFunctionExecutionDUnitTest::FunctionExecution_Inline_Bug40714);
  }

  /**
   * This test case verifies that if the execution of a function handled
   * by a Function Execution thread times out at the client, the ServerConnection
   * thread will eventually be released.
   * In order to test this, a slow function will be executed by a client
   * with a small time-out a number of times equal to the number of servers
   * in the cluster * the max number of threads configured.
   * After the function executions have timed-out, another request will be
   * sent by the client to any server and it should be served timely.
   * If the ServerConnection threads had not been released, this new
   * request will never be served because there would be not ServerConnection
   * threads available and the test case will time-out.
   */
  @Test
  public void testClientFunctionExecutionTimingOutDoesNotLeaveServerConnectionThreadsHanged() {
    // Set client connect-timeout to a very high value so that if there are no
    // ServerConnection threads available the test will time-out before the client times-out.
    int connectTimeout = (int) (GeodeAwaitility.getTimeout().toMillis() * 2);
    int maxThreads = 2;
    createScenarioWithClientConnectTimeout(connectTimeout, maxThreads);

    // The function must be executed a number of times equal
    // to the number of servers * the max-threads, to check if all the
    // threads are hanged.
    int executions = (3 * maxThreads);

    // functionTimeoutSecs should be lower than the
    // time taken by the slow function to return all
    // the results
    int functionTimeoutSecs = 2;

    Function<?> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_SLOW);
    registerFunctionAtServer(function);

    // Run the function that will time-out at the client
    // the number of specified times.
    IntStream.range(0, executions)
        .forEach(i -> assertThatThrownBy(() -> client
            .invoke(() -> executeSlowFunctionOnRegionNoFilter(function, PartitionedRegionName,
                functionTimeoutSecs)))
                    .getCause().getCause().isInstanceOf(ServerConnectivityException.class));

    // Make sure that the get returns timely. If it hangs, it means
    // that there are no threads available in the servers to handle the
    // request because they were hanged due to the previous function
    // executions.
    await().until(() -> {
      client.invoke(() -> executeGet(PartitionedRegionName, "key"));
      return true;
    });
  }

  private Object executeGet(String regionName, Object key) {
    Region<?, ?> region = cache.getRegion(regionName);
    return region.get(key);
  }

  private Object executeSlowFunctionOnRegionNoFilter(Function<?> function, String regionName,
      int functionTimeoutSecs) {
    FunctionService.registerFunction(function);
    Region<?, ?> region = cache.getRegion(regionName);

    Execution execution = FunctionService.onRegion(region);

    Object[] args = {Boolean.TRUE};
    return execution.setArguments(args).execute(function.getId(), functionTimeoutSecs,
        TimeUnit.SECONDS).getResult();
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

    Region<Object, Object> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      ResultCollector<?, ?> rc1 =
          dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(function.getId());

      HashMap<?, ?> resultMap = ((HashMap<?, ?>) rc1.getResult());
      assertThat(resultMap).hasSize(3);

      for (Map.Entry<?, ?> o : resultMap.entrySet()) {
        ArrayList<?> resultListForMember = (ArrayList<?>) o.getValue();

        for (Object result : resultListForMember) {
          assertThat(result).isEqualTo(true);
        }
      }
    } catch (Exception e) {
      logger.info("Got an exception : " + e.getMessage());
      assertThat(e).isInstanceOf(CacheClosedException.class);
    }
  }

  private static void putOperation() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
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
    ArrayList<Object> commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarioNoSingleHop(commonAttributes, 20, 20, 20);
  }

  private void createScenarioWithClientConnectTimeout(int connectTimeout, int maxThreads) {
    ArrayList<Object> commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 0, null);
    createClientServerScenarioNoSingleHop(commonAttributes, 20, 20, 20, maxThreads, connectTimeout);
  }


  private void createScenarioForBucketFilter() {
    ArrayList<Object> commonAttributes = createCommonServerAttributes("TestPartitionedRegion",
        new BucketFilterPRResolver(), 0, null);
    createClientServerScenarioNoSingleHop(commonAttributes, 20, 20, 20);
  }

  private static void checkBucketsOnServer() {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(PartitionedRegionName);
    HashMap<Integer, Integer> localBucket2RegionMap =
        (HashMap<Integer, Integer>) region.getDataStore().getSizeLocally();
    logger.info(
        "Size of the " + PartitionedRegionName + " in this VM :- " + localBucket2RegionMap.size());
    Set<Map.Entry<Integer, Integer>> entrySet = localBucket2RegionMap.entrySet();
    assertThat(entrySet).isNotNull();
  }

  private static void serverAllKeyExecution(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets / 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    int j = 0;
    HashSet<Integer> origVals = new HashSet<>();
    for (String item : testKeysSet) {
      Integer val = j++;
      origVals.add(val);
      region.put(item, val);
    }
    ResultCollector<?, ?> rc1 = executeOnAll(dataSet, Boolean.TRUE, function, isByName);
    List<?> resultList = (List<?>) rc1.getResult();
    assertThat(resultList).hasSize(3);

    for (Object result : resultList) {
      assertThat(result).isEqualTo(true);
    }
    ResultCollector<?, ?> rc2 = executeOnAll(dataSet, testKeysSet, function, isByName);
    List<?> l2 = (List<?>) rc2.getResult();
    assertThat(l2).hasSize(3);
    HashSet<Integer> foundVals = new HashSet<>();
    for (Object value : l2) {
      List<?> subL = (List<?>) value;
      assertThat(subL).hasSizeGreaterThan(0);
      for (Object o : subL) {
        assertThat(foundVals.add((Integer) o)).isTrue();
      }
    }
    assertThat(foundVals).containsExactlyInAnyOrderElementsOf(origVals);
  }

  public static void getAll() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final List<String> testKeysList = new ArrayList<>();
    for (int i = (totalNumBuckets * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    int j = 0;
    Map<String, Integer> origVals = new HashMap<>();
    for (String key : testKeysList) {
      Integer val = j++;
      origVals.put(key, val);
      region.put(key, val);
    }
    Map<String, Integer> resultMap = region.getAll(testKeysList);
    assertThat(resultMap).containsExactlyInAnyOrderEntriesOf(origVals);
    await().untilAsserted(
        () -> assertThat(region.getAll(testKeysList)).containsExactlyInAnyOrderEntriesOf(origVals));
  }

  public static void putAll() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final List<String> testKeysList = new ArrayList<>();
    for (int i = (totalNumBuckets * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    int j = 0;
    Map<String, Integer> origVals = new HashMap<>();
    for (String key : testKeysList) {
      Integer val = j++;
      origVals.put(key, val);
      region.put(key, val);
    }
    Map<String, Integer> resultMap = region.getAll(testKeysList);
    assertThat(resultMap).containsExactlyInAnyOrderEntriesOf(origVals);
    await().untilAsserted(
        () -> assertThat(region.getAll(testKeysList)).containsExactlyInAnyOrderEntriesOf(origVals));
  }

  private static void serverMultiKeyExecutionOnASingleBucket(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
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
    for (String key : testKeysSet) {
      Set<String> singleKeySet = Collections.singleton(key);
      Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution dataSet = FunctionService.onRegion(region);
      ResultCollector<?, ?> rc1 = execute(dataSet, singleKeySet, Boolean.TRUE, function, isByName);
      List<?> list1 = (List<?>) rc1.getResult();
      assertThat(list1).hasSize(1);

      ResultCollector<?, ?> rc2 =
          execute(dataSet, singleKeySet, new HashSet<>(singleKeySet), function, isByName);
      List<?> list2 = (List<?>) rc2.getResult();

      assertThat(list2).hasSize(1);
      List<Integer> subList = (List<Integer>) list2.iterator().next();
      assertThat(subList).hasSize(1);
      assertThat(subList).containsOnly(region.get(singleKeySet.iterator().next()));
    }
  }

  private static void serverMultiKeyExecution(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    int j = 0;
    HashSet<Integer> origVals = new HashSet<>();
    for (String element : testKeysSet) {
      Integer val = j++;
      origVals.add(val);
      region.put(element, val);
    }
    ResultCollector<?, ?> rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    List<?> l = (List<?>) rc1.getResult();
    assertThat(l).hasSize(3);
    for (Object item : l) {
      assertThat(item).isEqualTo(true);
    }

    ResultCollector<?, ?> rc2 = execute(dataSet, testKeysSet, testKeysSet, function, isByName);
    List<?> l2 = (List<?>) rc2.getResult();
    assertThat(l2).hasSize(3);
    HashSet<Integer> foundVals = new HashSet<>();
    for (Object value : l2) {
      List<?> subL = (List<?>) value;
      assertThat(subL).hasSizeGreaterThan(0);
      for (Object o : subL) {
        assertThat(foundVals.add((Integer) o)).isTrue();
      }
    }
    assertThat(foundVals).containsExactlyInAnyOrderElementsOf(origVals);
  }


  private static void serverMultiKeyExecutionSocketTimeOut(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    int j = 0;
    for (String value : testKeysSet) {
      Integer val = j++;
      region.put(value, val);
    }
    ResultCollector<?, ?> rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    List<?> l = (List<?>) rc1.getResult();
    logger.info("Result size : " + l.size());
    assertThat(l).hasSize(3);
    for (Object o : l) {
      assertThat(o).isEqualTo(true);
    }
  }

  private static void serverSingleKeyExecutionSocketTimeOut(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function<Object> function = new TestFunction<>(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, 1);

    ResultCollector<?, ?> rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    assertThat(((List<?>) rs.getResult()).get(0)).isEqualTo(true);

    ResultCollector<?, ?> rs2 = execute(dataSet, testKeysSet, testKey, function, isByName);
    assertThat(((List<?>) rs2.getResult()).get(0)).isEqualTo(testKey);
  }

  private static void serverMultiKeyExecution_Inline() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution dataSet = FunctionService.onRegion(region);

    int j = 0;
    for (String value : testKeysSet) {
      Integer val = j++;
      region.put(value, val);
    }
    ResultCollector<?, ?> rc1 =
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
    List<?> list = (List<?>) rc1.getResult();
    logger.info("Result size : " + list.size());
    assertThat(list).hasSize(3);
    for (Object item : list) {
      assertThat(item).isEqualTo(true);
    }
  }

  private static void serverMultiKeyExecution_FunctionInvocationTargetException() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
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
    ResultCollector<?, ?> rc1 =
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

    List<?> list = (List<?>) rc1.getResult();
    assertThat(list.get(0)).isEqualTo(0);
  }

  private static void serverMultiKeyExecutionNoResult(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final HashSet<String> testKeysSet = new HashSet<>();
    for (int i = (totalNumBuckets * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function<Object> function = new TestFunction<>(false, TEST_FUNCTION7);
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
      ResultCollector<?, ?> rc1 = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      assertThatThrownBy(rc1::getResult).isExactlyInstanceOf(FunctionException.class)
          .hasMessageStartingWith(
              String.format("Cannot %s result as the Function#hasResult() is false",
                  "return any"));
    } finally {
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "FunctionException" + "</ExpectedException>");
    }
  }

  private static void serverSingleKeyExecution(Boolean isByName) {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);

    region.put(testKey, 1);

    ResultCollector<?, ?> rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
    assertThat(((List<?>) rs.getResult()).get(0)).isEqualTo(true);

    ResultCollector<?, ?> rs2 = execute(dataSet, testKeysSet, testKey, function, isByName);
    assertThat(((List<?>) rs2.getResult()).get(0)).isEqualTo(1);

    HashMap<String, Integer> putData = new HashMap<>();
    putData.put(testKey + "1", 2);
    putData.put(testKey + "2", 3);

    ResultCollector<?, ?> rs1 = execute(dataSet, testKeysSet, putData, function, isByName);
    assertThat(((List<?>) rs1.getResult()).get(0)).isEqualTo(true);

    assertThat(region.get(testKey + "1")).isEqualTo(2);
    assertThat(region.get(testKey + "2")).isEqualTo(3);
  }

  private static void serverSingleKeyExecution_FunctionInvocationTargetException() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function<Object> function =
        new TestFunction<>(true, TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, 1);

    ResultCollector<?, ?> rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, false);
    ArrayList<?> list = (ArrayList<?>) rs.getResult();
    assertThat(((Integer) list.get(0))).isGreaterThanOrEqualTo(5);
  }

  private static void serverSingleKeyExecution_Inline() {
    Region<String, Integer> region = cache.getRegion(PartitionedRegionName);
    assertThat(region).isNotNull();
    final String testKey = "execKey";
    final Set<String> testKeysSet = new HashSet<>();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Execution dataSet = FunctionService.onRegion(region);

    FunctionAdapter functionAdapter = new FunctionAdapter() {
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
    };

    dataSet.withFilter(testKeysSet).setArguments(Boolean.TRUE).execute(functionAdapter);

    region.put(testKey, 1);

    ResultCollector<?, ?> rs =
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
    assertThat(((List<?>) rs.getResult()).get(0)).isEqualTo("Failure");

    ResultCollector<?, ?> rs2 =
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

    assertThat(((List<?>) rs2.getResult()).get(0)).isEqualTo("Success");

  }

  private static ResultCollector<?, ?> execute(Execution dataSet, Set<?> testKeysSet,
      Serializable args,
      Function<?> function, Boolean isByName) {
    if (isByName) {// by name
      return dataSet.withFilter(testKeysSet).setArguments(args).execute(function.getId());
    } else { // By Instance
      return dataSet.withFilter(testKeysSet).setArguments(args).execute(function);
    }
  }

  private static ResultCollector<?, ?> executeOnAll(Execution dataSet, Serializable args,
      Function<?> function, Boolean isByName) {
    if (isByName) {// by name
      return dataSet.setArguments(args).execute(function.getId());
    } else { // By Instance
      return dataSet.setArguments(args).execute(function);
    }
  }
}
