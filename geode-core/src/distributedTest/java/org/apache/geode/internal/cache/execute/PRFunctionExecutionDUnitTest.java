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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION1;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION2;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION3;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION7;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_BUCKET_FILTER;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_HA;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_LASTRESULT;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({FunctionServiceTest.class})
public class PRFunctionExecutionDUnitTest extends CacheTestCase {

  private static final String STRING_KEY = "execKey";

  private String regionName;

  private String regionNameTop;
  private String regionNameColocated1;
  private String regionNameColocated2;

  @Before
  public void setUp() {
    regionName = getUniqueName();

    regionNameTop = regionName + "_top";
    regionNameColocated1 = regionName + "_colo1";
    regionNameColocated2 = regionName + "_colo2";
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> DistributedSystem.setThreadsSocketPolicy(false));
    DistributedSystem.setThreadsSocketPolicy(false);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.test.junit.rules.**;org.apache.geode.internal.cache.execute.**;org.apache.geode.internal.cache.functions.**;org.apache.geode.test.dunit.**");
    return config;
  }

  /**
   * Test to validate that the function execution is successful on PR with Loner Distributed System
   */
  @Test
  public void testFunctionExecution() throws Exception {
    Properties config = getDistributedSystemProperties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    getCache(config);

    Region<String, Integer> region = createPartitionedRegion(regionName, 10, 0);

    Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION1);
    FunctionService.registerFunction(function);

    Execution<Boolean, Boolean, Collection<Boolean>> execution = FunctionService.onRegion(region);

    ResultCollector<Boolean, Collection<Boolean>> resultCollector =
        execution.setArguments(true).withFilter(createKeySet(STRING_KEY)).execute(function);

    assertThat(resultCollector.getResult()).containsExactly(true);
  }

  @Test
  public void testHAFunctionExecution() throws Exception {
    Region<String, Integer> region = createPartitionedRegion(regionName, 10, 0);

    Function<Void> function = new TestFunction<>(false, TestFunction.TEST_FUNCTION10);

    assertThatThrownBy(() -> FunctionService.registerFunction(function))
        .isInstanceOf(FunctionException.class)
        .hasMessageContaining("For Functions with isHA true, hasResult must also be true.");

    Execution<String, Void, Void> dataSet = FunctionService.onRegion(region);
    assertThatThrownBy(() -> dataSet.withFilter(createKeySet(STRING_KEY)).setArguments(STRING_KEY)
        .execute(function)).isInstanceOf(FunctionException.class)
            .hasMessageContaining("For Functions with isHA true, hasResult must also be true.");
  }

  /**
   * Test remote execution by a pure accessor which doesn't have the function factory present.
   */
  @Test
  public void testRemoteSingleKeyExecution_byName() throws Exception {
    VM accessor = getHost(0).getVM(2);
    VM datastore = getHost(0).getVM(3);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    accessor.invoke(() -> {
      Region<String, Integer> region = getPartitionedRegion(regionName);
      region.put(STRING_KEY, 1);

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution<Object, Object, List<Object>> dataSet = FunctionService.onRegion(region);
      dataSet.withFilter(createKeySet(STRING_KEY)).setArguments(true).execute(function.getId());

      ResultCollector<Object, List<Object>> resultCollector =
          dataSet.withFilter(createKeySet(STRING_KEY)).setArguments(true).execute(function.getId());

      assertThat(resultCollector.getResult()).hasSize(1).containsExactly(true);

      resultCollector = dataSet.withFilter(createKeySet(STRING_KEY)).setArguments(STRING_KEY)
          .execute(function.getId());

      assertThat(resultCollector.getResult()).hasSize(1).containsExactly(1);

      Map<String, Integer> putData = createEntryMap(2, STRING_KEY, 1, 2);
      resultCollector = dataSet.withFilter(createKeySet(STRING_KEY)).setArguments(putData)
          .execute(function.getId());

      assertThat(resultCollector.getResult()).hasSize(1).containsExactly(true);
      assertThat(region.get(STRING_KEY + "1")).isEqualTo(2);
      assertThat(region.get(STRING_KEY + "2")).isEqualTo(3);
    });
  }

  /**
   * Test local execution by a datastore Function throws the FunctionInvocationTargetException. As
   * this is the case of HA then system should retry the function execution. After 5th attempt
   * function will send Boolean as last result. factory present.
   */
  @Test
  public void testLocalSingleKeyExecution_byName_FunctionInvocationTargetException()
      throws Exception {
    Region<String, Integer> region = createPartitionedRegion(regionName, 10, 0);
    region.put(STRING_KEY, 1);

    Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION_REEXECUTE_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution<Boolean, Integer, List<Integer>> dataSet = FunctionService.onRegion(region);

    ResultCollector<Integer, List<Integer>> resultCollector =
        dataSet.withFilter(createKeySet(STRING_KEY)).setArguments(true).execute(function.getId());

    List<Integer> list = resultCollector.getResult();
    assertThat(list.get(0)).isEqualTo(5);
  }

  /**
   * Test remote execution by a pure accessor which doesn't have the function factory
   * present.Function throws the FunctionInvocationTargetException. As this is the case of HA then
   * system should retry the function execution. After 5th attempt function will send Boolean as
   * last result.
   */
  @Test
  public void testRemoteSingleKeyExecution_byName_FunctionInvocationTargetException()
      throws Exception {
    VM accessor = getHost(0).getVM(2);
    VM datastore = getHost(0).getVM(3);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      Function function = new TestFunction(true, TEST_FUNCTION_REEXECUTE_EXCEPTION);
      FunctionService.registerFunction(function);
    });

    accessor.invoke(() -> {
      Region<String, Integer> region = getPartitionedRegion(regionName);
      region.put(STRING_KEY, 1);

      Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION_REEXECUTE_EXCEPTION);
      FunctionService.registerFunction(function);
      Execution<Boolean, Integer, List<Integer>> dataSet = FunctionService.onRegion(region);

      ResultCollector<Integer, List<Integer>> resultCollector =
          dataSet.withFilter(createKeySet(STRING_KEY)).setArguments(true).execute(function.getId());

      List<Integer> list = resultCollector.getResult();
      assertThat(list.get(0)).isEqualTo(5);
    });
  }

  /**
   * Test remote execution by a pure accessor which doesn't have the function factory present.
   */
  @Test
  public void testRemoteSingleKeyExecution_byInstance() throws Exception {
    VM accessor = getHost(0).getVM(2);
    VM datastore = getHost(0).getVM(3);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);

      Function function = new TestFunction(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
    });

    accessor.invoke(() -> {
      Region<String, Integer> region = getPartitionedRegion(regionName);
      region.put(STRING_KEY, 1);

      Function function = new TestFunction(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);

      Execution<Boolean, Boolean, List<Boolean>> booleanDataSet = FunctionService.onRegion(region);
      ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
          booleanDataSet.withFilter(createKeySet(STRING_KEY)).setArguments(true).execute(function);
      assertThat(booleanResultCollector.getResult().get(0)).isTrue();

      Execution<String, Integer, List<Integer>> integerDataSet = FunctionService.onRegion(region);
      ResultCollector<Integer, List<Integer>> integerResultCollector = integerDataSet
          .withFilter(createKeySet(STRING_KEY)).setArguments(STRING_KEY).execute(function);
      assertThat(integerResultCollector.getResult().get(0)).isEqualTo(1);

      Map<String, Integer> putData = createEntryMap(2, STRING_KEY, 1, 2);
      Execution<Map<String, Integer>, Boolean, List<Boolean>> mapDataSet =
          FunctionService.onRegion(region);
      ResultCollector<Boolean, List<Boolean>> mapResultCollector =
          mapDataSet.withFilter(createKeySet(STRING_KEY)).setArguments(putData).execute(function);
      assertThat(mapResultCollector.getResult().get(0)).isTrue();

      assertThat(region.get(STRING_KEY + "1")).isEqualTo(2);
      assertThat(region.get(STRING_KEY + "2")).isEqualTo(3);
    });
  }

  /**
   * Test remote execution of inline function by a pure accessor
   */
  @Test
  public void testRemoteSingleKeyExecution_byInlineFunction() throws Exception {
    VM accessor = getHost(0).getVM(2);
    VM datastore = getHost(0).getVM(3);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
    });

    accessor.invoke(() -> {
      Region<String, Integer> region = getPartitionedRegion(regionName);
      region.put(STRING_KEY, 1);

      Execution<Boolean, Boolean, List<Boolean>> dataSet = FunctionService.onRegion(region);
      ResultCollector<Boolean, List<Boolean>> rs1 = dataSet.withFilter(createKeySet(STRING_KEY))
          .setArguments(true).execute(new BooleanFunction(true));

      assertThat(rs1.getResult().get(0)).isTrue();
    });
  }

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the function factory
   * present. ResultCollector = DefaultResultCollector haveResults = true;
   */
  @Test
  public void testRemoteMultiKeyExecution_byName() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      Collection<Integer> expectedValues = new HashSet<>();
      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        expectedValues.add(value);
        pr.put(key, value);
      }

      Function function = new TestFunction(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);

      Execution<Boolean, Boolean, List<Boolean>> booleanExecution = FunctionService.onRegion(pr);
      ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
          booleanExecution.withFilter(keySet).setArguments(true).execute(function.getId());

      List<Boolean> booleanResultList = booleanResultCollector.getResult();
      assertThat(booleanResultList).hasSize(3).containsExactly(true, true, true);

      Execution<Set<String>, List<Integer>, List<List<Integer>>> integerExecution =
          FunctionService.onRegion(pr);
      ResultCollector<List<Integer>, List<List<Integer>>> valuesResultCollector =
          integerExecution.withFilter(keySet).setArguments(keySet).execute(function.getId());

      List<List<Integer>> valuesResults = valuesResultCollector.getResult();
      assertThat(valuesResults).hasSize(3).isInstanceOf(List.class);
      assertThat(valuesResults.get(0)).isInstanceOf(List.class);

      Collection<Integer> actualValues = new HashSet<>();
      for (List<Integer> values : valuesResults) {
        assertThat(values.size()).isGreaterThan(0);
        for (int value : values) {
          assertThat(actualValues.add(value)).isTrue();
        }
      }
      assertThat(actualValues).isEqualTo(expectedValues);
    });
  }

  @Test
  public void testRemoteMultiKeyExecution_BucketMoved() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(0, 1, 0, 113));
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(40, 1, 0, 113));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_LASTRESULT));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(40, 1, 0, 113));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_LASTRESULT));
    });

    accessor.invoke(() -> {
      Region<Integer, String> region = getPartitionedRegion(regionName);
      for (int i = 0; i < 113; i++) {
        region.put(i, STRING_KEY + i);
      }
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(40, 1, 0, 113));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_LASTRESULT));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION_LASTRESULT);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> dataSet = FunctionService.onRegion(pr);

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          dataSet.setArguments(true).execute(function.getId());

      List<Boolean> resultList = resultCollector.getResult();
      assertThat(resultList).hasSize(2).containsExactly(true, true);
    });
  }

  @Test
  public void testLocalMultiKeyExecution_BucketMoved() throws Exception {
    IgnoredException.addIgnoredException("BucketMovedException");

    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(40, 0, 0, 113));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_LASTRESULT));
    });

    datastore0.invoke(() -> {
      Region<Integer, String> region = getPartitionedRegion(regionName);
      for (int i = 0; i < 113; i++) {
        region.put(i, STRING_KEY + i);
      }
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(40, 0, 0, 113));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_LASTRESULT));
    });

    datastore0.invoke(() -> {
      Region<Integer, String> region = getPartitionedRegion(regionName);

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION_LASTRESULT);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> dataSet = FunctionService.onRegion(region);

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          dataSet.setArguments(true).execute(function.getId());

      List<Boolean> resultList = resultCollector.getResult();
      assertThat(resultList).hasSize(2).containsExactly(true, true);
    });
  }

  /**
   * Test remote execution by a pure accessor which doesn't have the function factory
   * present.Function throws the FunctionInvocationTargetException. As this is the case of HA then
   * system should retry the function execution. After 5th attempt function will send Boolean as
   * last result.
   */
  @Test
  public void testRemoteMultipleKeyExecution_byName_FunctionInvocationTargetException()
      throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_REEXECUTE_EXCEPTION));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_REEXECUTE_EXCEPTION));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_REEXECUTE_EXCEPTION));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION_REEXECUTE_EXCEPTION);
      FunctionService.registerFunction(function);
      Execution<Boolean, Integer, List<Integer>> dataSet = FunctionService.onRegion(pr);

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      ResultCollector<Integer, List<Integer>> resultCollector =
          dataSet.withFilter(keySet).setArguments(true).execute(function.getId());

      List<Integer> resultList = resultCollector.getResult();
      assertThat(resultList).hasSize(3).containsExactly(5, 5, 5);
    });
  }

  @Test
  public void testRemoteMultiKeyExecutionHA_CacheClose() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 1);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 1);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_HA));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 1);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_HA));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 1);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_HA));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }
    });

    AsyncInvocation<List<Boolean>> async = accessor.invokeAsync(() -> executeFunction());

    datastore0.invoke(() -> {
      Thread.sleep(3_000);
      getCache().close();
    });

    List<Boolean> resultList = async.get();
    assertThat(resultList).hasSize(2).containsExactly(true, true);
  }

  @Test
  public void testRemoteMultiKeyExecutionHA_Disconnect() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 1);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 1);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_HA));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 1);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_HA));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 1);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_HA));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }
    });

    AsyncInvocation<List<Boolean>> async = accessor.invokeAsync(() -> executeFunction());

    datastore0.invoke(() -> {
      Thread.sleep(3000);
      getCache().getDistributedSystem().disconnect();
    });

    List<Boolean> resultList = async.get();
    assertThat(resultList).hasSize(2).containsExactly(true, true);
  }

  /**
   * Test multi-key remote execution of inline function by a pure accessor ResultCollector =
   * DefaultResultCollector haveResults = true;
   */
  @Test
  public void testRemoteMultiKeyExecution_byInlineFunction() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      Execution<Boolean, Boolean, List<Boolean>> execution = FunctionService.onRegion(pr);

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          execution.withFilter(keySet).setArguments(true).execute(new BooleanFunction(true));

      List<Boolean> results = resultCollector.getResult();
      assertThat(results).hasSize(3).containsExactly(true, true, true);
    });
  }

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the function factory
   * present. ResultCollector = CustomResultCollector haveResults = true;
   */
  @Test
  public void testRemoteMultiKeyExecutionWithCollector_byName() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> execution =
          FunctionService.onRegion(pr).withCollector(new CustomResultCollector());

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          execution.withFilter(keySet).setArguments(true).execute(function.getId());

      List<Boolean> results = resultCollector.getResult();
      assertThat(results).hasSize(3).containsExactly(true, true, true);
    });
  }

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the function factory
   * present. ResultCollector = DefaultResultCollector haveResults = false;
   */
  @Test
  public void testRemoteMultiKeyExecutionNoResult_byName() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(false, TEST_FUNCTION7));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(false, TEST_FUNCTION7));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(false, TEST_FUNCTION7));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      Function<Void> function = new TestFunction<>(false, TEST_FUNCTION7);
      FunctionService.registerFunction(function);
      Execution<Boolean, Void, Void> dataSet = FunctionService.onRegion(pr);

      ResultCollector<Void, Void> resultCollector =
          dataSet.withFilter(keySet).setArguments(true).execute(function.getId());
      assertThatThrownBy(() -> resultCollector.getResult()).isInstanceOf(FunctionException.class)
          .hasMessageStartingWith(
              String.format("Cannot %s result as the Function#hasResult() is false",
                  "return any"));
    });
  }

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the function factory
   * present. ResultCollector = DefaultResultCollector haveResults = true; result Timeout = 10
   * milliseconds expected result to be 0.(as the execution gets the timeout)
   */
  @Test
  public void testRemoteMultiKeyExecution_timeout() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      Function<Object> function = new TestFunction<>(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution<String, Object, List<Object>> dataSet = FunctionService.onRegion(pr);

      ResultCollector<Object, List<Object>> resultCollector =
          dataSet.withFilter(keySet).setArguments("TestingTimeOut").execute(function.getId());

      List<Object> resultList = resultCollector.getResult(10, TimeUnit.SECONDS);
      assertThat(resultList).hasSize(3).containsExactly(null, null, null);
    });
  }

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the function factory
   * present. ResultCollector = CustomResultCollector haveResults = false;
   */
  @Test
  public void testRemoteMultiKeyExecutionWithCollectorNoResult_byName() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(false, TEST_FUNCTION7));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(false, TEST_FUNCTION7));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(false, TEST_FUNCTION7));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      Function<Object> function = new TestFunction<>(false, TEST_FUNCTION7);
      FunctionService.registerFunction(function);
      Execution<Boolean, Object, List<Object>> dataSet =
          FunctionService.onRegion(pr).withCollector(new CustomResultCollector());

      ResultCollector<Object, List<Object>> resultCollector =
          dataSet.withFilter(keySet).setArguments(true).execute(function.getId());

      assertThatThrownBy(() -> resultCollector.getResult()).isInstanceOf(FunctionException.class)
          .hasMessageStartingWith(
              String.format("Cannot %s result as the Function#hasResult() is false",
                  "return any"));
    });
  }

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the function factory
   * present.
   */
  @Test
  public void testRemoteMultiKeyExecution_byInstance() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      Collection<Integer> expectedValues = new HashSet<>();
      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        expectedValues.add(value);
        pr.put(key, value);
      }

      Function<List<Integer>> function = new TestFunction<>(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);

      Execution<Boolean, Boolean, List<Boolean>> booleanExecution = FunctionService.onRegion(pr);
      ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
          booleanExecution.withFilter(keySet).setArguments(true).execute(function.getId());

      List<Boolean> booleanResultList = booleanResultCollector.getResult();
      assertThat(booleanResultList).hasSize(3).containsExactly(true, true, true);

      Execution<Set<String>, List<Integer>, List<List<Integer>>> keysExecution =
          FunctionService.onRegion(pr);
      ResultCollector<List<Integer>, List<List<Integer>>> valuesResultCollector =
          keysExecution.withFilter(keySet).setArguments(keySet).execute(function.getId());

      List<List<Integer>> valueListResultList = valuesResultCollector.getResult();
      assertThat(valueListResultList).hasSize(3);

      Collection<Integer> actualValues = new HashSet<>();
      for (List<Integer> values : valueListResultList) {
        assertThat(values.size()).isGreaterThan(0);
        for (int value : values) {
          assertThat(actualValues.add(value)).isTrue();
        }
      }
      assertThat(actualValues).isEqualTo(expectedValues);
    });
  }

  /**
   * Test bucketFilter functionality
   */
  @Test
  public void testBucketFilter_1() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0, new DivisorResolver(10));
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, new DivisorResolver(10));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_BUCKET_FILTER));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, new DivisorResolver(10));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_BUCKET_FILTER));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, new DivisorResolver(10));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_BUCKET_FILTER));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);
      for (int i = 0; i < 50; ++i) {
        pr.put(i, i);
      }
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION_BUCKET_FILTER);
      FunctionService.registerFunction(function);
      InternalExecution execution = (InternalExecution) FunctionService.onRegion(pr);

      Set<Integer> bucketIdSet = createBucketIdSet(2);

      ResultCollector<Integer, List<Integer>> resultCollector =
          execution.withBucketFilter(bucketIdSet).execute(function);

      List<Integer> results = resultCollector.getResult();
      assertThat(results).hasSameElementsAs(bucketIdSet);
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION_BUCKET_FILTER);
      FunctionService.registerFunction(function);
      InternalExecution execution = (InternalExecution) FunctionService.onRegion(pr);

      Set<Integer> bucketIdSet = createBucketIdSet(2, 3);

      ResultCollector<Integer, List<Integer>> resultCollector =
          execution.withBucketFilter(bucketIdSet).execute(function);

      List<Integer> results = resultCollector.getResult();
      assertThat(results).hasSameElementsAs(bucketIdSet);
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION_BUCKET_FILTER);
      FunctionService.registerFunction(function);
      InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(pr);

      Set<Integer> bucketIdSet = createBucketIdSet(1, 2, 3, 0, 4);

      ResultCollector<Integer, List<Integer>> resultCollector =
          dataSet.withBucketFilter(bucketIdSet).execute(function);

      List<Integer> results = resultCollector.getResult();
      assertThat(results).hasSameElementsAs(bucketIdSet);
    });
  }

  @Test
  public void testBucketFilterOverride() throws Exception {
    VM accessor = getHost(0).getVM(3);
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0, new DivisorResolver(10));
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, new DivisorResolver(10));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_BUCKET_FILTER));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, new DivisorResolver(10));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_BUCKET_FILTER));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, new DivisorResolver(10));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_BUCKET_FILTER));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);
      for (int i = 0; i < 50; ++i) {
        pr.put(i, i);
      }
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION_BUCKET_FILTER);
      FunctionService.registerFunction(function);
      InternalExecution execution = (InternalExecution) FunctionService.onRegion(pr);

      Set<Integer> bucketIdSet = createBucketIdSet(2);
      Set<Integer> keySet = createKeySet(33, 43);
      Set<Integer> expectedBucketIdSet = createBucketIdSet(3, 4);

      ResultCollector<Integer, List<Integer>> resultCollector =
          execution.withBucketFilter(bucketIdSet).withFilter(keySet).execute(function);

      List<Integer> results = resultCollector.getResult();
      assertThat(results).hasSameElementsAs(expectedBucketIdSet);
    });
  }

  /**
   * Test ability to execute a multi-key function by a local data store ResultCollector =
   * DefaultResultCollector haveResult = true
   */
  @Test
  public void testLocalMultiKeyExecution_byName() throws Exception {
    PartitionedRegion pr = createPartitionedRegion(regionName, 10, 0);

    Set<String> keySet = new HashSet<>();
    for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
      keySet.add(STRING_KEY + i);
    }

    Collection<Integer> expectedValues = new HashSet<>();
    int valueIndex = 0;
    for (String key : keySet) {
      int value = valueIndex++;
      expectedValues.add(value);
      pr.put(key, value);
    }

    Function<Integer> function = new TestFunction<>(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);

    Execution<Boolean, Boolean, List<Boolean>> booleanExecution = FunctionService.onRegion(pr);
    ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
        booleanExecution.withFilter(keySet).setArguments(true).execute(function.getId());

    List<Boolean> booleanResults = booleanResultCollector.getResult();
    assertThat(booleanResults).hasSize(1).containsExactly(true);

    Execution<Set<String>, List<Integer>, List<List<Integer>>> valuesExecution =
        FunctionService.onRegion(pr);
    ResultCollector<List<Integer>, List<List<Integer>>> valuesResultCollector =
        valuesExecution.withFilter(keySet).setArguments(keySet).execute(function.getId());

    List<List<Integer>> valuesResults = valuesResultCollector.getResult();
    assertThat(valuesResults).hasSize(1);

    Collection<Integer> actualValues = new HashSet<>();
    for (List<Integer> values : valuesResults) {
      assertThat(values.size()).isGreaterThan(0);
      for (int value : values) {
        assertThat(actualValues.add(value)).isTrue();
      }
    }
    assertThat(actualValues).isEqualTo(expectedValues);
  }

  /**
   * Test ability to execute a multi-key function by a local data store
   */
  @Test
  public void testLocalMultiKeyExecution_byInstance() throws Exception {
    PartitionedRegion pr = createPartitionedRegion(regionName, 10, 0);

    Set<String> keySet = new HashSet<>();
    for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
      keySet.add(STRING_KEY + i);
    }

    Collection<Integer> expectedValues = new HashSet<>();
    int valueIndex = 0;
    for (String key : keySet) {
      int value = valueIndex++;
      expectedValues.add(value);
      pr.put(key, value);
    }

    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);

    Execution<Boolean, Boolean, List<Boolean>> booleanExecution = FunctionService.onRegion(pr);
    ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
        booleanExecution.withFilter(keySet).setArguments(true).execute(function);

    List<Boolean> booleanResults = booleanResultCollector.getResult();
    assertThat(booleanResults).hasSize(1).containsExactly(true);

    Execution<Set<String>, List<Integer>, List<List<Integer>>> valuesExecution =
        FunctionService.onRegion(pr);
    ResultCollector<List<Integer>, List<List<Integer>>> valuesResultCollector =
        valuesExecution.withFilter(keySet).setArguments(keySet).execute(function);

    List<List<Integer>> valuesResults = valuesResultCollector.getResult();
    assertThat(valuesResults).hasSize(1);

    Collection<Integer> actualValues = new HashSet<>();
    for (List<Integer> values : valuesResults) {
      assertThat(values.size()).isGreaterThan(0);
      for (int value : values) {
        assertThat(actualValues.add(value)).isTrue();
      }
    }
    assertThat(actualValues).isEqualTo(expectedValues);
  }

  /**
   * Ensure that the execution is limited to a single bucket put another way, that the routing logic
   * works correctly such that there is not extra execution
   */
  @Test
  public void testMultiKeyExecutionOnASingleBucket_byName() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore3.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      // Assert there is data each bucket
      for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
        assertThat(pr.getBucketKeys(bucketId).size()).isGreaterThan(0);
      }

      Function function = new TestFunction(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);

      for (String key : keySet) {
        Set<String> singleKeySet = createKeySet(key);

        Execution<Boolean, Boolean, List<Boolean>> booleanExecution = FunctionService.onRegion(pr);
        ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
            booleanExecution.withFilter(singleKeySet).setArguments(true).execute(function.getId());

        List<Boolean> booleanResults = booleanResultCollector.getResult();
        assertThat(booleanResults).hasSize(1).containsExactly(true);

        Execution<Set<String>, List<Integer>, List<List<Integer>>> valuesExecution =
            FunctionService.onRegion(pr);
        ResultCollector<List<Integer>, List<List<Integer>>> valuesResultCollector = valuesExecution
            .withFilter(singleKeySet).setArguments(singleKeySet).execute(function.getId());

        List<List<Integer>> valuesResults = valuesResultCollector.getResult();
        assertThat(valuesResults).hasSize(1);

        List<Integer> values = valuesResults.get(0);

        assertThat(values).hasSize(1);
        assertThat(values.get(0)).isEqualTo(pr.get(singleKeySet.iterator().next()));
      }
    });
  }

  /**
   * Ensure that the execution is limited to a single bucket put another way, that the routing logic
   * works correctly such that there is not extra execution
   */
  @Test
  public void testMultiKeyExecutionOnASingleBucket_byInstance() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore3.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 3; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      // Assert there is data each bucket
      for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
        assertThat(pr.getBucketKeys(bucketId).size()).isGreaterThan(0);
      }

      Function function = new TestFunction(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);

      for (String key : keySet) {
        Set<String> singleKeySet = createKeySet(key);

        Execution<Boolean, Boolean, List<Boolean>> booleanExecution = FunctionService.onRegion(pr);
        ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
            booleanExecution.withFilter(singleKeySet).setArguments(true).execute(function);

        List<Boolean> booleanResults = booleanResultCollector.getResult();
        assertThat(booleanResults).hasSize(1).containsExactly(true);

        Execution<Set<String>, List<Integer>, List<List<Integer>>> valuesExecution =
            FunctionService.onRegion(pr);
        ResultCollector<List<Integer>, List<List<Integer>>> valuesResultCollector =
            valuesExecution.withFilter(singleKeySet).setArguments(singleKeySet).execute(function);

        List<List<Integer>> valuesResults = valuesResultCollector.getResult();
        assertThat(valuesResults).hasSize(1);

        List<Integer> values = valuesResults.get(0);

        assertThat(values).hasSize(1);
        assertThat(values.get(0)).isEqualTo(pr.get(singleKeySet.iterator().next()));
      }
    });
  }

  /**
   * Ensure that the execution is happening all the PR as a whole
   */
  @Test
  public void testExecutionOnAllNodes_byName() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore3.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 3; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      // Assert there is data in each bucket
      for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
        assertThat(pr.getBucketKeys(bucketId).size()).isGreaterThan(0);
      }

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> execution = FunctionService.onRegion(pr);

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          execution.setArguments(true).execute(function.getId());

      List<Boolean> results = resultCollector.getResult();
      assertThat(results).hasSize(4).containsExactly(true, true, true, true);
    });
  }

  /**
   * Ensure that the execution is happening all the PR as a whole
   */
  @Test
  public void testExecutionOnAllNodes_byInstance() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM accessor = getHost(0).getVM(3);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(0, 0, 17));
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION2));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 3; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      // Assert there is data in each bucket
      for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
        assertThat(pr.getBucketKeys(bucketId).size()).isGreaterThan(0);
      }

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> dataSet = FunctionService.onRegion(pr);

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          dataSet.setArguments(true).execute(function);

      List<Boolean> booleanResults = resultCollector.getResult();
      assertThat(booleanResults).hasSize(3).containsExactly(true, true, true);
    });
  }

  /**
   * Ensure that the execution of inline function is happening all the PR as a whole
   */
  @Test
  public void testExecutionOnAllNodes_byInlineFunction() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0, 17));
    });

    datastore3.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> keySet = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 3; i > 0; i--) {
        keySet.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : keySet) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      // Assert there is data in each bucket
      for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
        assertThat(pr.getBucketKeys(bucketId).size()).isGreaterThan(0);
      }

      Execution<Boolean, Boolean, List<Boolean>> execution = FunctionService.onRegion(pr);
      ResultCollector<Boolean, List<Boolean>> booleanResultCollector =
          execution.setArguments(true).execute(new BooleanFunction(true));

      List<Boolean> booleanResults = booleanResultCollector.getResult();
      assertThat(booleanResults).hasSize(4).containsExactly(true, true, true, true);
    });
  }

  /**
   * Ensure that the execution is happening on all the PR as a whole with LocalReadPR as
   * LocalDataSet
   */
  @Test
  public void testExecutionOnAllNodes_LocalReadPR() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(regionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore3.invoke(() -> {
      Region<CustId, Customer> region = getPartitionedRegion(regionName);

      Set<CustId> keySet = new HashSet<>();
      for (int i = 1; i <= 10; i++) {
        CustId custId = new CustId(i);
        Customer customer = new Customer("name" + i, "Address" + i);
        region.put(custId, customer);
        keySet.add(custId);
      }

      Function<List<Customer>> function = new TestFunction<>(true, TEST_FUNCTION3);
      FunctionService.registerFunction(function);
      Execution<Set<CustId>, List<Customer>, List<List<Customer>>> execution =
          FunctionService.onRegion(region);

      ResultCollector<List<Customer>, List<List<Customer>>> resultCollector =
          execution.setArguments(keySet).execute(function.getId());

      List<List<Customer>> customerListResults = resultCollector.getResult();
      assertThat(customerListResults).hasSize(4);

      List<Customer> values = new ArrayList<>();
      for (List<Customer> customers : customerListResults) {
        values.addAll(customers);
      }
      assertThat(values).hasSameSizeAs(keySet);
    });
  }

  /**
   * Ensure that the execution is happening on all the PR as a whole with LocalReadPR as
   * LocalDataSet
   */
  @Test
  public void testExecutionOnMultiNodes_LocalReadPR() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    String customerRegionName = "CustomerPartitionedRegionName";
    String orderRegionName = "OrderPartitionedRegionName";

    datastore0.invoke(() -> {
      createPartitionedRegion(customerRegionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(customerRegionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(customerRegionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(customerRegionName, createPartitionAttributes(10, 0,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 17));
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION3));
    });

    datastore0.invoke(() -> {
      createPartitionedRegion(orderRegionName, createPartitionAttributes(customerRegionName, 10,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 0, 17));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(orderRegionName, createPartitionAttributes(customerRegionName, 10,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 0, 17));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(orderRegionName, createPartitionAttributes(customerRegionName, 10,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 0, 17));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(orderRegionName, createPartitionAttributes(customerRegionName, 10,
          new CustomerIDPartitionResolver("CustomerIDPartitionResolver"), 0, 17));
    });

    datastore3.invoke(() -> {
      Region<CustId, Customer> customerRegion = getPartitionedRegion(customerRegionName);

      Set<CustId> keySet = new HashSet<>();
      for (int i = 1; i <= 10; i++) {
        CustId custId = new CustId(i);
        Customer customer = new Customer("name" + i, "Address" + i);
        customerRegion.put(custId, customer);
        keySet.add(custId);
      }

      Region<OrderId, Order> orderRegion = getPartitionedRegion(orderRegionName);

      for (int i = 1; i <= 100; i++) {
        CustId custId = new CustId(i);
        for (int j = 1; j <= 10; j++) {
          int orderIdInt = i * 10 + j;

          OrderId orderId = new OrderId(orderIdInt, custId);
          Order order = new Order("Order" + orderIdInt);

          orderRegion.put(orderId, order);
          // TODO: assertTrue(partitionedregion.containsKey(orderId));
          // TODO: assertIndexDetailsEquals(order,partitionedregion.get(orderId));
        }
      }

      Function<List<Order>> function = new TestFunction<>(true, TEST_FUNCTION3);
      FunctionService.registerFunction(function);
      Execution<Set<CustId>, List<Order>, List<List<Order>>> execution =
          FunctionService.onRegion(customerRegion);

      ResultCollector<List<Order>, List<List<Order>>> resultCollector =
          execution.withFilter(keySet).execute(function.getId());

      Collection<List<Order>> results = resultCollector.getResult();
      assertThat(results.size()).isGreaterThan(0).isLessThanOrEqualTo(4);

      List<Order> values = new ArrayList<>();
      for (List<Order> orderList : results) {
        values.addAll(orderList);
      }
      assertThat(values).hasSameSizeAs(keySet);
    });
  }

  /**
   * Assert the {@link RegionFunctionContext} yields the proper objects.
   */
  @Test
  public void testLocalDataContext() throws Exception {
    VM accessor = getHost(0).getVM(1);
    VM datastore1 = getHost(0).getVM(2);
    VM datastore2 = getHost(0).getVM(3);

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
    });

    int key1 = 1;
    int key2 = 2;

    accessor.invoke(() -> {
      Region<Integer, Integer> region = createPartitionedRegion(regionName, 0, 0);
      // Assuming that bucket balancing will create a single bucket (per key)
      // in different datastores
      region.put(key1, key1);
      region.put(key2, key2);
    });

    accessor.invoke(() -> validateLocalDataContext(key1, key2));
    datastore1.invoke(() -> validateLocalDataContext(key1, key2));
    datastore2.invoke(() -> validateLocalDataContext(key1, key2));
  }

  private void validateLocalDataContext(final int key1, final int key2) {
    Region region = getPartitionedRegion(regionName);
    Function function = new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        RegionFunctionContext regionFunctionContext = (RegionFunctionContext) context;

        Iterable<Integer> filterSet = (Set<Integer>) regionFunctionContext.getFilter();
        assertThat(filterSet).hasSize(1).containsExactly(key1);

        Region<Integer, Integer> dataSet = regionFunctionContext.getDataSet();
        assertThat(PartitionRegionHelper.isPartitionedRegion(dataSet)).isTrue();

        Region localData = PartitionRegionHelper.getLocalDataForContext(regionFunctionContext);
        assertThat(PartitionRegionHelper.getColocatedRegions(localData)).isEmpty();

        // Assert the data is local only
        assertThat(localData.get(key2)).isNull();
        assertThat(localData.get(key1)).isEqualTo(key1);

        Set<Integer> localDataKeySet = localData.keySet();
        assertThat(localDataKeySet).hasSize(1).containsExactly(key1);

        Collection<Integer> localDataValuesCollection = localData.values();
        assertThat(localDataValuesCollection).hasSize(1).containsExactly(key1);

        validateLocalEntrySet(key1, localData.entrySet());

        Set<Map.Entry<Integer, Integer>> localDataEntrySet = localData.entrySet();
        assertThat(localDataEntrySet).hasSize(1);
        Map.Entry<Integer, Integer> localDataEntry = localDataEntrySet.iterator().next();
        assertThat(localDataEntry.getKey()).isEqualTo(key1);
        assertThat(localDataEntry.getValue()).isEqualTo(key1);

        context.getResultSender().lastResult(true);
      }

      // @Override
      @Override
      public String getId() {
        return getClass().getName();
      }
    };

    List<Boolean> results = (List) FunctionService.onRegion(region)
        .withFilter(Collections.singleton(key1)).execute(function).getResult();

    assertThat(results).hasSize(1).containsExactly(true);
  }

  /**
   * Assert the {@link RegionFunctionContext} yields the proper objects.
   */
  @Test
  public void testLocalDataContextWithColocation() throws Exception {
    VM accessor = getHost(0).getVM(1);
    VM datastore1 = getHost(0).getVM(2);
    VM datastore2 = getHost(0).getVM(3);

    datastore1.invoke(() -> createColocatedPartitionedRegions(10));
    datastore2.invoke(() -> createColocatedPartitionedRegions(10));
    accessor.invoke(() -> createColocatedPartitionedRegions(0));

    int key1 = 1;
    int key2 = 2;

    accessor.invoke(() -> {
      Region<Integer, Integer> regionTop = getCache().getRegion(regionNameTop);
      Region<Integer, Integer> regionColocated1 = getCache().getRegion(regionNameColocated1);
      Region<Integer, Integer> regionColocated2 =
          getCache().getRegion("root" + SEPARATOR + regionNameColocated2);

      // Assuming that bucket balancing will create a single bucket (per key)
      // in different datastores
      regionTop.put(key1, key1);
      regionTop.put(key2, key2);
      regionColocated1.put(key1, key1);
      regionColocated1.put(key2, key2);
      regionColocated2.put(key1, key1);
      regionColocated2.put(key2, key2);
    });

    accessor.invoke(() -> validateRegionFunctionContextForColocatedRegions(key1, key2));
    datastore1.invoke(() -> validateRegionFunctionContextForColocatedRegions(key1, key2));
    datastore2.invoke(() -> validateRegionFunctionContextForColocatedRegions(key1, key2));
  }

  private void createColocatedPartitionedRegions(final int localMaxMemory) {
    createRootRegion(regionNameTop,
        createColocatedPartitionedRegionAttributes(null, localMaxMemory, 0));
    RegionAttributes colo =
        createColocatedPartitionedRegionAttributes(regionNameTop, localMaxMemory, 0);
    createRootRegion(regionNameColocated1, colo);
    createRegion(regionNameColocated2, "root", colo);
  }

  private void validateRegionFunctionContextForColocatedRegions(final int key1, final int key2) {
    Region rootRegion = getRootRegion(regionNameTop);
    Function function = new Function() {
      @Override
      public boolean hasResult() {
        return true;
      }

      @Override
      public void execute(FunctionContext context) {
        RegionFunctionContext regionFunctionContext = (RegionFunctionContext) context;
        Iterable<Integer> filterSet = (Set<Integer>) regionFunctionContext.getFilter();
        assertThat(filterSet).hasSize(1).containsExactly(key1);

        Region region = regionFunctionContext.getDataSet();
        assertThat(region).isInstanceOf(PartitionedRegion.class);

        Map<String, ? extends Region> colocatedRegionsMap =
            PartitionRegionHelper.getColocatedRegions(region);
        assertThat(colocatedRegionsMap).doesNotContainKey(regionNameTop);

        Region localData = PartitionRegionHelper.getLocalDataForContext(regionFunctionContext);
        Map<String, ? extends Region> colocatedLocalDataMap =
            PartitionRegionHelper.getColocatedRegions(localData);
        assertThat(colocatedLocalDataMap).doesNotContainKey(regionNameTop);

        Region colocatedRegion1 = getRootRegion(regionNameColocated1);
        Region colocatedRegion2 = getRootRegion().getSubregion(regionNameColocated2);
        {
          assertThat(colocatedRegionsMap.get(colocatedRegion1.getFullPath()))
              .isSameAs(colocatedRegion1);

          Region localDataRegion = PartitionRegionHelper.getLocalData(colocatedRegion1);
          assertThat(localDataRegion).isInstanceOf(LocalDataSet.class);

          validateLocalKeySet(key1, localDataRegion.keySet());
          validateLocalValues(key1, localDataRegion.values());
          validateLocalEntrySet(key1, localDataRegion.entrySet());
        }
        {
          assertThat(colocatedRegionsMap.get(colocatedRegion2.getFullPath()))
              .isSameAs(colocatedRegion2);

          Region localDataRegion = PartitionRegionHelper.getLocalData(colocatedRegion2);
          assertThat(localDataRegion).isInstanceOf(LocalDataSet.class);

          validateLocalEntrySet(key1, localDataRegion.entrySet());
          validateLocalKeySet(key1, localDataRegion.keySet());
          validateLocalValues(key1, localDataRegion.values());
        }

        // Assert context's local colocated data
        {
          Region localDataRegion = colocatedLocalDataMap.get(colocatedRegion1.getFullPath());
          assertThat(localDataRegion.getFullPath()).isEqualTo(colocatedRegion1.getFullPath());
          assertThat(localDataRegion).isInstanceOf(LocalDataSet.class);

          validateLocalEntrySet(key1, localDataRegion.entrySet());
          validateLocalKeySet(key1, localDataRegion.keySet());
          validateLocalValues(key1, localDataRegion.values());
        }
        {
          Region localDataRegion = colocatedLocalDataMap.get(colocatedRegion2.getFullPath());
          assertThat(localDataRegion.getFullPath()).isEqualTo(colocatedRegion2.getFullPath());
          assertThat(localDataRegion).isInstanceOf(LocalDataSet.class);

          validateLocalEntrySet(key1, localDataRegion.entrySet());
          validateLocalKeySet(key1, localDataRegion.keySet());
          validateLocalValues(key1, localDataRegion.values());
        }

        // Assert both local forms of the "target" region is local only
        assertThat(localData.get(key2)).isNull();
        assertThat(localData.get(key1)).isEqualTo(key1);

        validateLocalEntrySet(key1, localData.entrySet());
        validateLocalKeySet(key1, localData.keySet());
        validateLocalValues(key1, localData.values());
        context.getResultSender().lastResult(true);
      }

      // @Override
      @Override
      public String getId() {
        return getClass().getName();
      }
    };

    ResultCollector<Boolean, List<Boolean>> resultCollector =
        FunctionService.onRegion(rootRegion).withFilter(createKeySet(key1)).execute(function);
    assertThat(resultCollector.getResult()).hasSize(1).containsExactly(true);
  }

  private List<Boolean> executeFunction() {
    PartitionedRegion pr = getPartitionedRegion(regionName);

    Set<String> keySet = new HashSet<>();
    for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
      keySet.add(STRING_KEY + i);
    }

    Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION_HA);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(pr);

    ResultCollector<Boolean, List<Boolean>> resultCollector =
        dataSet.withFilter(keySet).setArguments(true).execute(function.getId());

    return resultCollector.getResult();
  }

  private void validateLocalValues(final int value, final Iterable<Integer> values) {
    assertThat(Collections.singleton(value)).isEqualTo(values);
    assertThat(values).contains(value);
    assertThat(values).hasSize(1);

    Iterator iterator = values.iterator();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(value);
    assertThat(iterator.hasNext()).isFalse();
  }

  private void validateLocalKeySet(final int key, final Iterable<Integer> keys) {
    assertThat(Collections.singleton(key)).isEqualTo(keys);
    assertThat(keys).hasSize(1);
  }

  private void validateLocalEntrySet(final int key, final Iterable<Integer> entries) {
    assertThat(entries).hasSize(1);

    Iterator iterator = entries.iterator();
    assertThat(iterator.hasNext()).isTrue();

    Entry regionEntry = (Entry) iterator.next();
    if (regionEntry instanceof EntrySnapshot) {
      assertThat(((EntrySnapshot) regionEntry).wasInitiallyLocal()).isTrue();
    } else {
      assertThat(regionEntry.isLocal()).isTrue();
    }

    assertThat(regionEntry.getKey()).isEqualTo(key);
    assertThat(regionEntry.getValue()).isEqualTo(key);
    assertThat(iterator.hasNext()).isFalse();
  }

  private RegionAttributes createColocatedPartitionedRegionAttributes(final String colocatedWith,
      final int localMaxMemory, final int redundancy) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setColocatedWith(colocatedWith);
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setPartitionResolver(new SimpleResolver());
    paf.setRedundantCopies(redundancy);

    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    return af.create();
  }

  private PartitionedRegion createPartitionedRegion(final String regionName,
      final int localMaxMemory, final int redundancy) {
    PartitionAttributes pa = createPartitionAttributes(localMaxMemory, redundancy);
    return createPartitionedRegion(regionName, pa);
  }

  private PartitionedRegion createPartitionedRegion(final String regionName,
      final int localMaxMemory, final int redundancy, final PartitionResolver resolver) {
    PartitionAttributes pa = createPartitionAttributes(localMaxMemory, redundancy, resolver);
    return createPartitionedRegion(regionName, pa);
  }

  private PartitionedRegion createPartitionedRegion(final String regionName,
      final PartitionAttributes partitionAttributes) {
    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributes);
    return (PartitionedRegion) regionFactory.create(regionName);
  }

  private PartitionAttributes createPartitionAttributes(final int localMaxMemory,
      final int redundancy) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);
    return paf.create();
  }

  private PartitionAttributes createPartitionAttributes(final int localMaxMemory,
      final int redundancy, final PartitionResolver resolver) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setPartitionResolver(resolver);
    paf.setRedundantCopies(redundancy);
    return paf.create();
  }

  private PartitionAttributes createPartitionAttributes(final int localMaxMemory,
      final int redundancy, final PartitionResolver resolver, final int totalNumBuckets) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setPartitionResolver(resolver);
    paf.setRedundantCopies(redundancy);
    paf.setTotalNumBuckets(totalNumBuckets);
    return paf.create();
  }

  private PartitionAttributes createPartitionAttributes(final int localMaxMemory,
      final int redundancy, final long startupRecoveryDelay, final int totalNumBuckets) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);
    paf.setStartupRecoveryDelay(startupRecoveryDelay);
    paf.setTotalNumBuckets(totalNumBuckets);
    return paf.create();
  }

  private PartitionAttributes createPartitionAttributes(final int localMaxMemory,
      final int redundancy, final int totalNumBuckets) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);
    paf.setTotalNumBuckets(totalNumBuckets);
    return paf.create();
  }

  private PartitionAttributes createPartitionAttributes(final String colocatedWith,
      final int localMaxMemory, final PartitionResolver resolver, final int redundancy,
      final int totalNumBuckets) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setColocatedWith(colocatedWith);
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setPartitionResolver(resolver);
    paf.setRedundantCopies(redundancy);
    paf.setTotalNumBuckets(totalNumBuckets);
    return paf.create();
  }

  private PartitionedRegion getPartitionedRegion(final String regionName) {
    return (PartitionedRegion) getCache().getRegion(regionName);
  }

  private Set<String> createKeySet(final String... keys) {
    Set<String> keySet = new HashSet<>();
    for (String key : keys) {
      keySet.add(key);
    }
    return keySet;
  }

  private Set<Integer> createKeySet(final int... keys) {
    Set<Integer> keySet = new HashSet<>();
    for (int key : keys) {
      keySet.add(key);
    }
    return keySet;
  }

  private Map<String, Integer> createEntryMap(final int count, final String keyPrefix,
      final int startKey, final int startValue) {
    Map<String, Integer> entryMap = new HashMap<>();
    int value = startValue;
    for (int keyIndex = startKey; keyIndex < startKey + count; keyIndex++) {
      entryMap.put(keyPrefix + keyIndex, value++);
    }
    return entryMap;
  }

  private Set<Integer> createBucketIdSet(final int... bucketIds) {
    Set<Integer> bucketIdSet = new HashSet<>();
    for (int bucketId : bucketIds) {
      bucketIdSet.add(bucketId);
    }
    return bucketIdSet;
  }

  private static class SimpleResolver implements PartitionResolver, Serializable {

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return (Serializable) opDetails.getKey();
    }

    @Override
    public String getName() {
      return getClass().getName();
    }

    @Override
    public void close() {
      // nothing
    }
  }

  private static class DivisorResolver implements PartitionResolver {

    private final int divisor;

    DivisorResolver(final int divisor) {
      this.divisor = divisor;
    }

    @Override
    public Object getRoutingObject(EntryOperation opDetails) {
      Object key = opDetails.getKey();
      return key.hashCode() / divisor;
    }

    @Override
    public String getName() {
      return getClass().getName();
    }

    @Override
    public void close() {
      // nothing
    }
  }
}
