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

import static java.lang.Boolean.TRUE;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_SEND_EXCEPTION;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_THROW_EXCEPTION;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(FunctionServiceTest.class)
@SuppressWarnings("serial")
public class FunctionExecutionWithExceptionDistributedTest implements Serializable {

  private String regionName;
  private VM datastoreVM0;
  private VM datastoreVM1;
  private VM datastoreVM2;

  private String stringKey;
  private Set<String> stringKeys;

  private Set<Integer> intKeys;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder()
      .addConfig(SERIALIZABLE_OBJECT_FILTER, MyFunctionExecutionException.class.getName()).build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();

    datastoreVM0 = getVM(0);
    datastoreVM1 = getVM(1);
    datastoreVM2 = getVM(2);

    stringKey = "execKey";
    stringKeys = new HashSet<>();
    stringKeys.add(stringKey);

    intKeys = new HashSet<>();
    for (int i = 0; i < 4; i++) {
      intKeys.add(i);
    }
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  private void createPartitionedRegion(final String regionName) {
    createPartitionedRegion(regionName, 10);
  }

  private void createPartitionedRegion(final String regionName, final int localMaxMemory) {
    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0).setLocalMaxMemory(localMaxMemory);

    RegionFactory<?, ?> regionFactory = cacheRule.getOrCreateCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  @Test
  public void testSingleKeyExecution_SendException_Datastore() {
    createPartitionedRegion(regionName);
    Region<String, Integer> region = cacheRule.getCache().getRegion(regionName);
    region.put(stringKey, 1);

    Function function = new TestFunction(true, TEST_FUNCTION_SEND_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    ResultCollector resultCollector =
        dataSet.withFilter(stringKeys).setArguments(TRUE).execute(function);
    List results = (List) resultCollector.getResult();
    assertThat(results.get(0) instanceof Exception).isTrue();

    resultCollector = dataSet.withFilter(stringKeys).setArguments(stringKeys).execute(function);
    results = (List) resultCollector.getResult();
    assertThat(results).hasSize(stringKeys.size() + 1);

    Iterator resultIterator = results.iterator();
    int exceptionCount = 0;
    while (resultIterator.hasNext()) {
      Object result = resultIterator.next();
      if (result instanceof MyFunctionExecutionException) {
        exceptionCount++;
      }
    }

    assertThat(exceptionCount).isEqualTo(1);
  }

  @Test
  public void testSingleKeyExecution_SendException_MultipleTimes_Datastore() {
    createPartitionedRegion(regionName);
    Region<String, Integer> region = cacheRule.getCache().getRegion(regionName);
    region.put(stringKey, 1);

    Function function = new TestFunction(true, TEST_FUNCTION_SEND_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    ResultCollector resultCollector =
        dataSet.withFilter(stringKeys).setArguments("Multiple").execute(function);
    List results = (List) resultCollector.getResult();
    assertThat(results.get(0) instanceof Exception).isTrue();
  }

  @Test
  public void testRemoteSingleKeyExecution_ThrowException_Datastore() {
    createPartitionedRegion(regionName);
    Region<String, Integer> region = cacheRule.getCache().getRegion(regionName);

    Function function = new TestFunction(true, TEST_FUNCTION_THROW_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(stringKey, 1);

    ResultCollector resultCollector =
        dataSet.withFilter(stringKeys).setArguments(TRUE).execute(function);
    assertThatThrownBy(() -> resultCollector.getResult()).isInstanceOf(Exception.class);
  }

  @Test
  public void testRemoteSingleKeyExecution_SendException_Accessor() {
    createPartitionedRegion(regionName, 0);

    datastoreVM0.invoke("createPartitionedRegion", () -> {
      createPartitionedRegion(regionName);

      Function function = new TestFunction(true, TEST_FUNCTION_SEND_EXCEPTION);
      FunctionService.registerFunction(function);
    });

    Region<String, Integer> region = cacheRule.getCache().getRegion(regionName);

    Function function = new TestFunction(true, TEST_FUNCTION_SEND_EXCEPTION);
    FunctionService.registerFunction(function);

    Execution dataSet = FunctionService.onRegion(region);

    region.put(stringKey, 1);

    ResultCollector resultCollector =
        dataSet.withFilter(stringKeys).setArguments(stringKeys).execute(function);
    List results = (List) resultCollector.getResult();
    assertThat(results).hasSize(stringKeys.size() + 1);

    Iterator resultIterator = results.iterator();
    int exceptionCount = 0;
    while (resultIterator.hasNext()) {
      Object result = resultIterator.next();
      if (result instanceof MyFunctionExecutionException) {
        exceptionCount++;
      }
    }

    assertThat(exceptionCount).isEqualTo(1);
  }

  @Test
  public void testRemoteSingleKeyExecution_ThrowException_Accessor() {
    createPartitionedRegion(regionName, 0);

    datastoreVM0.invoke("createPartitionedRegion", () -> {
      createPartitionedRegion(regionName);

      Function function = new TestFunction(true, TEST_FUNCTION_THROW_EXCEPTION);
      FunctionService.registerFunction(function);
    });

    Region<String, Integer> region = cacheRule.getCache().getRegion(regionName);

    Function function = new TestFunction(true, TEST_FUNCTION_THROW_EXCEPTION);
    FunctionService.registerFunction(function);

    Execution dataSet = FunctionService.onRegion(region);

    region.put(stringKey, 1);

    ResultCollector resultCollector =
        dataSet.withFilter(stringKeys).setArguments(TRUE).execute(function);
    assertThatThrownBy(() -> resultCollector.getResult()).isInstanceOf(Exception.class);
  }

  @Test
  public void testRemoteMultiKeyExecution_SendException() {
    createPartitionedRegion(regionName, 0);

    datastoreVM0.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM1.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM2.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));

    Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);

    Function function = new TestFunction(true, TEST_FUNCTION_SEND_EXCEPTION);
    FunctionService.registerFunction(function);

    Execution dataSet = FunctionService.onRegion(region);

    for (Integer key : intKeys) {
      region.put(key, "MyValue_" + key);
    }

    ResultCollector resultCollector =
        dataSet.withFilter(intKeys).setArguments(intKeys).execute(function);
    List results = (List) resultCollector.getResult();
    assertThat(results).hasSize((intKeys.size() * 3) + 3);

    Iterator resultIterator = results.iterator();
    int exceptionCount = 0;
    while (resultIterator.hasNext()) {
      Object result = resultIterator.next();
      if (result instanceof MyFunctionExecutionException) {
        exceptionCount++;
      }
    }

    assertThat(exceptionCount).isEqualTo(3);
  }

  @Test
  public void testRemoteAllKeyExecution_SendException() {
    createPartitionedRegion(regionName);

    datastoreVM0.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM1.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM2.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));

    Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);

    Function function = new TestFunction(true, TEST_FUNCTION_SEND_EXCEPTION);
    FunctionService.registerFunction(function);

    Execution dataSet = FunctionService.onRegion(region);

    for (Integer key : intKeys) {
      region.put(key, "MyValue_" + key);
    }

    ResultCollector resultCollector =
        dataSet.withFilter(intKeys).setArguments(intKeys).execute(function);
    List results = (List) resultCollector.getResult();
    assertThat(results).hasSize((intKeys.size() * 4) + 4);

    Iterator resultIterator = results.iterator();
    int exceptionCount = 0;
    while (resultIterator.hasNext()) {
      Object result = resultIterator.next();
      if (result instanceof MyFunctionExecutionException) {
        exceptionCount++;
      }
    }

    assertThat(exceptionCount).isEqualTo(4);
  }

  @Test
  public void testRemoteMultiKeyExecution_ThrowException() {
    createPartitionedRegion(regionName, 0);

    datastoreVM0.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM1.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM2.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));

    Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);

    Function function = new TestFunction(true, TEST_FUNCTION_THROW_EXCEPTION);
    FunctionService.registerFunction(function);

    Execution dataSet = FunctionService.onRegion(region);

    for (Integer key : intKeys) {
      region.put(key, "MyValue_" + key);
    }

    ResultCollector resultCollector =
        dataSet.withFilter(intKeys).setArguments(intKeys).execute(function.getId());
    assertThatThrownBy(() -> resultCollector.getResult()).isInstanceOf(Exception.class);
  }

  @Test
  public void testRemoteAllKeyExecution_ThrowException() {
    createPartitionedRegion(regionName);

    datastoreVM0.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM1.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));
    datastoreVM2.invoke("createPartitionedRegion", () -> createPartitionedRegion(regionName));

    Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);

    Function function = new TestFunction(true, TEST_FUNCTION_THROW_EXCEPTION);
    FunctionService.registerFunction(function);

    Execution dataSet = FunctionService.onRegion(region);

    for (Integer key : intKeys) {
      region.put(key, "MyValue_" + key);
    }

    ResultCollector resultCollector =
        dataSet.withFilter(intKeys).setArguments(intKeys).execute(function.getId());
    assertThatThrownBy(() -> resultCollector.getResult()).isInstanceOf(Exception.class);
  }
}
