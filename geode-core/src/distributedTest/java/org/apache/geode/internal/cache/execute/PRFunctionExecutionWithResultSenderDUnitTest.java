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
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION2;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION9;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_NO_LASTRESULT;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({FunctionServiceTest.class})
@SuppressWarnings("serial")
public class PRFunctionExecutionWithResultSenderDUnitTest extends CacheTestCase {

  private static final String STRING_KEY = "execKey";

  private String regionName;

  @Before
  public void setUp() {
    regionName = getUniqueName();
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

      Set<String> stringKeys = new HashSet<>();
      stringKeys.add(STRING_KEY);

      Function function = new TestFunction(true, TEST_FUNCTION2);
      FunctionService.registerFunction(function);
      Execution execution = FunctionService.onRegion(region);

      region.put(STRING_KEY, 1);
      region.put(STRING_KEY + "3", 3);
      region.put(STRING_KEY + "4", 4);

      stringKeys.add(STRING_KEY + "3");
      stringKeys.add(STRING_KEY + "4");

      ResultCollector<Boolean, List<Boolean>> resultCollector1 =
          execution.withFilter(stringKeys).setArguments(true).execute(function.getId());
      assertThat(resultCollector1.getResult()).hasSize(1).containsExactly(true);

      ResultCollector<List<Integer>, List<List<Integer>>> resultCollector2 =
          execution.withFilter(stringKeys).setArguments(stringKeys).execute(function.getId());
      List<List<Integer>> values = resultCollector2.getResult();
      assertThat(values).hasSize(1);
      assertThat(values.get(0)).hasSize(3).containsOnly(1, 3, 4);

      Map<String, Integer> putData = new HashMap<>();
      putData.put(STRING_KEY + "1", 2);
      putData.put(STRING_KEY + "2", 3);

      ResultCollector<Boolean, List<Boolean>> resultCollector3 =
          execution.withFilter(stringKeys).setArguments(putData).execute(function.getId());
      assertThat(resultCollector3.getResult()).hasSize(1).containsExactly(true);

      assertThat(region.get(STRING_KEY + "1")).isEqualTo(2);
      assertThat(region.get(STRING_KEY + "2")).isEqualTo(3);
    });
  }

  /**
   * Test remote execution by a pure accessor which doesn't have the function factory present And
   * the function doesn't send last result. FunctionException is expected in this case
   */
  @Test
  public void testRemoteExecution_NoLastResult() throws Exception {
    VM accessor = getHost(0).getVM(0);
    VM datastore = getHost(0).getVM(1);

    accessor.invoke(() -> {
      createPartitionedRegion(regionName, 0, 0);
    });

    datastore.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_NO_LASTRESULT));
    });

    accessor.invoke(() -> {
      Region<String, Integer> region = getPartitionedRegion(regionName);

      Set<String> stringKeys = new HashSet<>();
      stringKeys.add(STRING_KEY);

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION_NO_LASTRESULT);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> execution = FunctionService.onRegion(region);

      region.put(STRING_KEY, 1);
      region.put(STRING_KEY + "3", 3);
      region.put(STRING_KEY + "4", 4);

      stringKeys.add(STRING_KEY + "3");
      stringKeys.add(STRING_KEY + "4");

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          execution.withFilter(stringKeys).setArguments(true).execute(function.getId());

      assertThatThrownBy(resultCollector::getResult).isInstanceOf(FunctionException.class)
          .hasMessageContaining("did not send last result");
    });
  }

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the function factory
   * present.
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
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION9));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION9));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION9));
    });

    accessor.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Set<String> stringKeys = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
        stringKeys.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : stringKeys) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      Function function = new TestFunction(true, TEST_FUNCTION9);
      FunctionService.registerFunction(function);

      Execution<Boolean, Boolean, List<Boolean>> booleanExecution = FunctionService.onRegion(pr);
      ResultCollector<Boolean, List<Boolean>> resultCollector1 =
          booleanExecution.withFilter(stringKeys).setArguments(true).execute(function.getId());
      List<Boolean> booleanResults = resultCollector1.getResult();
      assertThat(booleanResults).hasSize(3).containsExactly(true, true, true);

      Execution<Set<String>, List<Integer>, List<List<Integer>>> execution =
          FunctionService.onRegion(pr);
      ResultCollector<List<Integer>, List<List<Integer>>> resultCollector2 =
          execution.withFilter(stringKeys).setArguments(stringKeys).execute(function.getId());
      List<List<Integer>> valuesResults = resultCollector2.getResult();
      assertThat(valuesResults).hasSize(pr.getTotalNumberOfBuckets() * 2 * 3);
    });
  }

  /**
   * Test ability to execute a multi-key function by a local data store
   * <p>
   * TODO: extract to IntegrationTest
   */
  @Test
  public void testLocalMultiKeyExecution_byName() throws Exception {
    PartitionedRegion pr = createPartitionedRegion(regionName, 10, 0);

    Set<String> stringKeys = new HashSet<>();
    for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
      stringKeys.add(STRING_KEY + i);
    }

    int valueIndex = 0;
    for (String key : stringKeys) {
      int value = valueIndex++;
      pr.put(key, value);
    }

    Function function = new TestFunction(true, TEST_FUNCTION9);
    FunctionService.registerFunction(function);
    Execution execution = FunctionService.onRegion(pr);

    ResultCollector<Boolean, List<Boolean>> resultCollector1 =
        execution.withFilter(stringKeys).setArguments(true).execute(function.getId());
    List<Boolean> results = resultCollector1.getResult();
    assertThat(results).hasSize(1).containsExactly(true);

    ResultCollector<List<Integer>, List<List<Integer>>> resultCollector2 =
        execution.withFilter(stringKeys).setArguments(stringKeys).execute(function.getId());
    List<List<Integer>> valuesResults = resultCollector2.getResult();
    assertThat(valuesResults).hasSize(pr.getTotalNumberOfBuckets() * 2);
  }

  /**
   * Test local execution on datastore with function that doesn't send last result.
   * FunctionException is expected in this case
   *
   * <p>
   * TODO: extract to IntegrationTest
   */
  @Test
  public void testLocalExecution_NoLastResult() throws Exception {
    PartitionedRegion pr = createPartitionedRegion(regionName, 10, 0);

    Set<String> stringKeys = new HashSet<>();
    for (int i = pr.getTotalNumberOfBuckets() * 2; i > 0; i--) {
      stringKeys.add(STRING_KEY + i);
    }

    int valueIndex = 0;
    for (String key : stringKeys) {
      int value = valueIndex++;
      pr.put(key, value);
    }

    Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION_NO_LASTRESULT);
    FunctionService.registerFunction(function);
    Execution<Boolean, Boolean, List<Boolean>> execution = FunctionService.onRegion(pr);

    ResultCollector<Boolean, List<Boolean>> resultCollector =
        execution.withFilter(stringKeys).setArguments(true).execute(function.getId());

    assertThatThrownBy(resultCollector::getResult).isInstanceOf(FunctionException.class)
        .hasMessageContaining("did not send last result");
  }

  /**
   * Test execution on all datastores with function that doesn't send last result. FunctionException
   * is expected in this case
   */
  @Test
  public void testExecutionOnAllNodes_NoLastResult() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_NO_LASTRESULT));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_NO_LASTRESULT));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_NO_LASTRESULT));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION_NO_LASTRESULT));
    });

    datastore3.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> stringKeys = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 3; i > 0; i--) {
        stringKeys.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : stringKeys) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      // Assert there is data in each bucket
      for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
        assertThat(pr.getBucketKeys(bucketId).size()).isGreaterThan(0);
      }

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION_NO_LASTRESULT);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> execution = FunctionService.onRegion(pr);

      ResultCollector resultCollector = execution.setArguments(true).execute(function.getId());

      assertThatThrownBy(resultCollector::getResult).isInstanceOf(FunctionException.class)
          .hasMessageContaining("did not send last result");
    });
  }

  @Test
  public void testExecutionOnAllNodes_byName() throws Exception {
    VM datastore0 = getHost(0).getVM(0);
    VM datastore1 = getHost(0).getVM(1);
    VM datastore2 = getHost(0).getVM(2);
    VM datastore3 = getHost(0).getVM(3);

    datastore0.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION9));
    });

    datastore1.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION9));
    });

    datastore2.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION9));
    });

    datastore3.invoke(() -> {
      createPartitionedRegion(regionName, 10, 0, 17);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION9));
    });

    datastore3.invoke(() -> {
      PartitionedRegion pr = getPartitionedRegion(regionName);

      Collection<String> stringKeys = new HashSet<>();
      for (int i = pr.getTotalNumberOfBuckets() * 3; i > 0; i--) {
        stringKeys.add(STRING_KEY + i);
      }

      int valueIndex = 0;
      for (String key : stringKeys) {
        int value = valueIndex++;
        pr.put(key, value);
      }

      // Assert there is data in each bucket
      for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
        assertThat(pr.getBucketKeys(bucketId).size()).isGreaterThan(0);
      }

      Function<Boolean> function = new TestFunction<>(true, TEST_FUNCTION9);
      FunctionService.registerFunction(function);
      Execution<Boolean, Boolean, List<Boolean>> execution = FunctionService.onRegion(pr);

      ResultCollector<Boolean, List<Boolean>> resultCollector =
          execution.setArguments(true).execute(function.getId());

      List<Boolean> results = resultCollector.getResult();
      assertThat(results).hasSize(4).containsExactly(true, true, true, true);
    });
  }

  /**
   * TODO: extract to IntegrationTest with RegressionTest suffix
   * <p>
   * TODO: this test has nothing to do with PartitionedRegions. Move it to a FunctionService test
   * <p>
   * TRAC #41832: Function execution should throw Exceptions when any configuration is not valid
   */
  @Test
  public void executeThrowsIfRegionIsNotReplicated() throws Exception {
    Properties config = new Properties();
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");
    getCache(config);

    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY); // ie, a NON-REPLICATED region

    Region region = getCache().createRegion(regionName, factory.create());

    Function function = new TestFunction(true, TEST_FUNCTION2);
    FunctionService.registerFunction(function);

    Execution execution = FunctionService.onRegion(region).setArguments(true);

    assertThatThrownBy(() -> execution.execute(function.getId()))
        .isInstanceOf(FunctionException.class)
        .hasMessageStartingWith("No Replicated Region found for executing function");
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
      final int localMaxMemory, final int redundancy, final int totalNumBuckets) {
    PartitionAttributes pa = createPartitionAttributes(localMaxMemory, redundancy, totalNumBuckets);
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
}
