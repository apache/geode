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

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

/**
 * TRAC #40714: Registering a function on a Java client changes the behavior when executing an
 * instance of the function
 *
 * On a Java client if a function is registered with id="f1":
 *
 * <pre>
 * FunctionService.registerFunction(new FunctionAdapter() {
 *   private final Integer state = 1;
 *
 *   public String getId() {
 *     return "f1";
 *   }
 *
 *   public void execute(FunctionContext fc) {}
 * });
 * </pre>
 *
 * But execute a particular instance of a function e.g.:
 *
 * <pre>
 * FunctionService.onRegion(r).execute(new FunctionAdapter() {
 *   private final Integer state = 2;
 *
 *   public String getId() {
 *     return "f1";
 *   }
 *
 *   public void execute(FunctionContext fc) {}
 * });
 * </pre>
 *
 * The execution code does not serialize (nor execute) the instance which was given. The problems
 * are:
 * <ul>
 *
 * <li>Confusing behavior, the programmer intended an instance (and its state) to be serialized and
 * executed.</li>
 *
 * <li>Assumes that a function with the same Id is registered on the server which is not necessarily
 * the case.</li>
 *
 * </ul>
 *
 * <p>
 * Extracted from {@link PRFunctionExecutionDUnitTest}.
 */
@Category({FunctionServiceTest.class})
public class ExecuteFunctionInstanceRegressionTest extends CacheTestCase {

  private String regionName;

  private VM datastore0;
  private VM datastore1;
  private VM datastore2;
  private VM datastore3;

  @Before
  public void setUp() {
    datastore0 = getHost(0).getVM(0);
    datastore1 = getHost(0).getVM(1);
    datastore2 = getHost(0).getVM(2);
    datastore3 = getHost(0).getVM(3);

    regionName = getUniqueName();

    getCache();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.test.junit.rules.**;org.apache.geode.internal.cache.execute.**;org.apache.geode.internal.cache.functions.**;org.apache.geode.test.dunit.**");
    return config;
  }

  @Test
  public void providedFunctionInstanceShouldBeUsed() throws Exception {
    datastore0.invoke(() -> createPartitionedRegion());
    datastore1.invoke(() -> createPartitionedRegion());
    datastore2.invoke(() -> createPartitionedRegion());
    datastore3.invoke(() -> createPartitionedRegion());

    datastore0.invoke(() -> FunctionService.registerFunction(new BooleanFunction(false)));
    datastore1.invoke(() -> FunctionService.registerFunction(new BooleanFunction(false)));
    datastore2.invoke(() -> FunctionService.registerFunction(new BooleanFunction(false)));
    datastore3.invoke(() -> FunctionService.registerFunction(new BooleanFunction(false)));

    datastore3.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(regionName);
      createAllBuckets(pr);

      ResultCollector<Boolean, Collection<Boolean>> rc =
          executeFunctionWithId(pr, BooleanFunction.class.getName());
      validateResults(rc, false);
    });

    datastore3.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(regionName);
      ResultCollector<Boolean, Collection<Boolean>> rc =
          executeFunctionWithInstance(pr, new BooleanFunction(true));

      validateResults(rc, true);
    });
  }

  private void createPartitionedRegion() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(10);
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(17);

    RegionFactory<Integer, Integer> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void createAllBuckets(final PartitionedRegion pr) {
    for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
      pr.put(bucketId, bucketId);
    }

    for (int bucketId = 0; bucketId < pr.getTotalNumberOfBuckets(); bucketId++) {
      assertThat(pr.getBucketKeys(bucketId)).hasSize(1);
    }
  }

  private ResultCollector<Boolean, Collection<Boolean>> executeFunctionWithInstance(
      final Region region, final BooleanFunction function) {
    Execution<Void, Boolean, Collection<Boolean>> dataSet = FunctionService.onRegion(region);
    ResultCollector<Boolean, Collection<Boolean>> resultCollector = dataSet.execute(function);
    return resultCollector;
  }

  private ResultCollector executeFunctionWithId(final Region region, final String functionId) {
    Execution<Void, Boolean, Collection<Boolean>> dataSet = FunctionService.onRegion(region);
    ResultCollector<Boolean, Collection<Boolean>> resultCollector = dataSet.execute(functionId);
    return resultCollector;
  }

  private void validateResults(final ResultCollector<Boolean, Collection<Boolean>> resultCollector,
      final boolean expectedResult) {
    Collection<Boolean> results = resultCollector.getResult();
    assertThat(results).hasSize(4).containsExactly(expectedResult, expectedResult, expectedResult,
        expectedResult);
  }
}
