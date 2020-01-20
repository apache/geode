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
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.cache.execute.FunctionExecutionOnLonerRegressionTest.UncheckedUtils.cast;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

/**
 * This tests make sure that, in case of LonerDistributedSystem we don't get ClassCast Exception.
 * Just making sure that the function executed on lonerDistributedSystem.
 *
 * <p>
 * Regression test for bug: If invoked from LonerDistributedSystem, FunctionService.onMembers()
 * throws ClassCastException
 */
@Category(FunctionServiceTest.class)
@SuppressWarnings("serial")
public class FunctionExecutionOnLonerRegressionTest {

  private InternalCache cache;

  private Set<String> keysForGet;
  private Set<String> expectedValues;

  @Before
  public void setUp() {
    cache = (InternalCache) new CacheFactory(getDistributedSystemProperties()).create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void precondition_isLonerDistributionManager() {
    DistributionManager distributionManager = cache.getDistributionManager();

    assertThat(distributionManager).isInstanceOf(LonerDistributionManager.class);
  }

  @Test
  public void executeFunctionOnLonerWithPartitionedRegionShouldNotThrowClassCastException() {
    Region<String, String> region = cache
        .<String, String>createRegionFactory(PARTITION)
        .create("region");

    populateRegion(region);

    ResultCollector<Collection<String>, Collection<String>> resultCollector = FunctionServiceCast
        .<Void, Collection<String>, Collection<String>>onRegion(region)
        .withFilter(keysForGet)
        .execute(new TestFunction(DataSetSupplier.PARTITIONED));

    assertThat(resultCollector.getResult())
        .containsExactlyInAnyOrder(expectedValues.toArray(new String[0]));
  }

  @Test
  public void executeFunctionOnLonerWithReplicateRegionShouldNotThrowClassCastException() {
    Region<String, String> region = cache
        .<String, String>createRegionFactory(REPLICATE)
        .create("region");

    populateRegion(region);

    ResultCollector<Collection<String>, Collection<String>> resultCollector = FunctionServiceCast
        .<Void, Collection<String>, Collection<String>>onRegion(region)
        .withFilter(keysForGet)
        .execute(new TestFunction(DataSetSupplier.REPLICATE));

    assertThat(resultCollector.getResult())
        .containsExactlyInAnyOrder(expectedValues.toArray(new String[0]));
  }

  private Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.test.junit.rules.**;org.apache.geode.internal.cache.execute.**;org.apache.geode.internal.cache.functions.**;org.apache.geode.test.dunit.**");
    return config;
  }

  private void populateRegion(Region<String, String> region) {
    keysForGet = new HashSet<>();
    expectedValues = new HashSet<>();

    for (int i = 0; i < 20; i++) {
      String key = "KEY_" + i;
      String value = "VALUE_" + i;

      region.put(key, value);

      if (i == 4 || i == 7 || i == 9) {
        keysForGet.add(key);
        expectedValues.add(value);
      }
    }
  }

  private enum DataSetSupplier {
    PARTITIONED(PartitionRegionHelper::getLocalDataForContext),
    REPLICATE(RegionFunctionContext::getDataSet);

    private final Function<RegionFunctionContext, Region<String, String>> dataSet;

    DataSetSupplier(Function<RegionFunctionContext, Region<String, String>> dataSet) {

      this.dataSet = dataSet;
    }

    Region<String, String> dataSet(RegionFunctionContext context) {
      return dataSet.apply(context);
    }
  }

  private static class TestFunction implements org.apache.geode.cache.execute.Function<String> {

    private final DataSetSupplier dataSetSupplier;

    private TestFunction(DataSetSupplier dataSetSupplier) {
      this.dataSetSupplier = dataSetSupplier;
    }

    @Override
    public void execute(FunctionContext<String> context) {
      RegionFunctionContext regionFunctionContext = (RegionFunctionContext) context;
      Set<String> keys = cast(regionFunctionContext.getFilter());
      String lastKey = keys.iterator().next();
      keys.remove(lastKey);

      Region<String, String> region = dataSetSupplier.dataSet(regionFunctionContext);

      for (String key : keys) {
        context.getResultSender().sendResult(region.get(key));
      }

      context.getResultSender().lastResult(region.get(lastKey));
    }

    @Override
    public String getId() {
      return getClass().getName();
    }
  }

  @SuppressWarnings({"unchecked", "WeakerAccess"})
  private static class FunctionServiceCast {

    static <IN, OUT, AGG> Execution<IN, OUT, AGG> onRegion(Region<?, ?> region) {
      return FunctionService.onRegion(region);
    }
  }

  @SuppressWarnings({"unchecked", "unused"})
  static class UncheckedUtils {

    static <T> T cast(Object object) {
      return (T) object;
    }
  }
}
