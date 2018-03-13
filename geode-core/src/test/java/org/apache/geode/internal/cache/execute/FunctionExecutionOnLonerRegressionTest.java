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
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * This tests make sure that, in case of LonerDistributedSystem we don't get ClassCast Exception.
 * Just making sure that the function executed on lonerDistributedSystem
 *
 * <p>
 * TRAC #41118: If invoked from LonerDistributedSystem, FunctionService.onMembers() throws
 * ClassCastException
 *
 * <p>
 * Extracted from {@link PRFunctionExecutionDUnitTest}.
 */
@Category({IntegrationTest.class, FunctionServiceTest.class})
public class FunctionExecutionOnLonerRegressionTest {

  private Region<String, String> region;
  private Set<String> keysForGet;
  private Set<String> expectedValues;

  @Before
  public void setUp() {
    keysForGet = new HashSet<>();
    expectedValues = new HashSet<>();

    Properties config = getDistributedSystemProperties();
    InternalDistributedSystem ds = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionManager dm = ds.getDistributionManager();
    assertThat(dm).isInstanceOf(LonerDistributionManager.class);

    Cache cache = CacheFactory.create(ds);

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
    region = regionFactory.create("region");

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

  private Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.put(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.test.junit.rules.**;org.apache.geode.internal.cache.execute.**;org.apache.geode.internal.cache.functions.**;org.apache.geode.test.dunit.**");
    return config;
  }

  @Test
  public void executeFunctionOnLonerShouldNotThrowClassCastException() throws Exception {
    Execution<Void, Collection<String>, Collection<String>> execution =
        FunctionService.onRegion(region).withFilter(keysForGet);
    ResultCollector<Collection<String>, Collection<String>> resultCollector =
        execution.execute(new TestFunction());
    assertThat(resultCollector.getResult())
        .containsExactlyInAnyOrder(expectedValues.toArray(new String[0]));
  }

  private static class TestFunction implements Function {

    @Override
    public void execute(final FunctionContext context) {
      RegionFunctionContext regionFunctionContext = (RegionFunctionContext) context;
      Set keys = regionFunctionContext.getFilter();
      Set keysTillSecondLast = new HashSet();
      int setSize = keys.size();
      Iterator keysIterator = keys.iterator();
      for (int i = 0; i < (setSize - 1); i++) {
        keysTillSecondLast.add(keysIterator.next());
      }
      for (Object k : keysTillSecondLast) {
        context.getResultSender()
            .sendResult(PartitionRegionHelper.getLocalDataForContext(regionFunctionContext).get(k));
      }
      Object lastResult = keysIterator.next();
      context.getResultSender().lastResult(
          PartitionRegionHelper.getLocalDataForContext(regionFunctionContext).get(lastResult));
    }

    @Override
    public String getId() {
      return getClass().getName();
    }
  }
}
