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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PROXY;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RegionConcurrentOperationDUnitTest implements Serializable {

  private static DUnitBlackboard blackboard;

  Object key = "KEY";
  String[] value = new String[] {"VALUE"};

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @After
  public void tearDown() {
    blackboard.initBlackboard();
  }

  @Test
  public void getOnProxyRegionFromMultipleThreadsReturnsDifferentObjects() throws Exception {
    VM member1 = getVM(0);
    String regionName = getClass().getSimpleName();

    cacheRule.createCache();
    cacheRule.getCache().createRegionFactory(REPLICATE_PROXY).create(regionName);

    member1.invoke(() -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(REPLICATE)
          .setCacheLoader(new TestCacheLoader()).create(regionName);
    });

    Future get1 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      return region.get(key);
    });

    Future get2 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();
    assertThat(get1value).isNotSameAs(get2value);
  }

  @Test
  public void getOnPreLoadedRegionFromMultipleThreadsReturnSameObject() throws Exception {
    VM member1 = getVM(0);
    String regionName = getClass().getSimpleName();

    cacheRule.createCache();
    cacheRule.getCache().createRegionFactory().setDataPolicy(DataPolicy.PRELOADED)
        .setScope(Scope.DISTRIBUTED_ACK).create(regionName);

    member1.invoke(() -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(REPLICATE)
          .setCacheLoader(new TestCacheLoader()).create(regionName);
    });
    assertThat(cacheRule.getCache().getRegion(regionName).size()).isEqualTo(0);

    Future get1 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      return region.get(key);
    });

    Future get2 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();

    assertThat(get1value).isSameAs(get2value);
    assertThat(cacheRule.getCache().getRegion(regionName).size()).isEqualTo(1);
  }

  private class TestCacheLoader implements CacheLoader, Serializable {

    @Override
    public synchronized Object load(LoaderHelper helper) {
      getBlackboard().signalGate("Loader");
      return value;
    }

    @Override
    public void close() {}
  }
}
