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

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PROXY;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RegionConcurrentOperationDUnitTest implements Serializable {

  Object key = "KEY";
  String value = "VALUE";

  private DistributedBlackboard getBlackboard() {
    return blackboard;
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

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

    Future<Object> get1 = executorServiceRule.submit(() -> {
      Region<Object, Object> region = cacheRule.getCache().getRegion(regionName);
      return region.get(key);
    });

    Future<Object> get2 = executorServiceRule.submit(() -> {
      Region<Object, Object> region = cacheRule.getCache().getRegion(regionName);
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

    Future<Object> get1 = executorServiceRule.submit(() -> {
      Region<Object, Object> region = cacheRule.getCache().getRegion(regionName);
      return region.get(key);
    });

    Future<Object> get2 = executorServiceRule.submit(() -> {
      Region<Object, Object> region = cacheRule.getCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();

    assertThat(get1value).isSameAs(get2value);
    assertThat(cacheRule.getCache().getRegion(regionName).size()).isEqualTo(1);
  }

  @Test
  public void getOnPartitionedRegionFromMultipleThreadsReturnsDifferentPdxInstances()
      throws Exception {
    String regionName = getClass().getSimpleName();
    CacheFactory cacheFactory = new CacheFactory();
    cacheFactory.setPdxReadSerialized(true);
    cacheRule.createCache(cacheFactory);
    InternalCache cache = cacheRule.getCache();
    cache.setCopyOnRead(true);
    Region<Object, Object> region = cache.createRegionFactory(PARTITION)
        .create(regionName);

    // Keep doing this concurrency test for 30 seconds.
    long endTime = Duration.ofSeconds(30).toMillis() + System.currentTimeMillis();

    while (System.currentTimeMillis() < endTime) {
      Callable<Object> getValue = () -> {
        while (true) {
          Object value = region.get(key);
          if (value != null) {
            return value;
          }
        }
      };

      // In this test, two threads are doing gets. One thread puts the value
      // We expect that the threads will *always* get different PdxInstance values
      Future<Object> get1 = executorServiceRule.submit(getValue);
      Future<Object> get2 = executorServiceRule.submit(getValue);
      Future<Object> put = executorServiceRule.submit(() -> region.put(key, new TestValue()));

      Object get1value = get1.get();
      Object get2value = get2.get();
      put.get();

      // Assert the values returned are different objects.
      // PdxInstances are not threadsafe and should not be shared between threads.
      assertThat(get1value).isNotSameAs(get2value);
      region.destroy(key);

    }
  }

  private class TestCacheLoader implements CacheLoader<Object, Object>, Serializable {

    @Override
    public synchronized Object load(LoaderHelper helper) {
      getBlackboard().signalGate("Loader");
      return value;
    }

    @Override
    public void close() {}
  }

  private static class TestValue implements PdxSerializable {
    int field1 = 5;
    String field2 = "field";

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("field1", field1);
      writer.writeString("field2", field2);

    }

    @Override
    public void fromData(PdxReader reader) {
      reader.readInt("field1");
      reader.readString("field2");
    }
  }
}
