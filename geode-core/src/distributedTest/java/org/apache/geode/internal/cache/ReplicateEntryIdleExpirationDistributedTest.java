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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.ExpirationAction.DESTROY;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class ReplicateEntryIdleExpirationDistributedTest implements Serializable {

  private static final AtomicBoolean KEEP_READING = new AtomicBoolean(true);

  private static final String KEY = "KEY";
  private static final String VALUE = "VALUE";

  private String regionName;

  private VM member1;
  private VM member2;
  private VM member3;
  private VM member4;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    member1 = getVM(0);
    member2 = getVM(1);
    member3 = getVM(2);
    member4 = getVM(3);

    regionName = getClass().getSimpleName();

    VM[] vms = new VM[] {member1, member2, member3};
    for (VM vm : vms) {
      vm.invoke(() -> {
        KEEP_READING.set(true);
        cacheRule.createCache();
        ExpiryTask.suspendExpiration();
        createRegion();
      });
    }
  }

  @After
  public void tearDown() throws Exception {
    VM[] vms = new VM[] {member1, member2, member3};
    for (VM vm : vms) {
      vm.invoke(() -> {
        KEEP_READING.set(false);
        ExpiryTask.permitExpiration();
      });
    }
  }

  @Test
  public void readsInOtherMemberShouldPreventExpiration() throws Exception {
    AsyncInvocation<?> memberReading = member3.invokeAsync(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put(KEY, VALUE);
      while (KEEP_READING.get()) {
        region.get(KEY);
        Thread.sleep(10);
      }
    });

    member2.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      await().until(() -> region.containsKey(KEY));
      assertThat(region.containsKey(KEY)).isTrue();
    });

    member1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      await().until(() -> region.containsKey(KEY));
      assertThat(region.containsKey(KEY)).isTrue();

      ExpiryTask.permitExpiration();
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (stopwatch.elapsed(SECONDS) <= 5 && region.containsKey(KEY)) {
        Thread.sleep(10);
      }
      assertThat(region.containsKey(KEY)).isTrue();
    });

    member3.invoke(() -> KEEP_READING.set(false));

    memberReading.await();
  }

  @Test
  public void readsInNormalMemberShouldPreventExpiration() throws Exception {
    member4.invoke(() -> {
      KEEP_READING.set(true);
      cacheRule.createCache();
      ExpiryTask.suspendExpiration();

      RegionFactory<String, String> factory = cacheRule.getCache().createRegionFactory();
      factory.setDataPolicy(DataPolicy.NORMAL).setScope(Scope.DISTRIBUTED_ACK);
      factory.setEntryIdleTimeout(new ExpirationAttributes(1, DESTROY));
      factory.create(regionName);
    });
    AsyncInvocation<?> memberReading = member4.invokeAsync(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put(KEY, VALUE);
      while (KEEP_READING.get()) {
        region.get(KEY);
        Thread.sleep(10);
      }
    });

    member2.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      await().until(() -> region.containsKey(KEY));
      assertThat(region.containsKey(KEY)).isTrue();
    });

    member1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      await().until(() -> region.containsKey(KEY));
      assertThat(region.containsKey(KEY)).isTrue();

      ExpiryTask.permitExpiration();
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (stopwatch.elapsed(SECONDS) <= 5 && region.containsKey(KEY)) {
        Thread.sleep(10);
      }
      assertThat(region.containsKey(KEY)).isTrue();
    });

    member4.invoke(() -> KEEP_READING.set(false));

    memberReading.await();
  }

  @Test
  public void readsInOtherMemberShouldPreventExpirationWhenEvictionEnabled() throws Exception {
    String evictionRegionName = "evictionRegion";
    member1.invoke(() -> createEvictionRegion(evictionRegionName));
    member2.invoke(() -> createEvictionRegion(evictionRegionName));
    member3.invoke(() -> createEvictionRegion(evictionRegionName));
    AsyncInvocation<?> memberReading = member3.invokeAsync(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(evictionRegionName);
      region.put(KEY, VALUE);
      while (KEEP_READING.get()) {
        region.get(KEY);
        Thread.sleep(10);
      }
    });

    member2.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(evictionRegionName);
      await().until(() -> region.containsKey(KEY));
      assertThat(region.containsKey(KEY)).isTrue();
    });

    member1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(evictionRegionName);
      await().until(() -> region.containsKey(KEY));
      assertThat(region.containsKey(KEY)).isTrue();

      ExpiryTask.permitExpiration();
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (stopwatch.elapsed(SECONDS) <= 5 && region.containsKey(KEY)) {
        Thread.sleep(10);
      }
      assertThat(region.containsKey(KEY)).isTrue();
    });

    member3.invoke(() -> KEEP_READING.set(false));

    memberReading.await();
  }

  private void createRegion() {
    RegionFactory<String, String> factory = cacheRule.getCache().createRegionFactory(REPLICATE);
    factory.setEntryIdleTimeout(new ExpirationAttributes(1, DESTROY));
    factory.create(regionName);
  }

  private void createEvictionRegion(String regionName) {
    RegionFactory<String, String> factory = cacheRule.getCache().createRegionFactory(REPLICATE);
    factory.setEntryIdleTimeout(new ExpirationAttributes(1, DESTROY));
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(100));
    factory.create(regionName);
  }
}
