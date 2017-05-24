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
package org.apache.geode.cache30;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableErrorCollector;

/**
 * Make sure entry expiration does not happen during gii for bug 35214
 *
 * <p>
 * TRAC #35214: hang during getInitialImage due to entry expiration
 *
 * <p>
 * Entries should not expire during GII
 *
 * @since GemFire 5.0
 */
@Category(DistributedTest.class)
public class EntriesDoNotExpireDuringGIIRegressionTest extends CacheTestCase {

  private static final int ENTRY_COUNT = 100;
  private static final String REGION_NAME = "r1";

  // TODO: value of expirationCount is not validated
  private AtomicInteger expirationCount;
  private AtomicBoolean afterRegionCreateInvoked;
  private VM otherVM;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableErrorCollector errorCollector = new SerializableErrorCollector();

  @Before
  public void setUp() throws Exception {
    this.expirationCount = new AtomicInteger(0);
    this.afterRegionCreateInvoked = new AtomicBoolean(false);
    this.otherVM = Host.getHost(0).getVM(0);
    initOtherVm(this.otherVM);

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    InitialImageOperation.slowImageProcessing = 30;
  }

  @After
  public void tearDown() throws Exception {
    InitialImageOperation.slowImageProcessing = 0;
  }

  /**
   * make sure entries do not expire during a GII
   */
  @Test
  public void entriesShouldNotExpireDuringGII() throws Exception {
    AsyncInvocation updater = updateOtherVm(this.otherVM);

    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setStatisticsEnabled(true);
    factory.setEntryIdleTimeout(new ExpirationAttributes(1, ExpirationAction.INVALIDATE));
    factory.addCacheListener(createCacheListener());

    Region region = createRootRegion(REGION_NAME, factory.create());

    updater.await();

    await().until(() -> region.values().size() == 0);

    assertThat(region.values().size()).isEqualTo(0);
    assertThat(region.keySet().size()).isEqualTo(ENTRY_COUNT);
  }

  private void initOtherVm(final VM otherVM) {
    otherVM.invoke(new CacheSerializableRunnable("init") {

      @Override
      public void run2() throws CacheException {
        getCache();

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);

        Region region = createRootRegion(REGION_NAME, factory.create());

        for (int i = 1; i <= ENTRY_COUNT; i++) {
          region.put("key" + i, "value" + i);
        }
      }
    });
  }

  private AsyncInvocation updateOtherVm(final VM otherVM) {
    return otherVM.invokeAsync(new CacheSerializableRunnable("update") {

      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(REGION_NAME);
        // let the main guys gii get started; we want to do updates during his gii

        // wait for profile of getInitialImage cache to show up
        CacheDistributionAdvisor advisor =
            ((DistributedRegion) region).getCacheDistributionAdvisor();
        int expectedProfiles = 1;
        await().until(
            () -> assertThat(numberProfiles(advisor)).isGreaterThanOrEqualTo(expectedProfiles));

        // start doing updates of the keys to see if we can get deadlocked
        int updateCount = 1;
        do {
          for (int i = 1; i <= ENTRY_COUNT; i++) {
            String key = "key" + i;
            if (region.containsKey(key)) {
              region.destroy(key);
            } else {
              region.put(key, "value" + i + "uc" + updateCount);
            }
          }
        } while (updateCount++ < 20);

        // do one more loop with no destroys
        for (int i = 1; i <= ENTRY_COUNT; i++) {
          String key = "key" + i;
          if (!region.containsKey(key)) {
            region.put(key, "value" + i + "uc" + updateCount);
          }
        }
      }
    });
  }

  private int numberProfiles(final CacheDistributionAdvisor advisor) {
    return advisor.adviseInitialImage(null).getReplicates().size();
  }

  private CacheListener createCacheListener() {
    return new CacheListenerAdapter() {

      @Override
      public void afterRegionCreate(final RegionEvent event) {
        afterRegionCreateInvoked.set(true);
      }

      @Override
      public void afterInvalidate(final EntryEvent event) {
        errorCollector.checkThat("afterRegionCreate should have been seen",
            afterRegionCreateInvoked.get(), is(true));
        errorCollector.checkThat("Region should have been initialized",
            ((LocalRegion) event.getRegion()).isInitialized(), is(true));

        expirationCount.incrementAndGet();

        InitialImageOperation.slowImageProcessing = 0;
      }
    };
  }

  private ConditionFactory await() {
    return Awaitility.await().atMost(2, MINUTES);
  }
}
