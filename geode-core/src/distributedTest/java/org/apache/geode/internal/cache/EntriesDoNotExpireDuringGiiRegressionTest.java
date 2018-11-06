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

import static org.apache.geode.cache.ExpirationAction.INVALIDATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableErrorCollector;

/**
 * Entries should not expire during GII
 *
 * <p>
 * TRAC #35214: hang during getInitialImage due to entry expiration
 *
 * @since GemFire 5.0
 */
@SuppressWarnings("serial")
public class EntriesDoNotExpireDuringGiiRegressionTest implements Serializable {

  private static final int ENTRY_COUNT = 100;
  private static final String REGION_NAME = "r1";

  private final AtomicInteger expirationCount = new AtomicInteger(0);
  private final AtomicBoolean afterRegionCreateInvoked = new AtomicBoolean(false);

  private VM otherVM;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableErrorCollector errorCollector = new SerializableErrorCollector();

  @Before
  public void setUp() throws Exception {
    otherVM = getVM(0);

    otherVM.invoke(() -> {
      RegionFactory<String, String> regionFactory =
          cacheRule.getOrCreateCache().createRegionFactory(REPLICATE);
      Region<String, String> region = regionFactory.create(REGION_NAME);

      for (int i = 1; i <= ENTRY_COUNT; i++) {
        region.put("key" + i, "value" + i);
      }
    });

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    InitialImageOperation.slowImageProcessing = 30;
  }

  @After
  public void tearDown() throws Exception {
    InitialImageOperation.slowImageProcessing = 0;
  }

  @Test
  public void entriesShouldNotExpireDuringGII() throws Exception {
    AsyncInvocation<Void> doRegionOps = otherVM.invokeAsync(() -> doRegionOps());

    RegionFactory<String, String> regionFactory =
        cacheRule.getOrCreateCache().createRegionFactory(REPLICATE);
    regionFactory.addCacheListener(new SlowGiiCacheListener());
    regionFactory.setEntryIdleTimeout(new ExpirationAttributes(1, INVALIDATE));
    regionFactory.setStatisticsEnabled(true);

    Region<String, String> region = regionFactory.create(REGION_NAME);

    doRegionOps.await();

    await().until(() -> region.values().isEmpty());

    assertThat(region.values()).hasSize(0);
    assertThat(region.keySet()).hasSize(ENTRY_COUNT);
    assertThat(expirationCount.get()).isGreaterThan(ENTRY_COUNT);
  }

  private void doRegionOps() {
    Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
    // let the main region's gii get started; we want to do updates during its gii

    // wait for profile of getInitialImage cache to show up
    CacheDistributionAdvisor advisor = ((DistributedRegion) region).getCacheDistributionAdvisor();
    int expectedProfiles = 1;
    await()
        .untilAsserted(
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

  private int numberProfiles(final CacheDistributionAdvisor advisor) {
    return advisor.adviseInitialImage(null).getReplicates().size();
  }

  private class SlowGiiCacheListener extends CacheListenerAdapter<String, String> {

    @Override
    public void afterRegionCreate(final RegionEvent<String, String> event) {
      afterRegionCreateInvoked.set(true);
    }

    @Override
    public void afterInvalidate(final EntryEvent<String, String> event) {
      errorCollector.checkThat("afterRegionCreate should have been seen",
          afterRegionCreateInvoked.get(), is(true));
      errorCollector.checkThat("Region should have been initialized",
          ((LocalRegion) event.getRegion()).isInitialized(), is(true));

      expirationCount.incrementAndGet();

      InitialImageOperation.slowImageProcessing = 0;
    }
  }
}
