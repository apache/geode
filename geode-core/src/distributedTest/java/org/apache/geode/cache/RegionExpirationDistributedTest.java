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
package org.apache.geode.cache;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;

import java.io.Serializable;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Test Region expiration - both time-to-live and idle timeout.
 *
 * <p>
 * Note: See LocalRegionTest and MultiVMRegionTestCase for more expiration tests.
 *
 * <p>
 * Single-JVM tests were extracted to {@link RegionExpirationIntegrationTest}.
 *
 * @since GemFire 3.0
 */
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class RegionExpirationDistributedTest implements Serializable {

  private static final int TTL_SECONDS = 10;
  private static final String KEY = "key";
  private static final String VALUE = "value";

  private static CacheListener<String, String> spyCacheListener;

  private String regionName;
  private VM withExpirationVM0;
  private VM withoutExpirationVM1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();

    withExpirationVM0 = getVM(0);
    withoutExpirationVM1 = getVM(1);
  }

  @Test
  @Parameters({"LOCAL_DESTROY", "DESTROY", "LOCAL_INVALIDATE", "INVALIDATE"})
  @TestCaseName("{method}({params})")
  public void regionExpiresAfterTtl(final Verification verification) throws Exception {
    // In withoutExpirationVM1, create the region - no ttl
    withoutExpirationVM1.invoke(() -> {
      cacheRule.getCache().createRegionFactory(REPLICATE).create(regionName);
    });

    // In withExpirationVM0, create the region with ttl, and put value
    withExpirationVM0.invoke(() -> {
      spyCacheListener = spy(CacheListener.class);

      RegionFactory<String, String> regionFactory =
          cacheRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.setRegionTimeToLive(
          new ExpirationAttributes(TTL_SECONDS, verification.expirationAction()));
      regionFactory.addCacheListener(spyCacheListener);
      Region<String, String> region = regionFactory.create(regionName);

      region.put(KEY, VALUE);
      assertThat(region.get(KEY)).isEqualTo(VALUE);
      assertThat(region.containsValueForKey(KEY)).isTrue();
    });

    withoutExpirationVM1.invoke(() -> {
      assertThat(cacheRule.getCache().getRegion(regionName).get(KEY)).isEqualTo(VALUE);
    });

    // Wait for expiration to occur
    // In withExpirationVM0, region should be absent (for destroy, localDestroy), or entry invalid
    withExpirationVM0.invoke(() -> {
      verification.verify(spyCacheListener);
      verification.assertThatExpirationOccurredInLocalMember(cacheRule.getCache(), regionName);
    });

    // In withoutExpirationVM1, region should be absent (for destroy), or entry invalid
    // (invalidate).
    withoutExpirationVM1.invoke(() -> {
      verification.assertRegionStateInRemoteMember(cacheRule.getCache(), regionName);
    });
  }

  private static final long TIMEOUT_MILLIS = MINUTES.toMillis(2);

  private static final Consumer<CacheListener<String, String>> VERIFY_AFTER_REGION_DESTROY =
      (spyCacheListener) -> Mockito.verify(spyCacheListener, timeout(TIMEOUT_MILLIS))
          .afterRegionDestroy(any());

  private static final Consumer<CacheListener<String, String>> VERIFY_AFTER_REGION_INVALIDATE =
      (spyCacheListener) -> Mockito.verify(spyCacheListener, timeout(TIMEOUT_MILLIS))
          .afterRegionInvalidate(any());

  private enum Verification {
    LOCAL_DESTROY(VERIFY_AFTER_REGION_DESTROY, ExpirationAction.LOCAL_DESTROY),
    DESTROY(VERIFY_AFTER_REGION_DESTROY, ExpirationAction.DESTROY),
    LOCAL_INVALIDATE(VERIFY_AFTER_REGION_INVALIDATE, ExpirationAction.LOCAL_INVALIDATE),
    INVALIDATE(VERIFY_AFTER_REGION_INVALIDATE, ExpirationAction.INVALIDATE);

    private final Consumer<CacheListener<String, String>> strategy;
    private final ExpirationAction expirationAction;

    Verification(final Consumer<CacheListener<String, String>> strategy,
        final ExpirationAction evictionAction) {
      this.strategy = strategy;
      expirationAction = evictionAction;
    }

    void verify(final CacheListener<String, String> spyCacheListener) {
      strategy.accept(spyCacheListener);
    }

    ExpirationAction expirationAction() {
      return expirationAction;
    }

    void assertThatExpirationOccurredInLocalMember(final Cache cache, final String regionName) {
      if (expirationAction().isInvalidate() || expirationAction().isLocalInvalidate()) {
        // verify region was invalidated
        Region<String, String> region = cache.getRegion(regionName);
        await()
            .untilAsserted(() -> assertThat(region.containsValueForKey(KEY)).isFalse());

      } else {
        // verify region was destroyed (or is in process of being destroyed)
        await().until(() -> isDestroyed(cache.getRegion(regionName)));
      }
    }

    void assertRegionStateInRemoteMember(final Cache cache, final String regionName) {
      if (expirationAction().isInvalidate()) {
        // distributed invalidate region
        Region<String, String> region = cache.getRegion(regionName);
        await().untilAsserted(() -> {
          assertThat(region.containsKey(KEY)).isTrue();
          assertThat(region.containsValueForKey(KEY)).isFalse();
        });

      } else if (expirationAction().isDestroy()) {
        // verify region was destroyed (or is in process of being destroyed)
        await().until(() -> isDestroyed(cache.getRegion(regionName)));

      } else {
        // for LOCAL_DESTROY or LOCAL_INVALIDATE, the value should be present
        Region<String, String> region = cache.getRegion(regionName);
        await().untilAsserted(() -> {
          assertThat(region.containsValueForKey(KEY)).isTrue();
          assertThat(region.get(KEY)).isEqualTo(VALUE);
        });
      }
    }

    boolean isDestroyed(final Region region) {
      return region == null || region.isDestroyed();
    }
  }
}
