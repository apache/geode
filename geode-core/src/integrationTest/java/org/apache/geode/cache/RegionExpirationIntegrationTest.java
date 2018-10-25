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

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.ExpirationAction.DESTROY;
import static org.apache.geode.cache.ExpirationAction.INVALIDATE;
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InOrder;

import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RegionExpirationIntegrationTest {

  @Parameterized.Parameter(0)
  public DataPolicy dataPolicy;

  @Parameterized.Parameters(name = "{0}")
  public static Object[] data() {
    return new Object[] {DataPolicy.NORMAL, DataPolicy.EMPTY};
  }

  private Cache cache;
  private String regionName;
  private CacheListener<String, String> spyCacheListener;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    regionName = testName.getMethodName() + "_Region";
    spyCacheListener = mock(CacheListener.class);

    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
  }

  @Test
  public void increaseRegionTtl() throws Exception {
    int firstTtlSeconds = 3;
    int secondTtlSeconds = 8;
    long startNanos = nanoTime();

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setRegionTimeToLive(new ExpirationAttributes(firstTtlSeconds, DESTROY));
    regionFactory.setDataPolicy(dataPolicy);
    Region<String, String> region = regionFactory.create(regionName);

    region.getAttributesMutator()
        .setRegionTimeToLive(new ExpirationAttributes(secondTtlSeconds, DESTROY));

    await().until(() -> region.isDestroyed());
    assertThat(NANOSECONDS.toSeconds(nanoTime() - startNanos))
        .isGreaterThanOrEqualTo(secondTtlSeconds);
  }

  @Test
  public void decreaseRegionTtl() throws Exception {
    int firstTtlSeconds = 5;
    int secondTtlSeconds = 1;
    long startNanos = nanoTime();

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setRegionTimeToLive(new ExpirationAttributes(firstTtlSeconds, DESTROY));
    regionFactory.setDataPolicy(dataPolicy);
    Region<String, String> region = regionFactory.create(regionName);

    region.getAttributesMutator()
        .setRegionTimeToLive(new ExpirationAttributes(secondTtlSeconds, DESTROY));

    await().untilAsserted(() -> assertThat(region.isDestroyed()).isTrue());
    assertThat(NANOSECONDS.toSeconds(nanoTime() - startNanos)).isLessThan(firstTtlSeconds);
  }

  @Test
  public void regionTtlWithIdleMock() throws Exception {
    int ttlSeconds = 5;
    int idleSeconds = 1;

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setRegionTimeToLive(new ExpirationAttributes(ttlSeconds, DESTROY));
    regionFactory.setRegionIdleTimeout(new ExpirationAttributes(idleSeconds, INVALIDATE));
    regionFactory.setDataPolicy(dataPolicy);
    regionFactory.addCacheListener(spyCacheListener);
    Region<String, String> region = regionFactory.create(regionName);

    region.create("key", "val");

    // await Region INVALIDATE

    verify(spyCacheListener, timeout(SECONDS.toMillis(10))).afterRegionInvalidate(any());
    assertThat(region.get("key")).isNull();

    // await Region DESTROY

    verify(spyCacheListener, timeout(SECONDS.toMillis(30))).afterRegionDestroy(any());
    assertTrue(region.isDestroyed());

    InOrder inOrder = inOrder(spyCacheListener);
    inOrder.verify(spyCacheListener).afterRegionInvalidate(any());
    inOrder.verify(spyCacheListener).afterRegionDestroy(any());
  }
}
