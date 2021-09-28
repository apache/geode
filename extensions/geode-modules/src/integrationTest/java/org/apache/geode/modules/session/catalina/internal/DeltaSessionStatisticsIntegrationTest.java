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
package org.apache.geode.modules.session.catalina.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpSession;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.modules.session.catalina.AbstractSessionCache;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.modules.session.catalina.SessionCache;
import org.apache.geode.modules.session.catalina.callback.SessionExpirationCacheListener;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class DeltaSessionStatisticsIntegrationTest extends AbstractDeltaSessionIntegrationTest {
  private DeltaSessionStatistics statistics;

  @Before
  public void setUp() {
    statistics = new DeltaSessionStatistics(server.getCache().getDistributedSystem(), "testApp");
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void sessionsCreatedStatisticsShouldBeIncrementedWhenSessionIsAdded(
      RegionShortcut regionShortcut) {
    httpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .create(REGION_NAME);

    SessionCache mockSessionCache = mock(AbstractSessionCache.class);
    doCallRealMethod().when(mockSessionCache).putSession(any());
    when(mockSessionCache.getStatistics()).thenReturn(statistics);
    when(mockSessionCache.getOperatingRegion()).thenReturn(httpSessionRegion);

    mockDeltaSessionManager();
    doCallRealMethod().when(deltaSessionManager).add(any());
    when(deltaSessionManager.getSessionCache()).thenReturn(mockSessionCache);

    deltaSessionManager.add(new TestDeltaSession(deltaSessionManager, TEST_SESSION_ID));
    await().untilAsserted(() -> assertThat(statistics.getSessionsCreated()).isEqualTo(1));
    assertThat(httpSessionRegion.isEmpty()).isFalse();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void sessionsExpiredStatisticsShouldBeIncrementedWhenSessionExpires(
      RegionShortcut regionShortcut) {
    httpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .setCustomEntryTimeToLive(new TestCustomExpiry())
        .addCacheListener(new SessionExpirationCacheListener())
        .create(REGION_NAME);
    mockDeltaSessionManager();
    when(deltaSessionManager.getStatistics()).thenReturn(statistics);
    DeltaSession spyDeltasSession = spy(new TestDeltaSession(deltaSessionManager, TEST_SESSION_ID));
    httpSessionRegion.put(TEST_SESSION_ID, spyDeltasSession);

    await().untilAsserted(() -> assertThat(statistics.getSessionsExpired()).isEqualTo(1));
    assertThat(httpSessionRegion.isEmpty()).isTrue();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void sessionsInvalidatedStatisticsShouldBeIncrementedWhenSessionInInvalidated(
      RegionShortcut regionShortcut) {
    httpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .create(REGION_NAME);
    mockDeltaSessionManager();
    when(deltaSessionManager.getStatistics()).thenReturn(statistics);
    TestDeltaSession deltaSession = new TestDeltaSession(deltaSessionManager, TEST_SESSION_ID);
    httpSessionRegion.put(deltaSession.getId(), deltaSession);

    deltaSession.invalidate();
    await().untilAsserted(() -> assertThat(statistics.getSessionsInvalidated()).isEqualTo(1));
    assertThat(httpSessionRegion.isEmpty()).isTrue();
  }

  static class TestCustomExpiry implements CustomExpiry<String, HttpSession> {
    @Override
    public ExpirationAttributes getExpiry(Region.Entry<String, HttpSession> entry) {
      return new ExpirationAttributes(1, ExpirationAction.DESTROY);
    }
  }
}
