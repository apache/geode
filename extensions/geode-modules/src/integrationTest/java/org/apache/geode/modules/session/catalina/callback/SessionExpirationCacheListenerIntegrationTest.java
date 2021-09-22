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
package org.apache.geode.modules.session.catalina.callback;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpSession;

import junitparams.Parameters;
import org.apache.juli.logging.Log;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.modules.session.catalina.DeltaSessionManager;
import org.apache.geode.modules.session.catalina.SessionCache;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class SessionExpirationCacheListenerIntegrationTest {
  private static final String KEY = "key1";
  private static final String REGION_NAME = "sessions";
  private TestDeltaSession deltaSession;
  private Region<String, HttpSession> httpSessionRegion;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Before
  public void setUp() {
    deltaSession = new TestDeltaSession(KEY);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void listenerShouldInvokeProcessExpiredMethodWhenSessionIsDestroyedDueToGeodeExpiration(
      RegionShortcut regionShortcut) {
    httpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .setCustomEntryTimeToLive(new TestCustomExpiry())
        .addCacheListener(new SessionExpirationCacheListener())
        .create(REGION_NAME);
    httpSessionRegion.put(deltaSession.getId(), deltaSession);

    await().untilAsserted(() -> assertThat(deltaSession.processExpiredCalls.get()).isEqualTo(1));
    assertThat(httpSessionRegion.isEmpty()).isTrue();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void listenerShouldInvokeProcessExpiredMethodWhenSessionIsDestroyedDueToGeodeExpirationWithoutClientProxy(
      RegionShortcut regionShortcut) {
    httpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .addCacheListener(new SessionExpirationCacheListener())
        .create(REGION_NAME);

    DeltaSessionManager manager = mock(DeltaSessionManager.class);
    when(manager.getLogger()).thenReturn(mock(Log.class));
    when(manager.getRegionName()).thenReturn(REGION_NAME);
    when(manager.getSessionCache()).thenReturn(mock(SessionCache.class));
    when(manager.getSessionCache().getOperatingRegion()).thenReturn(httpSessionRegion);
    deltaSession.setOwner(manager);
    httpSessionRegion.put(deltaSession.getId(), deltaSession);

    deltaSession.expire(true);
    await().untilAsserted(() -> assertThat(deltaSession.processExpiredCalls.get()).isEqualTo(1));
    assertThat(httpSessionRegion.isEmpty()).isTrue();
  }

  static class TestCustomExpiry implements CustomExpiry<String, HttpSession> {
    @Override
    public ExpirationAttributes getExpiry(Region.Entry<String, HttpSession> entry) {
      return new ExpirationAttributes(1, ExpirationAction.DESTROY);
    }
  }

  static class TestDeltaSession extends DeltaSession {
    private final String id;
    final AtomicInteger processExpiredCalls = new AtomicInteger(0);

    TestDeltaSession(String id) {
      this.id = id;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public void processExpired() {
      processExpiredCalls.incrementAndGet();
    }

    @Override
    public Enumeration getAttributeNames() {
      return Collections.emptyEnumeration();
    }
  }
}
