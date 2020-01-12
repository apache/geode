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
package org.apache.geode.modules.session.catalina;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.junit.Rule;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class AbstractSessionValveIntegrationTest {
  private static final String REGION_NAME = "geode_modules_sessions";
  static final String TEST_CONTEXT = "testContext";
  static final String TEST_JVM_ROUTE = "testJvmRoute";
  static final String TEST_SESSION_ID = UUID.randomUUID().toString() + "." + TEST_JVM_ROUTE;

  TestDeltaSession deltaSession;
  DeltaSessionManager deltaSessionManager;

  Region<String, HttpSession> httpSessionRegion;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  private void mockDeltaSessionManager() {
    deltaSessionManager = mock(DeltaSessionManager.class);

    when(deltaSessionManager.getLogger()).thenReturn(mock(Log.class));
    SessionCache mockSessionCache = mock(AbstractSessionCache.class);
    doCallRealMethod().when(mockSessionCache).getSession(any());
    when(mockSessionCache.getOperatingRegion()).thenReturn(httpSessionRegion);
    when(deltaSessionManager.getSessionCache()).thenReturn(mockSessionCache);
  }

  protected void parameterizedSetUp(RegionShortcut regionShortcut) {
    httpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .create(REGION_NAME);

    mockDeltaSessionManager();
    deltaSession = spy(new TestDeltaSession(deltaSessionManager, TEST_SESSION_ID));

    httpSessionRegion.put(deltaSession.getId(), deltaSession);
  }

  static class TestDeltaSession extends DeltaSession {

    TestDeltaSession(Manager manager, String sessionId) {
      super(manager);
      this.id = sessionId;
    }

    @Override
    public String getContextName() {
      return TEST_CONTEXT;
    }

    @Override
    protected boolean isValidInternal() {
      return true;
    }
  }

  static class TestValve extends ValveBase {
    final boolean throwException;
    final AtomicInteger invocations = new AtomicInteger(0);
    final AtomicInteger exceptionsThrown = new AtomicInteger(0);

    TestValve(boolean throwException) {
      this.throwException = throwException;
    }

    @Override
    public void invoke(Request request, Response response) {
      invocations.incrementAndGet();

      if (throwException) {
        exceptionsThrown.incrementAndGet();
        throw new RuntimeException("Mock Exception");
      }
    }
  }
}
