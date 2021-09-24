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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Enumeration;

import javax.servlet.http.HttpSession;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class LocalSessionCacheLoaderIntegrationTest {
  private static final String SESSION_ID = "id";
  private static final String REGION_NAME = "sessions";
  private static final String BACKING_REGION_NAME = "sessions_local";
  private HttpSession mockSession;
  private Region<String, HttpSession> localHttpSessionRegion;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Before
  public void setUp() {
    mockSession = new TestDeltaSession(SESSION_ID, "mockSession");
  }

  public void parameterizedSetUp(RegionShortcut regionShortcut) {
    Region<String, HttpSession> backingHttpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .create(REGION_NAME);
    backingHttpSessionRegion.put(SESSION_ID, mockSession);

    localHttpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU)
        .setCacheLoader(new LocalSessionCacheLoader(backingHttpSessionRegion))
        .create(BACKING_REGION_NAME);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void getShouldReturnEntryFromTheBackingRegion(RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);

    HttpSession session = localHttpSessionRegion.get(SESSION_ID);
    assertThat(session).isEqualTo(mockSession);
  }

  static class TestDeltaSession extends DeltaSession {
    private final String id;
    private final String value;

    TestDeltaSession(String id, String value) {
      this.id = id;
      this.value = value;
    }

    public String getId() {
      return id;
    }

    String getValue() {
      return value;
    }

    @Override
    public Enumeration getAttributeNames() {
      return Collections.emptyEnumeration();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestDeltaSession that = (TestDeltaSession) o;

      if (!getId().equals(that.getId())) {
        return false;
      }
      return getValue().equals(that.getValue());
    }

    @Override
    public int hashCode() {
      int result = getId().hashCode();
      result = 31 * result + getValue().hashCode();
      return result;
    }
  }
}
