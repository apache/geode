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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.http.HttpSession;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@RunWith(JUnitParamsRunner.class)
public class LocalSessionCacheWriterIntegrationTest {
  private static final String SESSION_ID = "id";
  private static final String REGION_NAME = "sessions";
  private static final String BACKING_REGION_NAME = "sessions_local";
  private HttpSession mockSession;
  private HttpSession mockUpdateSession;
  private Region<String, HttpSession> localHttpSessionRegion;
  private Region<String, HttpSession> backingHttpSessionRegion;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Before
  public void setUp() {
    mockSession = new TestDeltaSession(SESSION_ID, "mockSession");
    mockUpdateSession = new TestDeltaSession(SESSION_ID, "mockUpdateSession");

    ExceptionThrower.throwExceptionOnCreate.set(false);
    ExceptionThrower.throwExceptionOnUpdate.set(false);
    ExceptionThrower.throwExceptionOnDestroy.set(false);
  }

  public void parameterizedSetUp(RegionShortcut regionShortcut) {
    backingHttpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .setCacheWriter(new ExceptionThrower())
        .create(REGION_NAME);

    localHttpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU)
        .setCacheWriter(new LocalSessionCacheWriter(backingHttpSessionRegion))
        .create(BACKING_REGION_NAME);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void createAndUpdateShouldBeAppliedToLocalRegionWhenTheySucceededOnTheBackingRegion(
      RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);

    // First time - Create
    localHttpSessionRegion.put(SESSION_ID, mockSession);
    assertThat(backingHttpSessionRegion.size()).isEqualTo(1);
    assertThat(backingHttpSessionRegion.get(SESSION_ID)).isEqualTo(mockSession);
    assertThat(localHttpSessionRegion.size()).isEqualTo(1);
    assertThat(localHttpSessionRegion.get(SESSION_ID)).isEqualTo(mockSession);

    // Second time - Update
    localHttpSessionRegion.put(SESSION_ID, mockUpdateSession);
    assertThat(backingHttpSessionRegion.size()).isEqualTo(1);
    assertThat(backingHttpSessionRegion.get(SESSION_ID)).isEqualTo(mockUpdateSession);
    assertThat(localHttpSessionRegion.size()).isEqualTo(1);
    assertThat(localHttpSessionRegion.get(SESSION_ID)).isEqualTo(mockUpdateSession);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void createAndUpdateShouldNotBeAppliedToLocalRegionWhenTheyFailedOnTheBackingRegion(
      RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);

    // First time - Create
    ExceptionThrower.throwExceptionOnCreate.set(true);
    assertThatThrownBy(() -> localHttpSessionRegion.put(SESSION_ID, mockSession))
        .isInstanceOf(RuntimeException.class).hasMessage("Mock Exception");
    assertThat(backingHttpSessionRegion.isEmpty()).isTrue();
    assertThat(localHttpSessionRegion.isEmpty()).isTrue();

    // Allow Create and Try to Update
    ExceptionThrower.throwExceptionOnCreate.set(false);
    localHttpSessionRegion.put(SESSION_ID, mockSession);
    assertThat(backingHttpSessionRegion.size()).isEqualTo(1);
    ExceptionThrower.throwExceptionOnUpdate.set(true);

    assertThatThrownBy(() -> localHttpSessionRegion.put(SESSION_ID, mockUpdateSession))
        .isInstanceOf(RuntimeException.class).hasMessage("Mock Exception");
    assertThat(backingHttpSessionRegion.size()).isEqualTo(1);
    assertThat(backingHttpSessionRegion.get(SESSION_ID)).isEqualTo(mockSession);
    assertThat(localHttpSessionRegion.size()).isEqualTo(1);
    assertThat(localHttpSessionRegion.get(SESSION_ID)).isEqualTo(mockSession);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void destroyShouldBeAppliedToLocalRegionWhenItSucceededOnTheBackingRegion(
      RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);
    localHttpSessionRegion.put(SESSION_ID, mockSession);
    assertThat(backingHttpSessionRegion.size()).isEqualTo(1);
    assertThat(localHttpSessionRegion.size()).isEqualTo(1);

    localHttpSessionRegion.destroy(SESSION_ID);
    assertThat(backingHttpSessionRegion.isEmpty()).isTrue();
    assertThat(localHttpSessionRegion.isEmpty()).isTrue();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void destroyShouldBeAppliedToLocalRegionEvenWhenItFailedOnTheBackingRegionWithEntryNotFoundException(
      RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);
    localHttpSessionRegion.put(SESSION_ID, mockSession);
    backingHttpSessionRegion.destroy(SESSION_ID);
    assertThat(localHttpSessionRegion.size()).isEqualTo(1);

    localHttpSessionRegion.destroy(SESSION_ID);
    assertThat(backingHttpSessionRegion.isEmpty()).isTrue();
    assertThat(localHttpSessionRegion.isEmpty()).isTrue();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void destroyShouldNotBeAppliedToLocalRegionWhenItFailedOnTheBackingRegion(
      RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);
    localHttpSessionRegion.put(SESSION_ID, mockSession);
    assertThat(backingHttpSessionRegion.size()).isEqualTo(1);
    assertThat(localHttpSessionRegion.size()).isEqualTo(1);

    ExceptionThrower.throwExceptionOnDestroy.set(true);
    assertThatThrownBy(() -> localHttpSessionRegion.destroy(SESSION_ID))
        .isInstanceOf(RuntimeException.class).hasMessage("Mock Exception");
    assertThat(backingHttpSessionRegion.size()).isEqualTo(1);
    assertThat(localHttpSessionRegion.size()).isEqualTo(1);
  }

  static class ExceptionThrower extends CacheWriterAdapter<String, HttpSession> {
    static AtomicBoolean throwExceptionOnCreate = new AtomicBoolean(false);
    static AtomicBoolean throwExceptionOnUpdate = new AtomicBoolean(false);
    static AtomicBoolean throwExceptionOnDestroy = new AtomicBoolean(false);

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      if (throwExceptionOnCreate.get()) {
        throw new RuntimeException("Mock Exception");
      }
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      if (throwExceptionOnUpdate.get()) {
        throw new RuntimeException("Mock Exception");
      }
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      if (throwExceptionOnDestroy.get()) {
        throw new RuntimeException("Mock Exception");
      }
    }
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
