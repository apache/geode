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

import static org.apache.geode.modules.util.RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.beans.PropertyChangeEvent;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Context;
import org.apache.catalina.Session;
import org.apache.juli.logging.Log;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionStatistics;

public abstract class DeltaSessionManagerJUnitTest {

  protected DeltaSessionManager manager;
  protected AbstractSessionCache sessionCache;
  protected Cache cache;
  protected Log logger;
  protected Context context;
  protected DeltaSessionStatistics managerStats;
  protected DeltaSessionStatistics cacheStats;
  protected Region<String, HttpSession> operatingRegion;

  public void initTest() {
    sessionCache = mock(AbstractSessionCache.class);
    cache = mock(GemFireCacheImpl.class);
    logger = mock(Log.class);
    context = mock(Context.class);
    managerStats = mock(DeltaSessionStatistics.class);
    cacheStats = mock(DeltaSessionStatistics.class);
    operatingRegion = mock(Region.class);

    doReturn(sessionCache).when(manager).getSessionCache();
    doReturn(logger).when(manager).getLogger();
    doReturn(context).when(manager).getTheContext();
    doReturn(managerStats).when(manager).getStatistics();
    doReturn(cacheStats).when(sessionCache).getStatistics();
    doReturn(operatingRegion).when(sessionCache).getOperatingRegion();
  }

  @Test
  public void getRegionAttributesIdSetsIdFromSessionCacheWhenAttributesIdIsNull() {
    String regionAttributesId = "attributesIdFromSessionCache";

    doReturn(regionAttributesId).when(sessionCache).getDefaultRegionAttributesId();
    String attrId = manager.getRegionAttributesId();

    verify(sessionCache).getDefaultRegionAttributesId();
    assertThat(attrId).isEqualTo(regionAttributesId);
  }

  @Test
  public void getEnableLocalCacheSetsIdFromSessionCacheWhenEnableLocalCacheIsNull() {
    boolean isLocalCacheEnabled = true;

    doReturn(isLocalCacheEnabled).when(sessionCache).getDefaultEnableLocalCache();
    Boolean localCacheEnabledValue = manager.getEnableLocalCache();

    verify(sessionCache).getDefaultEnableLocalCache();
    assertThat(localCacheEnabledValue).isEqualTo(isLocalCacheEnabled);
  }

  @Test
  public void findSessionsReturnsNullWhenIdIsNull() throws IOException {
    Session session = manager.findSession(null);

    assertThat(session).isNull();
  }

  @Test
  public void findSessionsReturnsNullAndLogsMessageWhenContextNameIsNotValid() throws IOException {
    String sessionId = "sessionId";
    String contextName = "contextName";
    String invalidContextName = "invalidContextName";

    DeltaSession expectedSession = mock(DeltaSession.class);
    when(sessionCache.getSession(sessionId)).thenReturn(expectedSession);
    when(expectedSession.getContextName()).thenReturn(invalidContextName);
    when(context.getName()).thenReturn(contextName);

    Session session = manager.findSession(sessionId);

    verify(logger).info(anyString());
    assertThat(session).isNull();
  }

  @Test
  public void findSessionsReturnsNullWhenIdIsNotFound() throws IOException {
    String sessionId = "sessionId";

    when(sessionCache.getSession(sessionId)).thenReturn(null);

    Session session = manager.findSession(sessionId);

    assertThat(session).isNull();
  }

  @Test
  public void findSessionsReturnsProperSessionByIdWhenIdAndContextNameIsValid() throws IOException {
    String sessionId = "sessionId";
    String contextName = "contextName";

    DeltaSession expectedSession = mock(DeltaSession.class);
    when(sessionCache.getSession(sessionId)).thenReturn(expectedSession);
    when(expectedSession.getContextName()).thenReturn(contextName);
    when(context.getName()).thenReturn(contextName);

    Session session = manager.findSession(sessionId);

    assertThat(session).isEqualTo(expectedSession);
  }

  @Test
  public void removeProperlyDestroysSessionFromSessionCacheWhenSessionIsNotExpired() {
    DeltaSession sessionToDestroy = mock(DeltaSession.class);
    String sessionId = "sessionId";

    when(sessionToDestroy.getId()).thenReturn(sessionId);
    when(sessionToDestroy.getExpired()).thenReturn(false);

    manager.remove(sessionToDestroy);

    verify(sessionCache).destroySession(sessionId);
  }

  @Test
  public void removeDoesNotDestroySessionFromSessionCacheWhenSessionIsExpired() {
    DeltaSession sessionToDestroy = mock(DeltaSession.class);
    String sessionId = "sessionId";

    when(sessionToDestroy.getId()).thenReturn(sessionId);
    when(sessionToDestroy.getExpired()).thenReturn(true);

    manager.remove(sessionToDestroy);

    verify(sessionCache, times(0)).destroySession(sessionId);
  }

  @Test
  public void addPutsSessionIntoSessionCacheAndIncrementsStats() {
    DeltaSession sessionToPut = mock(DeltaSession.class);

    manager.add(sessionToPut);

    verify(sessionCache).putSession(sessionToPut);
    verify(cacheStats).incSessionsCreated();
  }

  @Test
  public void listIdsListsAllPresentIds() {
    Set<String> ids = new HashSet<>();
    ids.add("id1");
    ids.add("id2");
    ids.add("id3");

    when(sessionCache.keySet()).thenReturn(ids);

    String listOutput = manager.listSessionIds();

    for (String id : ids) {
      assertThat(listOutput).contains(id);
    }
  }

  @Test
  public void successfulUnloadWithClientServerSessionCachePerformsLocalDestroy()
      throws IOException {
    when(sessionCache.getCache()).thenReturn(cache);
    when(context.getPath()).thenReturn("contextPath");
    when(sessionCache.isClientServer()).thenReturn(true);

    manager.unload();

    verify(operatingRegion).localClear();
  }

  @Test
  public void propertyChangeSetsMaxInactiveIntervalWithCorrectPropertyNameAndValue() {
    String propertyName = "sessionTimeout";
    PropertyChangeEvent event = mock(PropertyChangeEvent.class);
    Context eventContext = mock(Context.class);
    Integer newValue = 1;

    when(event.getSource()).thenReturn(eventContext);
    when(event.getPropertyName()).thenReturn(propertyName);
    when(event.getNewValue()).thenReturn(newValue);

    manager.propertyChange(event);

    verify(manager).setMaxInactiveInterval(newValue * 60);
  }

  @Test
  public void propertyChangeDoesNotSetMaxInactiveIntervalWithIncorrectPropertyName() {
    String propertyName = "wrong name";
    PropertyChangeEvent event = mock(PropertyChangeEvent.class);
    Context eventContext = mock(Context.class);

    when(event.getSource()).thenReturn(eventContext);
    when(event.getPropertyName()).thenReturn(propertyName);

    manager.propertyChange(event);

    verify(manager, times(0)).setMaxInactiveInterval(anyInt());
  }

  @Test
  public void propertyChangeDoesNotSetNewMaxInactiveIntervalWithCorrectPropertyNameAndInvalidPropertyValue() {
    String propertyName = "sessionTimeout";
    PropertyChangeEvent event = mock(PropertyChangeEvent.class);
    Context eventContext = mock(Context.class);
    Integer newValue = -2;
    Integer oldValue = DEFAULT_MAX_INACTIVE_INTERVAL;

    when(event.getSource()).thenReturn(eventContext);
    when(event.getPropertyName()).thenReturn(propertyName);
    when(event.getNewValue()).thenReturn(newValue);
    when(event.getOldValue()).thenReturn(oldValue);

    manager.propertyChange(event);

    verify(manager).setMaxInactiveInterval(oldValue);
  }
}
