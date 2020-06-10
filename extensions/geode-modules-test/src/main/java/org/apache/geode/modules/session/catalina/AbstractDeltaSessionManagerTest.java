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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.beans.PropertyChangeEvent;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Context;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.LinkedResultSet;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionStatistics;

public abstract class AbstractDeltaSessionManagerTest {

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
    final String regionAttributesId = "attributesIdFromSessionCache";

    doReturn(regionAttributesId).when(sessionCache).getDefaultRegionAttributesId();
    final String attrId = manager.getRegionAttributesId();

    verify(sessionCache).getDefaultRegionAttributesId();
    assertThat(attrId).isEqualTo(regionAttributesId);
  }

  @Test
  public void getEnableLocalCacheSetsIdFromSessionCacheWhenEnableLocalCacheIsNull() {
    final boolean isLocalCacheEnabled = true;

    doReturn(isLocalCacheEnabled).when(sessionCache).getDefaultEnableLocalCache();
    final Boolean localCacheEnabledValue = manager.getEnableLocalCache();

    verify(sessionCache).getDefaultEnableLocalCache();
    assertThat(localCacheEnabledValue).isEqualTo(isLocalCacheEnabled);
  }

  @Test
  public void findSessionsReturnsNullWhenIdIsNull() throws IOException {
    final Session session = manager.findSession(null);

    assertThat(session).isNull();
  }

  @Test
  public void findSessionsReturnsNullAndLogsMessageWhenContextNameIsNotValid() throws IOException {
    final String sessionId = "sessionId";
    final String contextName = "contextName";
    final String invalidContextName = "invalidContextName";

    final DeltaSession expectedSession = mock(DeltaSession.class);
    when(sessionCache.getSession(sessionId)).thenReturn(expectedSession);
    when(expectedSession.getContextName()).thenReturn(invalidContextName);
    when(context.getName()).thenReturn(contextName);

    final Session session = manager.findSession(sessionId);

    verify(logger).info(anyString());
    assertThat(session).isNull();
  }

  @Test
  public void findSessionsReturnsNullWhenIdIsNotFound() throws IOException {
    final String sessionId = "sessionId";

    when(sessionCache.getSession(sessionId)).thenReturn(null);

    final Session session = manager.findSession(sessionId);

    assertThat(session).isNull();
  }

  @Test
  public void findSessionsReturnsProperSessionByIdWhenIdAndContextNameIsValid() throws IOException {
    final String sessionId = "sessionId";
    final String contextName = "contextName";

    final DeltaSession expectedSession = mock(DeltaSession.class);
    when(sessionCache.getSession(sessionId)).thenReturn(expectedSession);
    when(expectedSession.getContextName()).thenReturn(contextName);
    when(context.getName()).thenReturn(contextName);

    final Session session = manager.findSession(sessionId);

    assertThat(session).isEqualTo(expectedSession);
  }

  @Test
  public void removeProperlyDestroysSessionFromSessionCacheWhenSessionIsNotExpired() {
    final DeltaSession sessionToDestroy = mock(DeltaSession.class);
    final String sessionId = "sessionId";

    when(sessionToDestroy.getId()).thenReturn(sessionId);
    when(sessionToDestroy.getExpired()).thenReturn(false);

    manager.remove(sessionToDestroy);

    verify(sessionCache).destroySession(sessionId);
  }

  @Test
  public void removeDoesNotDestroySessionFromSessionCacheWhenSessionIsExpired() {
    final DeltaSession sessionToDestroy = mock(DeltaSession.class);
    final String sessionId = "sessionId";

    when(sessionToDestroy.getId()).thenReturn(sessionId);
    when(sessionToDestroy.getExpired()).thenReturn(true);

    manager.remove(sessionToDestroy);

    verify(sessionCache, times(0)).destroySession(sessionId);
  }

  @Test
  public void addPutsSessionIntoSessionCacheAndIncrementsStats() {
    final DeltaSession sessionToPut = mock(DeltaSession.class);

    manager.add(sessionToPut);

    verify(sessionCache).putSession(sessionToPut);
    verify(cacheStats).incSessionsCreated();
  }

  @Test
  public void listIdsListsAllPresentIds() {
    final Set<String> ids = new HashSet<>();
    ids.add("id1");
    ids.add("id2");
    ids.add("id3");

    when(sessionCache.keySet()).thenReturn(ids);

    final String listOutput = manager.listSessionIds();

    for (final String id : ids) {
      assertThat(listOutput).contains(id);
    }
  }

  @Test
  public void loadActivatesAndAddsSingleSessionWithValidIdAndMoreRecentAccessTime()
      throws IOException, ClassNotFoundException {
    final String contextPath = "contextPath";
    final String expectedStoreDir = "";
    final DeltaSession newSession = mock(DeltaSession.class);
    final DeltaSession existingSession = mock(DeltaSession.class);

    prepareMocksForLoadTest(contextPath, newSession, existingSession, expectedStoreDir);

    manager.load();

    verify(newSession).activate();
    verify(manager).add(newSession);
  }

  @Test
  public void loadLogsWarningAndDoesNotAddSessionWhenSessionStoreNotFound()
      throws IOException, ClassNotFoundException {
    final String contextPath = "contextPath";
    final String expectedStoreDir = "";
    final DeltaSession newSession = mock(DeltaSession.class);
    final DeltaSession existingSession = mock(DeltaSession.class);

    prepareMocksForLoadTest(contextPath, newSession, existingSession, expectedStoreDir);

    doReturn(null).when(manager).getFileAtPath(any(), any());

    manager.load();

    verify(logger).debug("No session store file found");
    verify(manager, times(0)).add(any());
  }

  @Test
  public void loadDoesNotAddSessionToManagerWithValidIdAndLessRecentAccessTime()
      throws IOException, ClassNotFoundException {
    final String contextPath = "contextPath";
    final String expectedStoreDir = "";
    final DeltaSession newSession = mock(DeltaSession.class);
    final DeltaSession existingSession = mock(DeltaSession.class);

    prepareMocksForLoadTest(contextPath, newSession, existingSession, expectedStoreDir);

    when(existingSession.getLastAccessedTime()).thenReturn(2L);

    manager.load();

    verify(newSession, times(0)).activate();
    verify(manager, times(0)).add(newSession);
  }

  @Test
  public void unloadWritesSingleSessionToDiskWhenIdIsValid()
      throws IOException, NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    final String sessionId = "sessionId";
    final DeltaSession session = mock(DeltaSession.class);
    final FileOutputStream fos = mock(FileOutputStream.class);
    final BufferedOutputStream bos = mock(BufferedOutputStream.class);
    final ObjectOutputStream oos = mock(ObjectOutputStream.class);

    prepareMocksForUnloadTest(sessionId, fos, bos, oos, session);

    manager.unload();

    verify((StandardSession) session).writeObjectData(oos);
  }

  @Test
  public void unloadDoesNotWriteSessionToDiskAndClosesOutputStreamsWhenOutputStreamThrowsIOException()
      throws IOException, NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    final String sessionId = "sessionId";
    final DeltaSession session = mock(DeltaSession.class);
    final FileOutputStream fos = mock(FileOutputStream.class);
    final BufferedOutputStream bos = mock(BufferedOutputStream.class);
    final ObjectOutputStream oos = mock(ObjectOutputStream.class);

    prepareMocksForUnloadTest(sessionId, fos, bos, oos, session);

    final String exceptionMessage = "Output Stream IOException";

    final IOException exception = new IOException(exceptionMessage);

    doThrow(exception).when(manager).getObjectOutputStream(bos);

    assertThatThrownBy(() -> manager.unload()).isInstanceOf(IOException.class)
        .hasMessage(exceptionMessage);

    verify((StandardSession) session, times(0)).writeObjectData(oos);
    verify(bos).close();
    verify(fos).close();
  }

  @Test
  public void unloadDoesNotWriteSessionToDiskAndClosesOutputStreamsWhenSessionIsWrongClass()
      throws IOException, NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    final String sessionId = "sessionId";
    final DeltaSession session = mock(DeltaSession.class);
    final FileOutputStream fos = mock(FileOutputStream.class);
    final BufferedOutputStream bos = mock(BufferedOutputStream.class);
    final ObjectOutputStream oos = mock(ObjectOutputStream.class);

    prepareMocksForUnloadTest(sessionId, fos, bos, oos, session);

    final Session invalidSession =
        mock(Session.class, withSettings().extraInterfaces(DeltaSessionInterface.class));

    doReturn(invalidSession).when(manager).findSession(sessionId);

    assertThatThrownBy(() -> manager.unload()).isInstanceOf(IOException.class);

    verify((StandardSession) session, times(0)).writeObjectData(oos);
    verify(oos).close();
  }

  @Test
  public void successfulUnloadWithClientServerSessionCachePerformsLocalDestroy()
      throws IOException, NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    final String sessionId = "sessionId";
    final DeltaSession session = mock(DeltaSession.class);
    final FileOutputStream fos = mock(FileOutputStream.class);
    final BufferedOutputStream bos = mock(BufferedOutputStream.class);
    final ObjectOutputStream oos = mock(ObjectOutputStream.class);

    prepareMocksForUnloadTest(sessionId, fos, bos, oos, session);

    when(sessionCache.isClientServer()).thenReturn(true);
    when(session.getId()).thenReturn(sessionId);

    manager.unload();

    verify((StandardSession) session).writeObjectData(oos);
    verify(operatingRegion).localDestroy(sessionId);
  }

  @Test
  public void propertyChangeSetsMaxInactiveIntervalWithCorrectPropertyNameAndValue() {
    final String propertyName = "sessionTimeout";
    final PropertyChangeEvent event = mock(PropertyChangeEvent.class);
    final Context eventContext = mock(Context.class);
    final Integer newValue = 1;

    when(event.getSource()).thenReturn(eventContext);
    when(event.getPropertyName()).thenReturn(propertyName);
    when(event.getNewValue()).thenReturn(newValue);

    manager.propertyChange(event);

    verify(manager).setMaxInactiveInterval(newValue * 60);
  }

  @Test
  public void propertyChangeDoesNotSetMaxInactiveIntervalWithIncorrectPropertyName() {
    final String propertyName = "wrong name";
    final PropertyChangeEvent event = mock(PropertyChangeEvent.class);
    final Context eventContext = mock(Context.class);

    when(event.getSource()).thenReturn(eventContext);
    when(event.getPropertyName()).thenReturn(propertyName);

    manager.propertyChange(event);

    verify(manager, times(0)).setMaxInactiveInterval(anyInt());
  }

  @Test
  public void propertyChangeDoesNotSetNewMaxInactiveIntervalWithCorrectPropertyNameAndInvalidPropertyValue() {
    final String propertyName = "sessionTimeout";
    final PropertyChangeEvent event = mock(PropertyChangeEvent.class);
    final Context eventContext = mock(Context.class);
    final Integer newValue = -2;
    final Integer oldValue = DEFAULT_MAX_INACTIVE_INTERVAL;

    when(event.getSource()).thenReturn(eventContext);
    when(event.getPropertyName()).thenReturn(propertyName);
    when(event.getNewValue()).thenReturn(newValue);
    when(event.getOldValue()).thenReturn(oldValue);

    manager.propertyChange(event);

    verify(manager).setMaxInactiveInterval(oldValue);
  }

  public void prepareMocksForUnloadTest(final String sessionId, final FileOutputStream fos,
      final BufferedOutputStream bos, final ObjectOutputStream oos,
      final DeltaSession session)
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException, IOException {
    final String regionName = "regionName";
    final String contextPath = "contextPath";
    final String catalinaBaseSystemProp = "Catalina/Base";
    final String systemFileSeparator = "/";
    final String expectedStoreDir = catalinaBaseSystemProp + systemFileSeparator + "temp";

    final InternalQueryService queryService = mock(InternalQueryService.class);
    final Query query = mock(Query.class);
    final File store = mock(File.class);
    final SelectResults results = new LinkedResultSet();

    when(sessionCache.getCache()).thenReturn(cache);
    when(context.getPath()).thenReturn(contextPath);
    when(cache.getQueryService()).thenReturn(queryService);
    when(queryService.newQuery(anyString())).thenReturn(query);
    when(query.execute()).thenReturn(results);
    doReturn(catalinaBaseSystemProp).when(manager)
        .getSystemPropertyValue(DeltaSessionManager.catalinaBaseSystemProperty);
    doReturn(systemFileSeparator).when(manager)
        .getSystemPropertyValue(DeltaSessionManager.fileSeparatorSystemProperty);
    doReturn(store).when(manager).getFileAtPath(expectedStoreDir, contextPath);
    doReturn(fos).when(manager).getFileOutputStream(store);
    doReturn(bos).when(manager).getBufferedOutputStream(fos);
    doReturn(oos).when(manager).getObjectOutputStream(bos);
    doReturn(regionName).when(manager).getRegionName();
    doReturn(session).when(manager).findSession(sessionId);
    doNothing().when(manager).writeToObjectOutputStream(any(), any());

    results.add(sessionId);
  }

  public void prepareMocksForLoadTest(final String contextPath, final DeltaSession newSession,
      final DeltaSession existingSession, String expectedStoreDir)
      throws IOException, ClassNotFoundException {
    final String catalinaBaseSystemProp = "Catalina/Base";
    final String systemFileSeparator = "/";
    expectedStoreDir = catalinaBaseSystemProp + systemFileSeparator + "temp";
    final String newSessionId = "newSessionId";

    final File store = mock(File.class);
    final FileInputStream fis = mock(FileInputStream.class);
    final BufferedInputStream bis = mock(BufferedInputStream.class);
    final ObjectInputStream ois = mock(ObjectInputStream.class);
    final Loader loader = mock(Loader.class);

    when(context.getPath()).thenReturn(contextPath);
    when(context.getLoader()).thenReturn(loader);
    when(newSession.getId()).thenReturn(newSessionId);
    when(newSession.getLastAccessedTime()).thenReturn(1L);
    when(newSession.isValid()).thenReturn(true);
    when(existingSession.getLastAccessedTime()).thenReturn(0L);
    doReturn(catalinaBaseSystemProp).when(manager).getSystemPropertyValue("catalina.base");
    doReturn(systemFileSeparator).when(manager).getSystemPropertyValue("file.separator");
    doReturn(store).when(manager).getFileAtPath(expectedStoreDir, contextPath);
    doReturn(fis).when(manager).getFileInputStream(store);
    doReturn(bis).when(manager).getBufferedInputStream(fis);
    doReturn(ois).when(manager).getObjectInputStream(bis);
    doReturn(1).when(manager).getSessionCountFromObjectInputStream(ois);
    doReturn(newSession).when(manager).getNewSession();
    doReturn(existingSession).when(operatingRegion).get(newSessionId);
  }
}
