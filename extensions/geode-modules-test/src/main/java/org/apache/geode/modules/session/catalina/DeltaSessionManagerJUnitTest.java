package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.juli.logging.Log;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.cache.Region;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionStatistics;

public abstract class DeltaSessionManagerJUnitTest {

  protected DeltaSessionManager manager;
  protected AbstractSessionCache sessionCache;
  protected Log logger;
  protected Context context;
  protected DeltaSessionStatistics managerStats;
  protected DeltaSessionStatistics cacheStats;
  protected Region<String, HttpSession> operatingRegion;

  public void initTest() {
    sessionCache = mock(AbstractSessionCache.class);
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
  public void setContainerSetsProperContainerAndMaxInactiveInterval() {
    Context container = mock(Context.class);
    int containerMaxInactiveInterval = 3;

    doReturn(containerMaxInactiveInterval).when(container).getSessionTimeout();

    manager.setContainer(container);
    verify(manager).setMaxInactiveInterval(containerMaxInactiveInterval*60);
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

    for(String id : ids) {
      assertThat(listOutput).contains(id);
    }
  }

  @Test
  public void loadActivatesAndAddsSingleSessionWithValidIdAndMoreRecentAccessTime() throws IOException, ClassNotFoundException {
    String contextPath = "contextPath";
    String catalinaBaseSystemProp = "Catalina/Base";
    String systemFileSeparator = "/";
    String expectedStoreDir = catalinaBaseSystemProp + systemFileSeparator + "temp";
    String newSessionId = "newSessionId";

    File store = mock(File.class);
    FileInputStream fis = mock(FileInputStream.class);
    BufferedInputStream bis = mock(BufferedInputStream.class);
    ObjectInputStream ois = mock(ObjectInputStream.class);
    Loader loader = mock(Loader.class);
    DeltaSession newSession = mock(DeltaSession.class);
    DeltaSession existingSession = mock(DeltaSession.class);

    prepareMocksForLoadTest(contextPath, loader, newSession, newSessionId, existingSession, catalinaBaseSystemProp, systemFileSeparator, store, expectedStoreDir, fis, bis, ois);

    manager.load();

    verify(newSession).activate();
    verify(manager).add(newSession);
  }

  @Test
  public void loadLogsWarningAndDoesNotAddSessionWhenSessionStoreNotFound()
      throws IOException, ClassNotFoundException {
    String contextPath = "contextPath";
    String catalinaBaseSystemProp = "Catalina/Base";
    String systemFileSeparator = "/";
    String expectedStoreDir = catalinaBaseSystemProp + systemFileSeparator + "temp";
    String newSessionId = "newSessionId";

    File store = mock(File.class);
    FileInputStream fis = mock(FileInputStream.class);
    BufferedInputStream bis = mock(BufferedInputStream.class);
    ObjectInputStream ois = mock(ObjectInputStream.class);
    Loader loader = mock(Loader.class);
    DeltaSession newSession = mock(DeltaSession.class);
    DeltaSession existingSession = mock(DeltaSession.class);

    prepareMocksForLoadTest(contextPath, loader, newSession, newSessionId, existingSession, catalinaBaseSystemProp, systemFileSeparator, store, expectedStoreDir, fis, bis, ois);

    doReturn(null).when(manager).getFileAtPath(expectedStoreDir, contextPath);

    manager.load();

    verify(logger).debug("No session store file found");
    verify(manager, times(0)).add(any());
  }

  public void prepareMocksForLoadTest(String contextPath, Loader loader, DeltaSession newSession, String newSessionId, DeltaSession existingSession, String catalinaBaseSystemProp, String systemFileSeparator, File store, String expectedStoreDir,
                                      FileInputStream fis, BufferedInputStream bis, ObjectInputStream ois)
      throws IOException, ClassNotFoundException {
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
