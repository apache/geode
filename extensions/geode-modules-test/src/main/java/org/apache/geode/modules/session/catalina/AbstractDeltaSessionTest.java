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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionAttributeEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionStatistics;

public abstract class AbstractDeltaSessionTest<SessionT extends DeltaSession> {

  protected final DeltaSessionManager<?> manager = mock(DeltaSessionManager.class);
  private final Region<String, HttpSession> sessionRegion = mock(Region.class);
  private final SessionCache sessionCache = mock(ClientServerSessionCache.class);
  private final DeltaSessionStatistics stats = mock(DeltaSessionStatistics.class);
  private final Log logger = mock(Log.class);

  @Before
  public void setup() {
    String sessionRegionName = "sessionRegionName";
    when(manager.getRegionName()).thenReturn(sessionRegionName);
    when(manager.getSessionCache()).thenReturn(sessionCache);
    when(manager.getLogger()).thenReturn(logger);
    when(manager.getContextName()).thenReturn("contextName");
    when(manager.getStatistics()).thenReturn(stats);
    when(manager.isBackingCacheAvailable()).thenReturn(true);
    setupDeprecated();
    // For Client/Server behavior and some PeerToPeer use cases the session region and operating
    // regions
    // will be the same.
    when(sessionCache.getOperatingRegion()).thenReturn(sessionRegion);
    when(logger.isDebugEnabled()).thenReturn(true);
  }

  @SuppressWarnings("deprecation")
  protected void setupDeprecated() {
    when(manager.getPreferDeserializedForm()).thenReturn(true);
  }

  protected abstract SessionT newDeltaSession(Manager manager);

  @Test
  public void sessionConstructionThrowsIllegalArgumentExceptionIfProvidedManagerIsNotDeltaSessionManager() {
    final Manager invalidManager = mock(Manager.class);

    assertThatThrownBy(() -> newDeltaSession(invalidManager))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("The Manager must be an AbstractManager");
  }

  @Test
  public void sessionConstructionDoesNotThrowExceptionWithValidArgument() {
    newDeltaSession(manager);
    verify(logger).debug(anyString());
  }

  @Test
  public void getSessionCreatesFacadeWhenFacadeIsNullAndPackageProtectionDisabled() {
    final DeltaSession session = newDeltaSession(manager);

    final HttpSession returnedSession = session.getSession();

    assertThat(returnedSession).isNotNull();
  }

  @Test
  public void getSessionCreatesFacadeWhenFacadeIsNullAndPackageProtectionEnabled() {
    final DeltaSession session = spy(newDeltaSession(manager));
    final DeltaSessionFacade facade = mock(DeltaSessionFacade.class);
    doReturn(true).when(session).isPackageProtectionEnabled();
    doReturn(facade).when(session).getNewFacade(any(DeltaSession.class));

    final HttpSession returnedSession = session.getSession();

    assertThat(returnedSession).isEqualTo(facade);
  }

  @Test
  public void processExpiredIncrementsStatisticsCountForExpiredSessions() {
    final DeltaSession session = spy(newDeltaSession(manager));

    doNothing().when((StandardSession) session).expire(false);
    session.processExpired();

    verify(stats).incSessionsExpired();
  }

  @Test
  public void applyEventsAppliesEachEventAndPutsSessionIntoRegion() {
    final DeltaSessionAttributeEvent event1 = mock(DeltaSessionAttributeEvent.class);
    final DeltaSessionAttributeEvent event2 = mock(DeltaSessionAttributeEvent.class);
    final List<DeltaSessionAttributeEvent> events = new ArrayList<>();
    events.add(event1);
    events.add(event2);
    @SuppressWarnings("unchecked")
    final Region<String, DeltaSessionInterface> region = mock(Region.class);
    final DeltaSession session = spy(newDeltaSession(manager));

    session.applyAttributeEvents(region, events);

    // confirm that events were all added to the queue
    verify(session).addEventToEventQueue(event1);
    verify(session).addEventToEventQueue(event2);

    // confirm that session was put into region
    verify(region).put(session.getId(), session, true);
  }

  @Test
  public void commitThrowsIllegalStateExceptionWhenCalledOnInvalidSession() {
    final DeltaSession session = spy(newDeltaSession(manager));
    final String sessionId = "invalidatedSession";
    doReturn(sessionId).when(session).getId();

    assertThatThrownBy(session::commit).isInstanceOf(IllegalStateException.class)
        .hasMessage("commit: Session " + sessionId + " already invalidated");
  }

  @Test
  public void getSizeInBytesReturnsProperValueForMultipleAttributes() {
    final String attrName1 = "attrName1";
    final String attrName2 = "attrName2";
    final List<String> attrList = new ArrayList<>();
    attrList.add(attrName1);
    attrList.add(attrName2);

    final Enumeration<String> attrNames = Collections.enumeration(attrList);

    final byte[] value1 = {0, 0, 0, 0};
    final byte[] value2 = {0, 0, 0, 0, 0};
    final int totalSize = value1.length + value2.length;

    final DeltaSession session = spy(newDeltaSession(manager));
    doReturn(attrNames).when(session).getAttributeNames();
    doReturn(value1).when(session).getAttributeWithoutDeserialize(attrName1);
    doReturn(value2).when(session).getAttributeWithoutDeserialize(attrName2);

    final int sessionSize = session.getSizeInBytes();

    assertThat(sessionSize).isEqualTo(totalSize);
  }

  @Test
  public void serializeLogsWarningWhenExceptionIsThrownDuringSerialization() throws IOException {
    final Object obj = "unserialized object";
    final String exceptionMessaage = "Serialization failed.";
    final IOException exception = new IOException(exceptionMessaage);

    final DeltaSession session = spy(newDeltaSession(manager));
    doThrow(exception).when(session).serializeViaBlobHelper(obj);
    session.serialize(obj);

    verify(logger).warn(anyString(), any(IOException.class));
  }

  @Test
  public void serializeReturnsSerializedObject() throws IOException {
    final Object obj = "unserialized object";
    final byte[] serializedObj = BlobHelper.serializeToBlob(obj);

    final DeltaSession session = spy(newDeltaSession(manager));
    final byte[] result = session.serialize(obj);

    assertThat(result).isEqualTo(serializedObj);
  }

}
