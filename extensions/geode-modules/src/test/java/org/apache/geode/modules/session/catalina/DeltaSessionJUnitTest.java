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

public class DeltaSessionJUnitTest {

  private DeltaSessionManager manager = mock(DeltaSessionManager.class);
  private Region<String, HttpSession> sessionRegion = mock(Region.class);
  private SessionCache sessionCache = mock(ClientServerSessionCache.class);
  DeltaSessionStatistics stats = mock(DeltaSessionStatistics.class);
  private final String sessionRegionName = "sessionRegionName";
  private final String contextName = "contextName";
  private Log logger = mock(Log.class);

  @Before
  public void setup() {
    when(manager.getRegionName()).thenReturn(sessionRegionName);
    when(manager.getSessionCache()).thenReturn(sessionCache);
    when(manager.getLogger()).thenReturn(logger);
    when(manager.getContextName()).thenReturn(contextName);
    when(manager.getStatistics()).thenReturn(stats);
    // For Client/Server behavior and some PeerToPeer use cases the session region and operating
    // regions
    // will be the same.
    when(sessionCache.getOperatingRegion()).thenReturn(sessionRegion);
    when(logger.isDebugEnabled()).thenReturn(true);
  }

  @Test
  public void sessionConstructionThrowsIllegalArgumentExceptionIfProvidedManagerIsNotDeltaSessionManager() {
    Manager invalidManager = mock(Manager.class);

    assertThatThrownBy(() -> new DeltaSession(invalidManager))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("The Manager must be an AbstractManager");
  }

  @Test
  public void sessionConstructionDoesNotThrowExceptionWithValidArgument() {
    DeltaSession session = new DeltaSession(manager);

    verify(logger).debug(anyString());
  }

  @Test
  public void getSessionCreatesFacadeWhenFacadeIsNullAndPackageProtectionDisabled() {
    DeltaSession session = new DeltaSession(manager);

    HttpSession returnedSession = session.getSession();

    assertThat(returnedSession).isNotNull();
  }

  @Test
  public void getSessionCreatesFacadeWhenFacadeIsNullAndPackageProtectionEnabled() {
    DeltaSession session = spy(new DeltaSession(manager));
    DeltaSessionFacade facade = mock(DeltaSessionFacade.class);
    doReturn(true).when(session).isPackageProtectionEnabled();
    doReturn(facade).when(session).getNewFacade(any(DeltaSession.class));

    HttpSession returnedSession = session.getSession();

    assertThat(returnedSession).isEqualTo(facade);
  }

  @Test
  public void processExpiredIncrementsStatisticsCountForExpiredSessions() {
    DeltaSession session = spy(new DeltaSession(manager));

    doNothing().when((StandardSession) session).expire(false);
    session.processExpired();

    verify(stats).incSessionsExpired();
  }

  @Test
  public void applyEventsAppliesEachEventAndPutsSessionIntoRegion() {
    DeltaSessionAttributeEvent event1 = mock(DeltaSessionAttributeEvent.class);
    DeltaSessionAttributeEvent event2 = mock(DeltaSessionAttributeEvent.class);
    List<DeltaSessionAttributeEvent> events = new ArrayList<>();
    events.add(event1);
    events.add(event2);
    Region<String, DeltaSessionInterface> region = mock(Region.class);
    DeltaSession session = spy(new DeltaSession(manager));

    session.applyAttributeEvents(region, events);

    // confirm that events were all added to the queue
    verify(session).addEventToEventQueue(event1);
    verify(session).addEventToEventQueue(event2);

    // confirm that session was put into region
    verify(region).put(session.getId(), session, true);
  }

  @Test
  public void commitThrowsIllegalStateExceptionWhenCalledOnInvalidSession() {
    DeltaSession session = spy(new DeltaSession(manager));
    String sessionId = "invalidatedSession";
    doReturn(sessionId).when(session).getId();

    assertThatThrownBy(() -> session.commit()).isInstanceOf(IllegalStateException.class)
        .hasMessage("commit: Session " + sessionId + " already invalidated");
  }

  @Test
  public void getSizeInBytesReturnsProperValueForMultipleAttributes() {
    String attrName1 = "attrName1";
    String attrName2 = "attrName2";
    List attrList = new ArrayList<String>();
    attrList.add(attrName1);
    attrList.add(attrName2);

    Enumeration<String> attrNames = Collections.enumeration(attrList);

    byte[] value1 = {0, 0, 0, 0};
    byte[] value2 = {0, 0, 0, 0, 0};
    int totalSize = value1.length + value2.length;

    DeltaSession session = spy(new DeltaSession(manager));
    doReturn(attrNames).when(session).getAttributeNames();
    doReturn(value1).when(session).getAttributeWithoutDeserialize(attrName1);
    doReturn(value2).when(session).getAttributeWithoutDeserialize(attrName2);

    int sessionSize = session.getSizeInBytes();

    assertThat(sessionSize).isEqualTo(totalSize);
  }

  @Test
  public void serializeLogsWarningWhenExceptionIsThrownDuringSerialization() throws IOException {
    Object obj = "unserialized object";
    String exceptionMessaage = "Serialization failed.";
    IOException exception = new IOException(exceptionMessaage);

    DeltaSession session = spy(new DeltaSession(manager));
    doThrow(exception).when(session).serializeViaBlobHelper(obj);
    session.serialize(obj);

    verify(logger).warn(anyString(), any(IOException.class));
  }

  @Test
  public void serializeReturnsSerializedObject() throws IOException {
    Object obj = "unserialized object";
    byte[] serializedObj = BlobHelper.serializeToBlob(obj);

    DeltaSession session = spy(new DeltaSession(manager));
    byte[] result = session.serialize(obj);

    assertThat(result).isEqualTo(serializedObj);
  }

  // @Test
  // public void testToData() throws IOException {
  // DeltaSession session = spy(new DeltaSession(manager));
  // DataOutput out = mock(DataOutput.class);
  //
  // session.toData(out);
  // }
}
