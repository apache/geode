package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;

public class DeltaSessionJUnitTest {

  private DeltaSessionManager manager = mock(DeltaSessionManager.class);
  private Region<String, HttpSession> sessionRegion = mock(Region.class);
  private SessionCache sessionCache = mock(ClientServerSessionCache.class);
  private final String sessionRegionName = "sessionRegionName";
  private final String contextName = "contextName";
  private Log logger = mock(Log.class);

  @Before
  public void setup() {
    when(manager.getRegionName()).thenReturn(sessionRegionName);
    when(manager.getSessionCache()).thenReturn(sessionCache);
    when(manager.getLogger()).thenReturn(logger);
    when(manager.getContextName()).thenReturn(contextName);
    //For Client/Server behavior and some PeerToPeer use cases the session region and operating regions
    //will be the same.
    when(sessionCache.getOperatingRegion()).thenReturn(sessionRegion);
    when(logger.isDebugEnabled()).thenReturn(true);
  }

  @Test
  public void sessionConstructionThrowsIllegalArgumentExceptionIfProvidedManagerIsNotDeltaSessionManager() {
    Manager invalidManager = mock(Manager.class);

    assertThatThrownBy(() ->  new DeltaSession(invalidManager)).isInstanceOf(IllegalArgumentException.class)
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
  public void setAttributesPropertySetsAttributes() {
    String attrName = "Attr";
    Object attrValue = "Value";
    byte[] serializedValue = {0,0,0,0};
    boolean attrNotify = true;

    when(manager.isBackingCacheAvailable()).thenReturn(true);
    DeltaSession session = spy(new DeltaSession(manager));
    //doNothing().when((StandardSession)session).setAttribute(attrName, attrValue, attrNotify);
    doReturn(false).when(session).isCommitEnabled();
    doReturn(serializedValue).when(session).serialize(attrValue);

    session.setAttribute(attrName, attrValue, attrNotify);

    //verify(sessionRegion).put(any(String.class), any(DeltaSession.class), any());
  }

  @Test
  public void setAttributesThrowsExceptionIfBackingCacheDisabled() {
    String attrName = "Attr";
    Object attrValue = "Value";
    boolean attrNotify = true;

    when(manager.isBackingCacheAvailable()).thenReturn(false);
    DeltaSession session = spy(new DeltaSession(manager));

    assertThatThrownBy(() ->  session.setAttribute(attrName, attrValue, attrNotify)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No backing cache server is available.");
  }

}
