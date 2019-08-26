package org.apache.geode.modules.session.catalina;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
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
  }

  @Test
  public void sessionConstructionThrowsIllegalArgumentExceptionIfProvidedManagerIsNotDeltaSessionManager() {
    Manager invalidManager = mock(Manager.class);

    assertThatThrownBy(() ->  new DeltaSession(invalidManager)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("The Manager must be an AbstractManager");
  }

  @Test
  public void sessionConstructionDoesNotThrowExceptionWithValidArgument() {
    when(logger.isDebugEnabled()).thenReturn(true);
    DeltaSession session = new DeltaSession(manager);

    verify(logger).debug(anyString());
  }
}
