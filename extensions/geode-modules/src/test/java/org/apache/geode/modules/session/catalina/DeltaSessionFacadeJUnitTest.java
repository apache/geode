package org.apache.geode.modules.session.catalina;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Test;

public class DeltaSessionFacadeJUnitTest {

  @Test
  public void DeltaSessionFacadeMakesProperCallsOnSessionWhenInvoked() {
    DeltaSessionInterface session = spy(new DeltaSession());

    DeltaSessionFacade facade = new DeltaSessionFacade(session);

    doNothing().when(session).commit();
    doReturn(true).when(session).isValid();

    facade.commit();
    facade.isValid();

    verify(session).commit();
    verify(session).isValid();
  }
}
