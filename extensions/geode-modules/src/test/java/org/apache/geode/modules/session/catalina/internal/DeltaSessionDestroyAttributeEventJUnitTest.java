package org.apache.geode.modules.session.catalina.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

public class DeltaSessionDestroyAttributeEventJUnitTest {
  @Test
  public void DeltaSessionDestroyAttributeEventAppliesAttributeToSession() {
    String attributeName = "DestroyAttribute";

    DeltaSessionDestroyAttributeEvent event = new DeltaSessionDestroyAttributeEvent(attributeName);
    DeltaSessionInterface deltaSessionInterface = mock(DeltaSessionInterface.class);
    event.apply((deltaSessionInterface));

    verify(deltaSessionInterface).localDestroyAttribute(attributeName);
  }
}
