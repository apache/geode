package org.apache.geode.modules.session.catalina.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

public class DeltaSessionUpdateAttributeEventJUnitTest {
  @Test
  public void DeltaSessionDestroyAttributeEventAppliesAttributeToSession() {
    String attributeName = "UpdateAttribute";
    String attributeValue = "UpdateValue";

    DeltaSessionUpdateAttributeEvent event =
        new DeltaSessionUpdateAttributeEvent(attributeName, attributeValue);
    DeltaSessionInterface deltaSessionInterface = mock(DeltaSessionInterface.class);
    event.apply((deltaSessionInterface));

    verify(deltaSessionInterface).localUpdateAttribute(attributeName, attributeValue);
  }
}
