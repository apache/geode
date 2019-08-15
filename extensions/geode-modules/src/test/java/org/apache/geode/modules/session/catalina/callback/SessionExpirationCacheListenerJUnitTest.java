package org.apache.geode.modules.session.catalina.callback;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.modules.session.catalina.DeltaSession;

import javax.servlet.http.HttpSession;

import org.junit.Test;

public class SessionExpirationCacheListenerJUnitTest {
  @Test
  public void TestAfterDestroyProcessesSessionExpiredByGemfire() {
    SessionExpirationCacheListener listener = new SessionExpirationCacheListener();
    EntryEvent<String, HttpSession> event = mock(EntryEvent.class);
    DeltaSession session = mock(DeltaSession.class);

    when(event.getOperation()).thenReturn(Operation.EXPIRE_DESTROY);
    when(event.getOldValue()).thenReturn(session);

    listener.afterDestroy(event);

    verify(session).processExpired();
  }
}
