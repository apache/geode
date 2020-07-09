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

package org.apache.geode.modules.session.catalina.callback;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpSession;

import org.junit.Test;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.modules.session.catalina.DeltaSession;

public class SessionExpirationCacheListenerTest {
  @Test
  public void testAfterDestroyProcessesSessionExpired() {
    final SessionExpirationCacheListener listener = new SessionExpirationCacheListener();
    final EntryEvent<String, HttpSession> event = mock(EntryEvent.class);
    final DeltaSession session = mock(DeltaSession.class);

    when(event.getOperation()).thenReturn(Operation.EXPIRE_DESTROY);
    when(event.getOldValue()).thenReturn(session);

    listener.afterDestroy(event);

    verify(session).processExpired();
  }
}
