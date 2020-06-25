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

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;

import org.apache.catalina.Context;
import org.apache.juli.logging.Log;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.modules.session.catalina.callback.SessionExpirationCacheListener;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionStatistics;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public abstract class AbstractDeltaSessionIntegrationTest<DeltaSessionManagerT extends DeltaSessionManager<?>, DeltaSessionT extends DeltaSession> {
  protected static final String KEY = "key1";
  protected static final String REGION_NAME = "sessions";

  protected Region<String, HttpSession> region;
  protected final DeltaSessionManagerT manager;
  protected final Context context = mock(Context.class);

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  protected AbstractDeltaSessionIntegrationTest(final DeltaSessionManagerT manager) {
    this.manager = manager;
  }

  @Before
  public void before() {
    region = server.getCache()
        .<String, HttpSession>createRegionFactory(PARTITION)
        .addCacheListener(new SessionExpirationCacheListener())
        .create(REGION_NAME);

    when(manager.getLogger()).thenReturn(mock(Log.class));
    when(manager.getRegionName()).thenReturn(REGION_NAME);
    when(manager.getSessionCache()).thenReturn(mock(SessionCache.class));
    when(manager.getSessionCache().getOperatingRegion()).thenReturn(region);
    whenGetPreferDeserializedForm(manager);

    final DeltaSessionStatistics stats = mock(DeltaSessionStatistics.class);
    when(manager.getStatistics()).thenReturn(stats);
  }

  @SuppressWarnings("deprecation")
  private void whenGetPreferDeserializedForm(DeltaSessionManager<?> manager) {
    when(manager.getPreferDeserializedForm()).thenReturn(true);
  }

  protected abstract DeltaSessionT newSession(final DeltaSessionManagerT manager);

  @Test
  public void serializedAttributesNotLeakedWhenSessionInvalidated() throws IOException {
    final HttpSessionAttributeListener listener = mock(HttpSessionAttributeListener.class);
    when(context.getApplicationEventListeners()).thenReturn(new Object[] {listener});

    final DeltaSessionT session = spy(newSession(manager));
    session.setId(KEY, false);
    session.setValid(true);
    session.setOwner(manager);

    final String name = "attribute";
    final Object value1 = "value1";
    final byte[] serializedValue1 = BlobHelper.serializeToBlob(value1);
    // simulates initial deserialized state with serialized attribute values.
    session.getAttributes().put(name, serializedValue1);

    region.put(session.getId(), session);

    session.invalidate();

    final ArgumentCaptor<HttpSessionBindingEvent> event =
        ArgumentCaptor.forClass(HttpSessionBindingEvent.class);
    verify(listener).attributeRemoved(event.capture());
    verifyNoMoreInteractions(listener);
    assertThat(event.getValue().getValue()).isEqualTo(value1);
  }
}
