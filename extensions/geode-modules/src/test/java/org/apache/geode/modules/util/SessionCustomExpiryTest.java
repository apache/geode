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

package org.apache.geode.modules.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import javax.servlet.http.HttpSession;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.util.internal.GeodeGlossary;

public class SessionCustomExpiryTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private InternalCache cache = mock(InternalCache.class);
  private SessionCustomExpiry expiry;
  private int expiryDelay = 3;

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "SessionExpiry.Delay",
        String.valueOf(expiryDelay));

    Field isServerField = SessionCustomExpiry.class.getDeclaredField("isServer");
    isServerField.setAccessible(true);
    isServerField.set(null, null);

    expiry = spy(SessionCustomExpiry.class);
    doReturn(cache).when(expiry).getCache();
  }

  @Test
  public void testGetExpiryOnServerWhenSetMaxInactiveInterval() {
    final Region.Entry<String, HttpSession> entry = mock(Region.Entry.class);
    final DeltaSession session = mock(DeltaSession.class);
    final int maxInactiveInterval = 2000;

    when(cache.isServer()).thenReturn(true);
    when(entry.getValue()).thenReturn(session);
    when(session.getMaxInactiveInterval()).thenReturn(maxInactiveInterval);

    ExpirationAttributes expirationAttrs = expiry.getExpiry(entry);

    assertThat(expirationAttrs).isEqualTo(
        new ExpirationAttributes(maxInactiveInterval + expiryDelay, ExpirationAction.DESTROY));
  }

  @Test
  public void testGetExpiryOnServerWhenSetMaxInactiveIntervalZero() {
    final Region.Entry<String, HttpSession> entry = mock(Region.Entry.class);
    final DeltaSession session = mock(DeltaSession.class);
    final int maxInactiveInterval = 0;

    when(cache.isServer()).thenReturn(true);
    when(entry.getValue()).thenReturn(session);
    when(session.getMaxInactiveInterval()).thenReturn(maxInactiveInterval);

    ExpirationAttributes expirationAttrs = expiry.getExpiry(entry);

    assertThat(expirationAttrs)
        .isEqualTo(new ExpirationAttributes(maxInactiveInterval, ExpirationAction.DESTROY));
  }

  @Test
  public void testGetExpiryOnClientWhenSetMaxInactiveInterval() {
    final Region.Entry<String, HttpSession> entry = mock(Region.Entry.class);
    final DeltaSession session = mock(DeltaSession.class);
    final int maxInactiveInterval = 3000;

    when(cache.isServer()).thenReturn(false);
    when(entry.getValue()).thenReturn(session);
    when(session.getMaxInactiveInterval()).thenReturn(maxInactiveInterval);

    ExpirationAttributes expirationAttrs = expiry.getExpiry(entry);

    assertThat(expirationAttrs)
        .isEqualTo(new ExpirationAttributes(maxInactiveInterval, ExpirationAction.DESTROY));
  }

  @Test
  public void testGetExpiryOnClientWhenSetMaxInactiveIntervalZero() {
    final Region.Entry<String, HttpSession> entry = mock(Region.Entry.class);
    final DeltaSession session = mock(DeltaSession.class);
    final int maxInactiveInterval = 0;

    when(cache.isServer()).thenReturn(false);
    when(entry.getValue()).thenReturn(session);
    when(session.getMaxInactiveInterval()).thenReturn(maxInactiveInterval);

    ExpirationAttributes expirationAttrs = expiry.getExpiry(entry);

    assertThat(expirationAttrs)
        .isEqualTo(new ExpirationAttributes(maxInactiveInterval, ExpirationAction.DESTROY));
  }

  @Test
  public void testGetExpiryWhenSessionIsNull() {
    final Region.Entry<String, HttpSession> entry = mock(Region.Entry.class);

    when(cache.isServer()).thenReturn(true);
    when(entry.getValue()).thenReturn(null);

    ExpirationAttributes expirationAttrs = expiry.getExpiry(entry);

    assertThat(expirationAttrs).isEqualTo(new ExpirationAttributes(1, ExpirationAction.DESTROY));
  }
}
