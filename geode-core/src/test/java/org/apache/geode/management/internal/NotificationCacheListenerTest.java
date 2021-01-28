
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
package org.apache.geode.management.internal;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import javax.management.Notification;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.EntryEvent;

/**
 * Unit tests for {@link NotificationCacheListener} (ie the SUT). These are characterization tests
 * that define behavior for an existing class. Test method names specify the SUT method and the
 * result of invoking that method.
 */
public class NotificationCacheListenerTest {

  private NotificationHubClient notificationHubClient;
  private EntryEvent<NotificationKey, Notification> notificationEntryEvent;

  private NotificationCacheListener notificationCacheListener;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    notificationHubClient = mock(NotificationHubClient.class);
    notificationEntryEvent = uncheckedCast(mock(EntryEvent.class));

    notificationCacheListener = new NotificationCacheListener(notificationHubClient);
  }

  @Test
  public void afterCreate_notifiesNotificationHubClient() {
    notificationCacheListener.markReady();
    notificationCacheListener.afterCreate(notificationEntryEvent);

    verify(notificationHubClient)
        .sendNotification(notificationEntryEvent);
  }

  @Test
  public void afterCreate_doesNothing_IfNotReady() {
    notificationCacheListener.afterCreate(notificationEntryEvent);

    verify(notificationHubClient, never())
        .sendNotification(notificationEntryEvent);
  }

  @Test
  public void afterCreate_doesNothing_ifEventIsNull() {
    notificationCacheListener.markReady();
    notificationCacheListener.afterCreate(null);

    verify(notificationHubClient, never())
        .sendNotification(notificationEntryEvent);
  }

  @Test
  public void afterUpdate_notifiesNotificationHubClient() {
    notificationCacheListener.markReady();
    notificationCacheListener.afterUpdate(notificationEntryEvent);

    verify(notificationHubClient)
        .sendNotification(notificationEntryEvent);
  }

  @Test
  public void afterUpdate_doesNothing_ifNotReady() {
    notificationCacheListener.afterUpdate(notificationEntryEvent);

    verify(notificationHubClient, never())
        .sendNotification(notificationEntryEvent);
  }

  @Test
  public void afterUpdate_doesNothing_ifEventIsNull() {
    notificationCacheListener.markReady();
    notificationCacheListener.afterUpdate(null);

    verify(notificationHubClient, never())
        .sendNotification(notificationEntryEvent);
  }
}
