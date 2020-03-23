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

import javax.management.Notification;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;

/**
 * This listener will be attached to each notification region corresponding to a member
 */
class NotificationCacheListener extends CacheListenerAdapter<NotificationKey, Notification> {

  private final NotificationHubClient notificationHubClient;

  NotificationCacheListener(MBeanProxyFactory mBeanProxyFactory) {
    this(new NotificationHubClient(mBeanProxyFactory));
  }

  @VisibleForTesting
  NotificationCacheListener(NotificationHubClient notificationHubClient) {
    this.notificationHubClient = notificationHubClient;
  }

  @Override
  public void afterCreate(EntryEvent<NotificationKey, Notification> event) {
    notificationHubClient.sendNotification(event);
  }

  @Override
  public void afterUpdate(EntryEvent<NotificationKey, Notification> event) {
    notificationHubClient.sendNotification(event);
  }
}
