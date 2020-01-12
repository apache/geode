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

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;

/**
 * This listener will be attached to each notification region corresponding to a member
 *
 */
public class NotificationCacheListener implements CacheListener<NotificationKey, Notification> {

  /**
   * For the
   */
  private NotificationHubClient notifClient;

  private volatile boolean readyForEvents;

  public NotificationCacheListener(MBeanProxyFactory proxyHelper) {

    notifClient = new NotificationHubClient(proxyHelper);
    this.readyForEvents = false;

  }

  @Override
  public void afterCreate(EntryEvent<NotificationKey, Notification> event) {
    if (!readyForEvents) {
      return;
    }
    notifClient.sendNotification(event);

  }

  @Override
  public void afterDestroy(EntryEvent<NotificationKey, Notification> event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterInvalidate(EntryEvent<NotificationKey, Notification> event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterRegionClear(RegionEvent<NotificationKey, Notification> event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterRegionCreate(RegionEvent<NotificationKey, Notification> event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterRegionDestroy(RegionEvent<NotificationKey, Notification> event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterRegionInvalidate(RegionEvent<NotificationKey, Notification> event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterRegionLive(RegionEvent<NotificationKey, Notification> event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterUpdate(EntryEvent<NotificationKey, Notification> event) {
    if (!readyForEvents) {
      return;
    }
    notifClient.sendNotification(event);

  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  public void markReady() {
    readyForEvents = true;
  }

}
