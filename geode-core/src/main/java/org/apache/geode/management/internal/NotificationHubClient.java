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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class actually distribute the notification with the help of the actual broadcaster proxy.
 *
 *
 */

public class NotificationHubClient {

  private static final Logger logger = LogService.getLogger();

  /**
   * proxy factory
   */
  private final MBeanProxyFactory proxyFactory;

  protected NotificationHubClient(MBeanProxyFactory proxyFactory) {
    this.proxyFactory = proxyFactory;
  }

  /**
   * send the notification to actual client on the Managing node VM
   *
   * it does not throw any exception. it will capture all exception and log a warning
   *
   */
  public void sendNotification(EntryEvent<NotificationKey, Notification> event) {

    NotificationBroadCasterProxy notifBroadCaster;
    try {

      notifBroadCaster = proxyFactory.findProxy(event.getKey().getObjectName(),
          NotificationBroadCasterProxy.class);
      // Will return null if the Bean is filtered out.
      if (notifBroadCaster != null) {
        notifBroadCaster.sendNotification(event.getNewValue());
      }

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(" NOTIFICATION Not Done {}", e.getMessage(), e);
      }
      logger.warn(e.getMessage(), e);
    }

  }

}
