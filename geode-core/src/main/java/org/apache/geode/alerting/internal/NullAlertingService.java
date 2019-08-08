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
package org.apache.geode.alerting.internal;

import java.time.Instant;

import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;

/**
 * Null implementation of {@link AlertingService} that does nothing.
 */
public class NullAlertingService implements InternalAlertingService {

  @Immutable
  private static final NullAlertingService INSTANCE = new NullAlertingService();

  public static NullAlertingService get() {
    return INSTANCE;
  }

  private NullAlertingService() {
    // nothing
  }

  @Override
  public void useAlertMessaging(AlertMessaging alertMessaging) {
    // nothing
  }

  @Override
  public void sendAlerts(AlertLevel alertLevel, Instant timestamp, String threadName, long threadId,
      String formattedMessage, String stackTrace) {
    // nothing
  }

  @Override
  public void addAlertListener(DistributedMember member, AlertLevel alertLevel) {
    // nothing
  }

  @Override
  public boolean removeAlertListener(DistributedMember member) {
    return false;
  }

  @Override
  public boolean hasAlertListener(DistributedMember member, AlertLevel alertLevel) {
    return false;
  }

  @Override
  public boolean hasAlertListeners() {
    return false;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
  }
}
