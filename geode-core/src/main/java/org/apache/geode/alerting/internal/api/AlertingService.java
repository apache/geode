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
package org.apache.geode.alerting.internal.api;

import java.time.Instant;

import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.distributed.DistributedMember;

/**
 * Adds and removes alert listeners for local and remote members that need to receive
 * notification of system alerts.
 */
public interface AlertingService {

  void sendAlerts(AlertLevel alertLevel, Instant timestamp, String threadName, long threadId,
      String formattedMessage, String stackTrace);

  void addAlertListener(DistributedMember member, AlertLevel alertLevel);

  boolean removeAlertListener(DistributedMember member);

  boolean hasAlertListener(DistributedMember member, AlertLevel alertLevel);

  boolean hasAlertListeners();
}
