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

import static org.apache.geode.internal.admin.remote.AlertListenerMessage.create;

import java.time.Instant;

import org.apache.geode.alerting.spi.AlertLevel;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;

class AlertListenerMessageFactory {

  AlertListenerMessage createAlertListenerMessage(final DistributedMember member,
      final AlertLevel alertLevel,
      final Instant timestamp,
      final String connectionName,
      final String threadName,
      final long threadId,
      final String formattedMessage,
      final String stackTrace) {
    verifyDistributedMemberCanReceiveMessage(member);
    return create(member, alertLevel.intLevel(), timestamp, connectionName, threadName, threadId,
        formattedMessage, stackTrace);
  }

  /**
   * Remove verifyDistributedMemberCanReceiveMessage when AlertListenerMessage no longer casts to
   * InternalDistributedMember.
   */
  private void verifyDistributedMemberCanReceiveMessage(final DistributedMember member) {
    if (!(member instanceof InternalDistributedMember)) {
      throw new IllegalArgumentException(
          "Creation of AlertListenerMessage requires InternalDistributedMember instead of "
              + member.getClass().getSimpleName());
    }
  }
}
