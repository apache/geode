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

import java.util.Date;

import org.apache.logging.log4j.Logger;

import org.apache.geode.alerting.spi.AlertLevel;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.tcp.ReenteredConnectException;

public class ClusterAlertMessaging implements AlertMessaging {

  private static final Logger logger = LogService.getLogger();

  private final InternalDistributedSystem system;
  private final DistributionManager dm;
  private final AlertListenerMessageFactory alertListenerMessageFactory;

  public ClusterAlertMessaging(final InternalDistributedSystem system) {
    this(system, system.getDistributionManager(), new AlertListenerMessageFactory());
  }

  @VisibleForTesting
  ClusterAlertMessaging(final InternalDistributedSystem system,
      final DistributionManager dm,
      final AlertListenerMessageFactory alertListenerMessageFactory) {
    this.system = system;
    this.dm = dm;
    this.alertListenerMessageFactory = alertListenerMessageFactory;
  }

  @Override
  public void sendAlert(final DistributedMember member,
      final AlertLevel alertLevel,
      final Date date,
      final String threadName,
      final String formattedMessage,
      final String stackTrace) {
    try {
      String connectionName = system.getConfig().getName();

      AlertListenerMessage message = alertListenerMessageFactory.createAlertListenerMessage(member,
          alertLevel, date, connectionName, threadName, formattedMessage, stackTrace);

      if (member.equals(system.getDistributedMember())) {
        // process in local member
        logger.debug("Processing local alert message: {}, {}, {}, {}, {}, [{}], [{}].",
            member, alertLevel, date, connectionName, threadName, formattedMessage, stackTrace);
        processAlertListenerMessage(message);

      } else {
        // send to remote member
        logger.debug("Sending remote alert message: {}, {}, {}, {}, {}, [{}], [{}].",
            member, alertLevel, date, connectionName, threadName, formattedMessage, stackTrace);
        dm.putOutgoing(message);
      }
    } catch (ReenteredConnectException ignore) {
      // OK. We can't send to this recipient because we're in the middle of
      // trying to connect to it.
    }
  }

  @VisibleForTesting
  void processAlertListenerMessage(final AlertListenerMessage message) {
    verifyDistributionManagerCanProcessMessage();
    message.process((ClusterDistributionManager) dm);
  }

  /**
   * Remove verifyDistributionManagerCanProcessMessage when
   * {@link AlertListenerMessage#process(ClusterDistributionManager)} no longer requires
   * ClusterDistributionManager.
   */
  private void verifyDistributionManagerCanProcessMessage() {
    if (!(dm instanceof ClusterDistributionManager)) {
      throw new IllegalArgumentException(
          "Processing of AlertListenerMessage requires ClusterDistributionManager instead of "
              + dm.getClass().getSimpleName());
    }
  }
}
