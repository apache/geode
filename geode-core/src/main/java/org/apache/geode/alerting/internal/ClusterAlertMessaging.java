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

import static org.apache.geode.logging.internal.executors.LoggingExecutors.newFixedThreadPool;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.alerting.internal.spi.AlertingAction;
import org.apache.geode.alerting.internal.spi.AlertingIOException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.tcp.ReenteredConnectException;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClusterAlertMessaging implements AlertMessaging {

  private static final Logger LOGGER = LogService.getLogger();

  private final InternalDistributedSystem system;
  private final DistributionManager dm;
  private final AlertListenerMessageFactory alertListenerMessageFactory;
  private final ExecutorService executor;
  private final Consumer<AlertingIOException> alertingIOExceptionLogger;

  public ClusterAlertMessaging(final InternalDistributedSystem system) {
    this(system,
        system.getDistributionManager(),
        new AlertListenerMessageFactory(),
        newFixedThreadPool("AlertingMessaging Processor", true, 1),
        LOGGER::warn);
  }

  @VisibleForTesting
  ClusterAlertMessaging(final InternalDistributedSystem system, final DistributionManager dm,
      final AlertListenerMessageFactory alertListenerMessageFactory,
      final ExecutorService executor) {
    this(system, dm, alertListenerMessageFactory, executor, LOGGER::warn);
  }

  @VisibleForTesting
  ClusterAlertMessaging(final InternalDistributedSystem system, final DistributionManager dm,
      final AlertListenerMessageFactory alertListenerMessageFactory, final ExecutorService executor,
      final Consumer<AlertingIOException> alertingIOExceptionLogger) {
    this.system = system;
    this.dm = dm;
    this.alertListenerMessageFactory = alertListenerMessageFactory;
    this.executor = executor;
    this.alertingIOExceptionLogger = alertingIOExceptionLogger;
  }

  @Override
  public void sendAlert(final DistributedMember member,
      final AlertLevel alertLevel,
      final Instant timestamp,
      final String threadName,
      final long threadId,
      final String formattedMessage,
      final String stackTrace) {
    executor.submit(() -> AlertingAction.execute(() -> {
      try {
        String connectionName = system.getConfig().getName();

        AlertListenerMessage message =
            alertListenerMessageFactory.createAlertListenerMessage(member, alertLevel, timestamp,
                connectionName, threadName, threadId, formattedMessage, stackTrace);

        if (member.equals(system.getDistributedMember())) {
          // process in local member
          LOGGER.debug("Processing local alert message: {}, {}, {}, {}, {}, {}, [{}], [{}].",
              member, alertLevel, timestamp, connectionName, threadName, threadId,
              formattedMessage,
              stackTrace);
          processAlertListenerMessage(message);

        } else {
          // send to remote member
          LOGGER.debug("Sending remote alert message: {}, {}, {}, {}, {}, {}, [{}], [{}].",
              member, alertLevel, timestamp, connectionName, threadName, threadId,
              formattedMessage,
              stackTrace);
          dm.putOutgoing(message);
        }
      } catch (ReenteredConnectException ignore) {
        // OK. We can't send to this recipient because we're in the middle of
        // trying to connect to it.
      } catch (AlertingIOException e) {
        alertingIOExceptionLogger.accept(e);
      }
    }));
  }

  public void close() {
    executor.shutdownNow();
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + Integer.toHexString(hashCode());
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
