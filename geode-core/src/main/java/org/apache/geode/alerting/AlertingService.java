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
package org.apache.geode.alerting;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.alerting.internal.AlertListener;
import org.apache.geode.alerting.internal.AlertMessaging;
import org.apache.geode.alerting.internal.AlertingProviderLoader;
import org.apache.geode.alerting.internal.NullAlertMessaging;
import org.apache.geode.alerting.spi.AlertLevel;
import org.apache.geode.alerting.spi.AlertingProvider;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;

/**
 * Adds and removes {@code AlertListeners} for local and remote members that need to receive
 * notification of system {@code Alerts}.
 *
 * <p>
 * The {@code AlertingService} looks up the registered {@code AlertingProvider} from
 * {@code AlertingProviderRegistry} and delegates all calls to that provider.
 */
public class AlertingService {
  private static final Logger logger = LogManager.getLogger();

  @Immutable
  private static final AlertingProvider alertingProvider = new AlertingProviderLoader().load();

  // Listeners are ordered with the narrowest levels (e.g. FATAL) at the end
  private final CopyOnWriteArrayList<AlertListener> listeners = new CopyOnWriteArrayList<>();

  private final AtomicReference<AlertMessaging> alertMessagingRef =
      new AtomicReference<>(new NullAlertMessaging());

  public AlertingService() {
    // nothing
  }

  public void useAlertMessaging(AlertMessaging alertMessaging) {
    alertMessagingRef.set(alertMessaging);
  }

  public void sendAlerts(final AlertLevel alertLevel,
      final Date date,
      final String threadName,
      final String formattedMessage,
      final String stackTrace) {

    for (AlertListener listener : listeners) {
      if (alertLevel.meetsOrExceeds(listener.getLevel())) {
        break;
      }

      logger.trace("Sending alert message for {} to {}.", formattedMessage, listener.getMember());

      alertMessagingRef.get()
          .sendAlert(listener.getMember(), alertLevel, date, threadName, formattedMessage,
              stackTrace);
    }
  }

  public synchronized void addAlertListener(final DistributedMember member,
      final AlertLevel alertLevel) {
    if (alertLevel == AlertLevel.NONE) {
      return;
    }
    AlertListener listener = new AlertListener(alertLevel, member);

    // Add (or replace) a listener to the list of sorted listeners such that listeners with a
    // greater level (e.g. FATAL) will be at the end of the list.
    listeners.remove(listener);
    for (int i = 0; i < listeners.size(); i++) {
      if (listener.getLevel().compareTo(listeners.get(i).getLevel()) <= 0) {
        listeners.add(i, listener);
        return;
      }
    }
    listeners.add(listener);

    logger.debug("Added/Replaced alert listener for member {} at level {}.", member, alertLevel);
  }

  public synchronized boolean removeAlertListener(final DistributedMember member) {
    boolean memberWasFound = listeners.remove(new AlertListener(null, member));
    if (memberWasFound) {
      logger.debug("Removed alert listener for member {}.", member);
    }
    return memberWasFound;
  }

  public synchronized boolean hasAlertListener(final DistributedMember member,
      final AlertLevel alertLevel) {
    for (AlertListener listener : listeners) {
      if (listener.getMember().equals(member) && listener.getLevel().equals(alertLevel)) {
        return true;
      }
    }

    return false;
  }

  public boolean hasAlertListeners() {
    return !listeners.isEmpty();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
  }

  @VisibleForTesting
  public AlertingProvider getAlertingProvider() {
    return alertingProvider;
  }

  @VisibleForTesting
  synchronized List<AlertListener> getAlertListeners() {
    return Collections.unmodifiableList(listeners);
  }
}
