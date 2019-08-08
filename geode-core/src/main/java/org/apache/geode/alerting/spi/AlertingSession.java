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
package org.apache.geode.alerting.spi;

import org.apache.geode.alerting.AlertingService;
import org.apache.geode.alerting.internal.AlertingSessionNotifier;
import org.apache.geode.alerting.internal.AlertingSessionRegistryProvider;
import org.apache.geode.annotations.VisibleForTesting;

/**
 * The {@code DistributedSystem} connection uses {@code AlertingSession} to control the lifecycle
 * of the registered {@code AlertingProvider}.
 *
 * <p>
 * During initialization of {@code DistributedSystem} a new {@code AlertingSession} is instantiated
 * and {@code createSession} will be invoked with a new instance of {@code ClusterAlertMessaging}.
 * After
 * a {@code DistributionConfig} exists, it will then invoke {@code startSession}. During disconnect,
 * it will invoke {@code stopSession}.
 *
 * <p>
 * The {@code AlertAppender} will capture the {@code ClusterAlertMessaging} in order to send out
 * {@code Alert} messages to registered {@code Alert} listeners. {@code startSession} will cause
 * the appender to unpause and begin processing all incoming log events. Any log event that meets
 * the {@code AlertLevel} of one or more {@code Alert} listeners will result in the generation of
 * an {@code Alert} being sent to those listeners.
 */
public class AlertingSession {

  private final AlertingSessionNotifier alertingSessionNotifier;
  private State state = State.STOPPED;

  public static AlertingSession create() {
    return create(AlertingSessionRegistryProvider.get());
  }

  @VisibleForTesting
  static AlertingSession create(AlertingSessionNotifier alertingSessionListeners) {
    return new AlertingSession(alertingSessionListeners);
  }

  private AlertingSession(AlertingSessionNotifier alertingSessionNotifier) {
    this.alertingSessionNotifier = alertingSessionNotifier;
  }

  public synchronized void createSession(AlertingService alertingService) {
    state = state.changeTo(State.CREATED);
    alertingSessionNotifier.createSession(alertingService);
  }

  public synchronized void startSession() {
    state = state.changeTo(State.STARTED);
    alertingSessionNotifier.startSession();
  }

  public synchronized void stopSession() {
    state = state.changeTo(State.STOPPED);
    alertingSessionNotifier.stopSession();
  }

  public synchronized void shutdown() {
    // nothing
  }

  @VisibleForTesting
  AlertingSessionNotifier getAlertingSessionNotifier() {
    return alertingSessionNotifier;
  }

  synchronized State getState() {
    return state;
  }

  enum State {
    CREATED,
    STARTED,
    STOPPED;

    State changeTo(final State newState) {
      switch (newState) {
        case CREATED:
          if (this != STOPPED) {
            throw new IllegalStateException("Session must not exist before creating");
          }
          return CREATED;
        case STARTED:
          if (this != CREATED) {
            throw new IllegalStateException("Session must be created before starting");
          }
          return STARTED;
        case STOPPED:
          if (this != STARTED) {
            throw new IllegalStateException("Session must be started before stopping");
          }
      }
      return STOPPED;
    }
  }
}
