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
package org.apache.geode.internal.alerting;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Manages a collection of registered {@code AlertingSessionListener}s and forwards
 * {@code AlertingSession} lifecycle callbacks to each listener.
 */
public class AlertingSessionListeners {

  private static final AlertingSessionListeners INSTANCE = new AlertingSessionListeners();

  public static AlertingSessionListeners get() {
    return INSTANCE;
  }

  private final Set<AlertingSessionListener> listeners;

  AlertingSessionListeners() {
    listeners = new LinkedHashSet<>();
  }

  /**
   * Adds the {@code AlertingSessionListener} and returns true if it was not already
   * registered.
   */
  public synchronized boolean addAlertingSessionListener(final AlertingSessionListener listener) {
    return listeners.add(listener);
  }

  /**
   * Removes the {@code AlertingSessionListener} and returns true if it was registered.
   */
  public synchronized boolean removeAlertingSessionListener(
      final AlertingSessionListener listener) {
    return listeners.remove(listener);
  }

  /**
   * Removes all {@code AlertingSessionListener}s that are registered.
   */
  public synchronized void clear() {
    listeners.clear();
  }

  public synchronized void createSession(final AlertMessaging alertMessaging) {
    for (AlertingSessionListener listener : listeners) {
      listener.createSession(alertMessaging);
    }
  }

  public synchronized void startSession() {
    for (AlertingSessionListener listener : listeners) {
      listener.startSession();
    }
  }

  public synchronized void stopSession() {
    for (AlertingSessionListener listener : listeners) {
      listener.stopSession();
    }
  }
}
