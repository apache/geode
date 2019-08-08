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

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.geode.alerting.AlertingService;
import org.apache.geode.alerting.spi.AlertingSessionListener;
import org.apache.geode.alerting.spi.AlertingSessionRegistry;
import org.apache.geode.annotations.internal.MakeNotStatic;

public class AlertingSessionRegistryProvider
    implements AlertingSessionRegistry, AlertingSessionNotifier {

  @MakeNotStatic
  private static final AlertingSessionRegistryProvider INSTANCE =
      new AlertingSessionRegistryProvider();

  public static AlertingSessionRegistryProvider get() {
    return INSTANCE;
  }

  private final Set<AlertingSessionListener> listeners;

  AlertingSessionRegistryProvider() {
    listeners = new LinkedHashSet<>();
  }

  @Override
  public synchronized void addAlertingSessionListener(final AlertingSessionListener listener) {
    listeners.add(listener);
  }

  @Override
  public synchronized void removeAlertingSessionListener(
      final AlertingSessionListener listener) {
    listeners.remove(listener);
  }

  @Override
  public synchronized void clear() {
    listeners.clear();
  }

  @Override
  public synchronized void createSession(final AlertingService alertingService) {
    for (AlertingSessionListener listener : listeners) {
      listener.createSession(alertingService);
    }
  }

  @Override
  public synchronized void startSession() {
    for (AlertingSessionListener listener : listeners) {
      listener.startSession();
    }
  }

  @Override
  public synchronized void stopSession() {
    for (AlertingSessionListener listener : listeners) {
      listener.stopSession();
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
  }
}
