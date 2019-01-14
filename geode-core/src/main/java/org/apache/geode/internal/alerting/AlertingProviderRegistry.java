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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * Provides publication of {@code AlertingProvider} that may be initiated by a third party logging
 * library such as Log4J2.
 *
 * <p>
 * This mechanism is used instead of a manifest-based {@code ServiceLoader} so that
 * {@code AlertAppender} is registered only if Log4J2 creates it.
 */
public class AlertingProviderRegistry {

  private static final AlertingProvider NULL_ALERTING_PROVIDER = new NullAlertingProvider();

  private static final AlertingProviderRegistry INSTANCE = new AlertingProviderRegistry();

  public static AlertingProviderRegistry get() {
    return INSTANCE;
  }

  @VisibleForTesting
  public static AlertingProvider getNullAlertingProvider() {
    return NULL_ALERTING_PROVIDER;
  }

  private final AlertingSessionListeners alertingSessionListeners;
  private final AtomicReference<AlertingProvider> alertingProviderRef = new AtomicReference<>();

  AlertingProviderRegistry() {
    this(AlertingSessionListeners.get(), NULL_ALERTING_PROVIDER);
  }

  private AlertingProviderRegistry(final AlertingSessionListeners alertingSessionListeners,
      final AlertingProvider alertingProvider) {
    if (alertingSessionListeners == null) {
      throw new NullPointerException("alertingSessionListeners must not be null");
    }
    if (alertingProvider == null) {
      throw new NullPointerException("alertingProvider must not be null");
    }
    this.alertingSessionListeners = alertingSessionListeners;
    alertingProviderRef.set(alertingProvider);
  }

  public AlertingProvider getAlertingProvider() {
    return alertingProviderRef.get();
  }

  public void registerAlertingProvider(final AlertingProvider provider) {
    alertingSessionListeners.addAlertingSessionListener(provider);
    alertingProviderRef.compareAndSet(NULL_ALERTING_PROVIDER, provider);
  }

  public void unregisterAlertingProvider(final AlertingProvider provider) {
    alertingProviderRef.compareAndSet(provider, NULL_ALERTING_PROVIDER);
    alertingSessionListeners.removeAlertingSessionListener(provider);
  }

  public void clear() {
    unregisterAlertingProvider(alertingProviderRef.get());
  }
}
