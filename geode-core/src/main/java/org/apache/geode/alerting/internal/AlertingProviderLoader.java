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

import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.alerting.spi.AlertingProvider;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.internal.util.ListCollectingServiceLoader;

/**
 * Loads a {@link AlertingProvider} using this order of preference:
 *
 * <pre>
 * 1. System Property {@code geode.ALERTING_PROVIDER_NAME} if specified
 * 2. {@code META-INF/services/org.apache.geode.alerting.spi.AlertingProvider} implementation if
 * found
 * 3. {@code SimpleAlertingProvider} as last resort
 * </pre>
 */
public class AlertingProviderLoader {

  private static final Logger logger = LogManager.getLogger();

  /**
   * System property that may be used to override which {@code AlertingProvider} to use.
   */
  @VisibleForTesting
  public static final String ALERTING_PROVIDER_NAME_PROPERTY =
      GEODE_PREFIX + "ALERTING_PROVIDER_NAME";

  public AlertingProvider load() {
    // 1: use ALERTING_PROVIDER_NAME_PROPERTY if set
    AlertingProvider providerFromSystemProperty = checkSystemProperty();
    if (providerFromSystemProperty != null) {
      logger.info("Using {} from System Property {} for service {}",
          providerFromSystemProperty.getClass().getName(), ALERTING_PROVIDER_NAME_PROPERTY,
          AlertingProvider.class.getName());
      return providerFromSystemProperty;
    }

    // 2: use ListCollectingServiceLoader and select highest priority
    SortedMap<Integer, AlertingProvider> alertingProviders = new TreeMap<>();
    loadServiceProviders()
        .forEach(provider -> alertingProviders.put(provider.getPriority(), provider));

    if (!alertingProviders.isEmpty()) {
      AlertingProvider providerFromServiceLoader =
          alertingProviders.get(alertingProviders.lastKey());
      logger.info("Using {} from ServiceLoader for service {}",
          providerFromServiceLoader.getClass().getName(), AlertingProvider.class.getName());
      return providerFromServiceLoader;
    }

    // 3: use SimpleAlertingProvider
    logger.info("Using {} for service {}", SimpleAlertingProvider.class.getName(),
        AlertingProvider.class.getName());
    return new SimpleAlertingProvider();
  }

  private Iterable<AlertingProvider> loadServiceProviders() {
    CollectingServiceLoader<AlertingProvider> serviceLoader = new ListCollectingServiceLoader<>();
    Collection<AlertingProvider> alertingProviders =
        serviceLoader.loadServices(AlertingProvider.class);
    return alertingProviders;
  }

  private AlertingProvider checkSystemProperty() {
    String agentClassName = System.getProperty(ALERTING_PROVIDER_NAME_PROPERTY);
    if (agentClassName == null) {
      return null;
    }

    try {
      Class<? extends AlertingProvider> agentClass =
          ClassPathLoader.getLatest().forName(agentClassName).asSubclass(AlertingProvider.class);
      return agentClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException | InstantiationException
        | IllegalAccessException e) {
      logger.warn("Unable to create AlertingProvider of type {}", agentClassName, e);
    }

    return null;
  }
}
