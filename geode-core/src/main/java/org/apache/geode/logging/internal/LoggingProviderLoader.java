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
package org.apache.geode.logging.internal;

import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.internal.util.ListCollectingServiceLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LoggingProvider;

/**
 * Loads a {@link LoggingProvider} using this order of preference:
 *
 * <pre>
 * 1. System Property {@code geode.LOGGING_PROVIDER_NAME} if specified
 * 2. {@code META-INF/services/org.apache.geode.logging.internal.spi.LoggingProvider} implementation if found
 * 3. {@code SimpleLoggingProvider} as last resort
 * </pre>
 */
public class LoggingProviderLoader {

  private static final Logger logger = LogService.getLogger();

  /**
   * System property that may be used to override which {@code LoggingProvider} to use.
   */
  @VisibleForTesting
  public static final String LOGGING_PROVIDER_NAME_PROPERTY =
      GEODE_PREFIX + "LOGGING_PROVIDER_NAME";

  public LoggingProvider load() {
    // 1: use LOGGING_PROVIDER_NAME_PROPERTY if set
    LoggingProvider providerFromSystemProperty = checkSystemProperty();
    if (providerFromSystemProperty != null) {
      logger.info("Using {} from System Property {} for service {}",
          providerFromSystemProperty.getClass().getName(), LOGGING_PROVIDER_NAME_PROPERTY,
          LoggingProvider.class.getName());
      return providerFromSystemProperty;
    }

    // 2: use ListCollectingServiceLoader and select highest priority
    SortedMap<Integer, LoggingProvider> loggingProviders = new TreeMap<>();
    loadServiceProviders()
        .forEach(provider -> loggingProviders.put(provider.getPriority(), provider));

    if (!loggingProviders.isEmpty()) {
      LoggingProvider providerFromServiceLoader = loggingProviders.get(loggingProviders.lastKey());
      logger.info("Using {} from ServiceLoader for service {}",
          providerFromServiceLoader.getClass().getName(), LoggingProvider.class.getName());
      return providerFromServiceLoader;
    }

    // 3: use SimpleLoggingProvider
    logger.info("Using {} for service {}", SimpleLoggingProvider.class.getName(),
        LoggingProvider.class.getName());
    return new SimpleLoggingProvider();
  }

  private Iterable<LoggingProvider> loadServiceProviders() {
    CollectingServiceLoader<LoggingProvider> serviceLoader = new ListCollectingServiceLoader<>();
    Collection<LoggingProvider> loggingProviders =
        serviceLoader.loadServices(LoggingProvider.class);
    return loggingProviders;
  }

  private LoggingProvider checkSystemProperty() {
    String agentClassName = System.getProperty(LOGGING_PROVIDER_NAME_PROPERTY);
    if (agentClassName == null) {
      return null;
    }

    try {
      Class<? extends LoggingProvider> agentClass =
          ClassPathLoader.getLatest().forName(agentClassName).asSubclass(LoggingProvider.class);
      return agentClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException | InstantiationException
        | IllegalAccessException e) {
      logger.warn("Unable to create LoggingProvider of type {}", agentClassName, e);
    }

    return null;
  }
}
