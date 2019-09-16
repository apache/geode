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
package org.apache.geode.internal.logging;

import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.internal.logging.DefaultProviderChecker.DEFAULT_PROVIDER_AGENT_NAME;

import java.util.ServiceLoader;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.ClassPathLoader;

/**
 * Loads a {@link ProviderAgent} using this order of preference:
 *
 * <pre>
 * 1. {@code META-INF/services/org.apache.geode.internal.logging.ProviderAgent} if found
 * 2. System Property {@code geode.PROVIDER_AGENT_NAME} if specified
 * 3. {@code org.apache.geode.internal.logging.log4j.Log4jAgent} if
 * {@code org.apache.logging.log4j.core.Logger} can be class loaded
 * 4. The no-op implementation {@code NullProviderAgent} as last resort
 * </pre>
 *
 * <p>
 * TODO: extract logging.log4j package to geode-logging module and use ServiceLoader only
 */
public class ProviderAgentLoader {

  private static final Logger LOGGER = StatusLogger.getLogger();

  /**
   * System property that may be used to override which {@code ProviderAgent} to use.
   */
  static final String PROVIDER_AGENT_NAME_PROPERTY = GEODE_PREFIX + "PROVIDER_AGENT_NAME";

  private final AvailabilityChecker availabilityChecker;

  public ProviderAgentLoader() {
    this(new DefaultProviderChecker());
  }

  @VisibleForTesting
  ProviderAgentLoader(final AvailabilityChecker availabilityChecker) {
    this.availabilityChecker = availabilityChecker;
  }

  /**
   * Returns true if the default provider {@code Log4j Core} is available.
   */
  public boolean isDefaultAvailable() {
    return availabilityChecker.isAvailable();
  }

  public ProviderAgent findProviderAgent() {
    ServiceLoader<ProviderAgent> serviceLoaderFromExt =
        ServiceLoader.loadInstalled(ProviderAgent.class);
    if (serviceLoaderFromExt.iterator().hasNext()) {
      ProviderAgent providerAgent = serviceLoaderFromExt.iterator().next();
      LOGGER.info("Using {} from Extension ClassLoader for service {}",
          providerAgent.getClass().getName(), ProviderAgent.class.getName());
      return providerAgent;
    }

    ServiceLoader<ProviderAgent> serviceLoaderFromTccl = ServiceLoader.load(ProviderAgent.class);
    if (serviceLoaderFromTccl.iterator().hasNext()) {
      ProviderAgent providerAgent = serviceLoaderFromTccl.iterator().next();
      LOGGER.info("Using {} from Thread Context ClassLoader for service {}",
          providerAgent.getClass().getName(), ProviderAgent.class.getName());
      return providerAgent;
    }

    ServiceLoader<ProviderAgent> serviceLoaderFromSys =
        ServiceLoader.load(ProviderAgent.class, null);
    if (serviceLoaderFromSys.iterator().hasNext()) {
      ProviderAgent providerAgent = serviceLoaderFromSys.iterator().next();
      LOGGER.info("Using {} from System ClassLoader for service {}",
          providerAgent.getClass().getName(), ProviderAgent.class.getName());
      return providerAgent;
    }

    return createProviderAgent();
  }

  ProviderAgent createProviderAgent() {
    if (System.getProperty(PROVIDER_AGENT_NAME_PROPERTY) == null && !isDefaultAvailable()) {
      return new NullProviderAgent();
    }

    String agentClassName =
        System.getProperty(PROVIDER_AGENT_NAME_PROPERTY, DEFAULT_PROVIDER_AGENT_NAME);
    try {
      Class<? extends ProviderAgent> agentClass =
          ClassPathLoader.getLatest().forName(agentClassName).asSubclass(ProviderAgent.class);

      ProviderAgent providerAgent = agentClass.newInstance();
      if (DEFAULT_PROVIDER_AGENT_NAME.equals(providerAgent.getClass().getName())) {
        LOGGER.info("Using {} by default for service {}", providerAgent.getClass().getName(),
            ProviderAgent.class.getName());
      } else {
        LOGGER.info("Using {} from System Property {} for service {}",
            providerAgent.getClass().getName(), PROVIDER_AGENT_NAME_PROPERTY,
            ProviderAgent.class.getName());
      }

      return providerAgent;

    } catch (ClassNotFoundException | ClassCastException | InstantiationException
        | IllegalAccessException e) {
      LOGGER.warn("Unable to create ProviderAgent of type {}", agentClassName, e);
    }
    LOGGER.info("Using {} for service {}", NullProviderAgent.class.getName(),
        PROVIDER_AGENT_NAME_PROPERTY, ProviderAgent.class.getName());
    return new NullProviderAgent();
  }

  interface AvailabilityChecker {
    boolean isAvailable();
  }

}
