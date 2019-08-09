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

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.logging.ProviderAgentLoader.AvailabilityChecker;

class DefaultProviderChecker implements AvailabilityChecker {

  /**
   * The default {@code ProviderAgent} is {@code Log4jAgent}.
   */
  static final String DEFAULT_PROVIDER_AGENT_NAME =
      "org.apache.geode.internal.logging.log4j.Log4jAgent";

  static final String DEFAULT_PROVIDER_CLASS_NAME =
      "org.apache.logging.log4j.core.impl.Log4jContextFactory";

  private final Supplier<Class> contextFactoryClassSupplier;
  private final Function<String, Boolean> isClassLoadableFunction;
  private final Logger logger;

  DefaultProviderChecker() {
    this(() -> LogManager.getFactory().getClass(), DefaultProviderChecker::isClassLoadable,
        StatusLogger.getLogger());
  }

  @VisibleForTesting
  DefaultProviderChecker(Supplier<Class> contextFactoryClassSupplier,
      Function<String, Boolean> isClassLoadableFunction,
      Logger logger) {
    this.contextFactoryClassSupplier = contextFactoryClassSupplier;
    this.isClassLoadableFunction = isClassLoadableFunction;
    this.logger = logger;
  }

  @Override
  public boolean isAvailable() {
    if (!isClassLoadableFunction.apply(DEFAULT_PROVIDER_CLASS_NAME)) {
      logger.info("Unable to find Log4j Core.");
      return false;
    }

    boolean usingLog4jProvider =
        DEFAULT_PROVIDER_CLASS_NAME.equals(contextFactoryClassSupplier.get().getName());
    String message = "Log4j Core is available "
        + (usingLog4jProvider ? "and using" : "but not using") + " Log4jProvider.";
    logger.info(message);
    return usingLog4jProvider;
  }

  @VisibleForTesting
  static boolean isClassLoadable(String className) {
    try {
      ClassPathLoader.getLatest().forName(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

}
