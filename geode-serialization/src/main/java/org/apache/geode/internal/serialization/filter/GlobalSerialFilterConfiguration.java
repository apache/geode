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
package org.apache.geode.internal.serialization.filter;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedClassNames;
import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedSerializablesServices;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;

import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Implementation of {@code FilterConfiguration} that delegates to an {@code ObjectInputFilterApi}.
 */
class GlobalSerialFilterConfiguration implements FilterConfiguration {

  private static final Logger LOGGER = LogService.getLogger();

  private final SerializableObjectConfig serializableObjectConfig;
  private final FilterPatternFactory filterPatternFactory;
  private final Supplier<Set<String>> sanctionedClassesSupplier;
  private final Consumer<String> logger;
  private final GlobalSerialFilterFactory globalSerialFilterFactory;

  /**
   * Constructs instance with collaborators.
   */
  GlobalSerialFilterConfiguration(SerializableObjectConfig serializableObjectConfig) {
    this(serializableObjectConfig,
        new DefaultFilterPatternFactory(),
        () -> loadSanctionedClassNames(loadSanctionedSerializablesServices()),
        LOGGER::info,
        (pattern, sanctionedClasses) -> new DelegatingGlobalSerialFilterFactory()
            .create(pattern, sanctionedClasses));
  }

  @TestOnly
  GlobalSerialFilterConfiguration(SerializableObjectConfig serializableObjectConfig,
      Consumer<String> logger, GlobalSerialFilterFactory globalSerialFilterFactory) {
    this(serializableObjectConfig,
        new DefaultFilterPatternFactory(),
        () -> loadSanctionedClassNames(loadSanctionedSerializablesServices()),
        logger,
        globalSerialFilterFactory);
  }

  private GlobalSerialFilterConfiguration(
      SerializableObjectConfig serializableObjectConfig,
      FilterPatternFactory filterPatternFactory,
      Supplier<Set<String>> sanctionedClassesSupplier,
      Consumer<String> logger,
      GlobalSerialFilterFactory globalSerialFilterFactory) {
    this.serializableObjectConfig = serializableObjectConfig;
    this.filterPatternFactory = filterPatternFactory;
    this.sanctionedClassesSupplier = sanctionedClassesSupplier;
    this.logger = logger;
    this.globalSerialFilterFactory = globalSerialFilterFactory;
  }

  @Override
  public boolean configure() {
    try {
      // enable validate-serializable-objects
      serializableObjectConfig.setValidateSerializableObjects(true);

      // create a GlobalSerialFilter
      String pattern = filterPatternFactory
          .create(serializableObjectConfig.getSerializableObjectFilterIfEnabled());
      Set<String> sanctionedClasses = sanctionedClassesSupplier.get();
      GlobalSerialFilter globalSerialFilter =
          globalSerialFilterFactory.create(pattern, sanctionedClasses);

      // invoke setFilter on GlobalSerialFilter to set the process-wide filter
      globalSerialFilter.setFilter();

      // log statement that filter is now configured
      logger.accept("Global serial filter is now configured.");
      return true;

    } catch (UnsupportedOperationException e) {
      if (hasRootCauseWithMessage(e, IllegalStateException.class,
          "Serial filter can only be set once")) {

        // log statement that filter was already configured
        logger.accept("Global serial filter is already configured.");
      }
      return false;
    }
  }

  private static boolean hasRootCauseWithMessage(Throwable throwable,
      Class<? extends Throwable> causeClass, String message) {
    Throwable rootCause = getRootCause(throwable);
    return isInstanceOf(rootCause, causeClass) && hasMessage(rootCause, message);
  }

  private static boolean isInstanceOf(Throwable throwable, Class<? extends Throwable> causeClass) {
    return nonNull(throwable) && throwable.getClass().equals(causeClass);
  }

  private static boolean hasMessage(Throwable throwable, String message) {
    return nonNull(throwable) && throwable.getMessage().equalsIgnoreCase(message);
  }

  /**
   * Creates filter pattern string including the specified optional
   * {@code serializable-object-filter}.
   */
  @FunctionalInterface
  interface FilterPatternFactory {

    String create(String optionalSerializableObjectFilter);
  }

  /**
   * Default implementation of {@code FilterPatternFactory}.
   */
  static class DefaultFilterPatternFactory implements FilterPatternFactory {

    @Override
    public String create(String optionalSerializableObjectFilter) {
      return new SanctionedSerializablesFilterPattern()
          .append(optionalSerializableObjectFilter)
          .pattern();
    }
  }
}
