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
package org.apache.geode.metrics.internal;

import java.util.Collection;
import java.util.HashSet;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Binds a collection of meter binders to a meter registry.
 */
public class CompoundMeterBinder implements CloseableMeterBinder {
  private final Logger logger;
  private final Collection<MeterBinder> meterBinders = new HashSet<>();

  public CompoundMeterBinder(Collection<MeterBinder> binders) {
    this(LogService.getLogger(), binders);
  }

  @VisibleForTesting
  CompoundMeterBinder(Logger logger, Collection<MeterBinder> binders) {
    this.logger = logger;
    meterBinders.addAll(binders);
  }

  /**
   * Binds each constituent meter binder to the given registry and logs each thrown exception.
   */
  @Override
  public void bindTo(MeterRegistry registry) {
    meterBinders.forEach(binder -> bind(registry, binder));
  }

  /**
   * Closes each closeable meter binder and logs each thrown exception.
   */
  @Override
  public void close() {
    meterBinders.stream()
        .filter(AutoCloseable.class::isInstance)
        .map(AutoCloseable.class::cast)
        .forEach(this::close);
  }

  private void bind(MeterRegistry registry, MeterBinder binder) {
    try {
      binder.bindTo(registry);
    } catch (Exception thrown) {
      logger.warn("Exception while binding meter binder " + binder, thrown);
    }
  }

  private void close(AutoCloseable binder) {
    try {
      binder.close();
    } catch (Exception thrown) {
      logger.warn("Exception while closing meter binder " + binder, thrown);
    }
  }
}
