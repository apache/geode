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
package org.apache.geode.internal.cache.execute.metrics;

import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.internal.InternalEntity;

public class TimingFunctionInstrumentor implements FunctionInstrumentor {

  private final Supplier<MeterRegistry> meterRegistrySupplier;

  public TimingFunctionInstrumentor() {
    this(new MeterRegistrySupplier());
  }

  @VisibleForTesting
  TimingFunctionInstrumentor(Supplier<MeterRegistry> meterRegistrySupplier) {
    this.meterRegistrySupplier = meterRegistrySupplier;
  }

  @Override
  public <T> Function<T> instrument(Function<T> function) {
    MeterRegistry meterRegistry = meterRegistrySupplier.get();

    if (meterRegistry == null || function instanceof InternalEntity) {
      return function;
    }

    return new TimingFunction<>(function, meterRegistry);
  }

  @Override
  public void close(Function<?> function) {
    if (function instanceof TimingFunction) {
      try {
        ((TimingFunction) function).close();
      } catch (Exception ignored) {
        // ignored
      }
    }
  }
}
