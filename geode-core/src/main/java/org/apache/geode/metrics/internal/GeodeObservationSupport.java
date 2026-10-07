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

import java.util.function.Consumer;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * Utility methods for creating Geode observations with consistent error and stop handling.
 */
public final class GeodeObservationSupport {

  private GeodeObservationSupport() {}

  public static void observe(ObservationRegistry observationRegistry, String name,
      Consumer<Observation> observationConfigurer, ObservationRunnable runnable) {
    observe(observationRegistry, name, observationConfigurer, () -> {
      runnable.run();
      return null;
    });
  }

  public static <T> T observe(ObservationRegistry observationRegistry, String name,
      Consumer<Observation> observationConfigurer, ObservationSupplier<T> supplier) {
    Observation observation = startObservation(observationRegistry, name, observationConfigurer);
    try (Observation.Scope ignored = observation.openScope()) {
      return supplier.get();
    } catch (Throwable thrown) {
      observation.error(thrown);
      return sneakyThrow(thrown);
    } finally {
      observation.stop();
    }
  }

  public static Observation startObservation(ObservationRegistry observationRegistry, String name,
      Consumer<Observation> observationConfigurer) {
    Observation observation = observationRegistry == null
        ? Observation.NOOP
        : Observation.createNotStarted(name, observationRegistry);
    observationConfigurer.accept(observation);
    return observation.start();
  }

  @FunctionalInterface
  public interface ObservationRunnable {
    void run() throws Exception;
  }

  @FunctionalInterface
  public interface ObservationSupplier<T> {
    T get() throws Exception;
  }

  @SuppressWarnings("unchecked")
  private static <T, E extends Throwable> T sneakyThrow(Throwable thrown) throws E {
    throw (E) thrown;
  }
}
