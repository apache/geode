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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.Before;
import org.junit.Test;

public class GeodeObservationSupportTest {
  private final ObservationRegistry observationRegistry = ObservationRegistry.create();
  private final RecordingObservationHandler observationHandler = new RecordingObservationHandler();

  @Before
  public void setUp() {
    observationRegistry.observationConfig().observationHandler(observationHandler);
  }

  @Test
  public void observeRunnable_startsAndStopsObservation() {
    GeodeObservationSupport.observe(observationRegistry, "geode.test.operation",
        observation -> observation.lowCardinalityKeyValue("operation", "test"),
        () -> {
        });

    assertThat(observationHandler.events)
        .containsExactly("start:geode.test.operation", "stop:geode.test.operation");
    assertThat(observationHandler.lowCardinalityKeyValue("operation"))
        .isEqualTo("test");
  }

  @Test
  public void observeRunnable_recordsErrorAndStopsObservation() {
    RuntimeException failure = new RuntimeException("expected failure");

    assertThatThrownBy(() -> GeodeObservationSupport.observe(observationRegistry,
        "geode.test.operation", observation -> {
        }, () -> {
          throw failure;
        })).isSameAs(failure);

    assertThat(observationHandler.events)
        .containsExactly("start:geode.test.operation", "error:geode.test.operation",
            "stop:geode.test.operation");
    assertThat(observationHandler.error)
        .isSameAs(failure);
  }

  @Test
  public void observeSupplier_returnsSupplierValue() {
    String value = GeodeObservationSupport.observe(observationRegistry, "geode.test.operation",
        observation -> {
        }, () -> "result");

    assertThat(value)
        .isEqualTo("result");
    assertThat(observationHandler.events)
        .containsExactly("start:geode.test.operation", "stop:geode.test.operation");
  }

  @Test
  public void observeSupplier_recordsErrorAndStopsObservation() {
    RuntimeException failure = new RuntimeException("expected failure");

    assertThatThrownBy(() -> GeodeObservationSupport.observe(observationRegistry,
        "geode.test.operation", observation -> {
        }, () -> {
          throw failure;
        })).isSameAs(failure);

    assertThat(observationHandler.events)
        .containsExactly("start:geode.test.operation", "error:geode.test.operation",
            "stop:geode.test.operation");
    assertThat(observationHandler.error)
        .isSameAs(failure);
  }

  private static class RecordingObservationHandler
      implements ObservationHandler<Observation.Context> {
    private final List<String> events = new ArrayList<>();
    private Observation.Context context;
    private Throwable error;

    @Override
    public void onStart(Observation.Context context) {
      this.context = context;
      events.add("start:" + context.getName());
    }

    @Override
    public void onError(Observation.Context context) {
      error = context.getError();
      events.add("error:" + context.getName());
    }

    @Override
    public void onStop(Observation.Context context) {
      events.add("stop:" + context.getName());
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
      return true;
    }

    private String lowCardinalityKeyValue(String key) {
      return context.getLowCardinalityKeyValues().stream()
          .filter(keyValue -> keyValue.getKey().equals(key))
          .findFirst()
          .map(keyValue -> keyValue.getValue())
          .orElse(null);
    }
  }
}
