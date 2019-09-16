/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2
 * .0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express
 * or implied. See the License for the specific language governing permissions and limitations
 * under
 * the License.
 */
package org.apache.geode.test.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;

/**
 * Entry point for assertions for Micrometer meters.
 */
public class MicrometerAssertions {

  /**
   * Creates an assertion to evaluate the given meter.
   *
   * @param meter the meter to evaluate
   * @return the created assertion object
   */
  public static MeterAssert assertThat(Meter meter) {
    return new MeterAssert(meter);
  }

  /**
   * Creates an assertion to evaluate the given counter.
   *
   * @param counter the counter to evaluate
   * @return the created assertion object
   */
  public static CounterAssert assertThat(Counter counter) {
    return new CounterAssert(counter);
  }

  /**
   * Creates an assertion to evaluate the given gauge.
   *
   * @param gauge the gauge to evaluate
   * @return the created assertion object
   */
  public static GaugeAssert assertThat(Gauge gauge) {
    return new GaugeAssert(gauge);
  }

  /**
   * Creates an assertion to evaluate the given timer.
   *
   * @param timer the timer to evaluate
   * @return the created assertion object
   */
  public static TimerAssert assertThat(Timer timer) {
    return new TimerAssert(timer);
  }
}
