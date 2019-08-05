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
package org.apache.geode.test.micrometer;

import io.micrometer.core.instrument.Gauge;
import org.assertj.core.api.Condition;

/**
 * Assertions for {@link Gauge}s.
 */
public class GaugeAssert extends AbstractMeterAssert<GaugeAssert, Gauge> {
  /**
   * Creates an assertion to evaluate the given gauge.
   *
   * @param gauge the gauge to evaluate
   */
  GaugeAssert(Gauge gauge) {
    super(gauge, GaugeAssert.class);
  }

  /**
   * Verifies that the gauge has a value that satisfies the given condition.
   *
   * @param condition the criteria against which to evaluate the gauge's count
   * @return this assertion object
   * @throws AssertionError if the gauge is {@code null}
   * @throws AssertionError if the condition rejects the gauge's value
   */
  public GaugeAssert hasValue(Condition<? super Double> condition) {
    double value = actual.value();
    if (!condition.matches(value)) {
      failWithMessage("Expected gauge to have value <%s> but value was <%s>", condition, value);
    }
    return myself;
  }
}
