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

import io.micrometer.core.instrument.Counter;
import org.assertj.core.api.Condition;

/**
 * Assertions for {@link Counter}s.
 */
public class CounterAssert extends AbstractMeterAssert<CounterAssert, Counter> {

  /**
   * Creates an assertion to evaluate the given counter.
   *
   * @param counter the counter to evaluate
   */
  CounterAssert(Counter counter) {
    super(counter, CounterAssert.class);
  }

  /**
   * Verifies that the counter has a count that satisfies the given condition.
   *
   * @param condition the criteria against which to evaluate the counter's count
   * @return this assertion object
   * @throws AssertionError if the counter is {@code null}
   * @throws AssertionError if the condition rejects the counter's count
   */
  public CounterAssert hasCount(Condition<? super Double> condition) {
    isNotNull();
    double count = actual.count();
    if (!condition.matches(count)) {
      failWithMessage("Expected counter to have count <%s> but count was <%s>", condition, count);
    }
    return myself;
  }
}
