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

import static org.apache.geode.test.assertj.Conditions.equalTo;

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Timer;
import org.assertj.core.api.Condition;

/**
 * Assertions for {@link Timer}s.
 */
public class TimerAssert extends AbstractMeterAssert<TimerAssert, Timer> {
  /**
   * Creates an assertion to evaluate the given timer.
   *
   * @param timer the timer to evaluate
   */
  TimerAssert(Timer timer) {
    super(timer, TimerAssert.class);
  }

  /**
   * Verifies that the timer's count satisfies the given condition.
   *
   * @param count the expected value of the timer's count
   * @return this assertion object
   * @throws AssertionError if the timer is {@code null}
   * @throws AssertionError if the timer's count is not equal to the given count
   */
  public TimerAssert hasCount(long count) {
    return hasCount(equalTo(count));
  }

  /**
   * Verifies that the timer's count satisfies the given condition.
   *
   * @param condition the criteria against which to evaluate the timer's count
   * @return this assertion object
   * @throws AssertionError if the timer is {@code null}
   * @throws AssertionError if the condition rejects the timer's count
   */
  public TimerAssert hasCount(Condition<? super Long> condition) {
    isNotNull();
    long count = actual.count();
    if (!condition.matches(count)) {
      failWithMessage("Expected timer to have count <%s> but count was <%s>", condition, count);
    }
    return myself;
  }

  /**
   * Verifies that the timer's total time satisfies the given condition.
   *
   * @param timeUnit the time unit to which to convert the total time before evaluating
   * @param totalTime the expected value of the timer's total time
   * @return this assertion object
   * @throws AssertionError if the timer is {@code null}
   * @throws AssertionError if the timer's converted total time is not equal to the given total time
   */
  public TimerAssert hasTotalTime(TimeUnit timeUnit, double totalTime) {
    return hasTotalTime(timeUnit, equalTo(totalTime));
  }

  /**
   * Verifies that the timer's total time satisfies the given condition.
   *
   * @param timeUnit the time unit to which to convert the total time before evaluating
   * @param condition the criteria against which to evaluate the timer's total time
   * @return this assertion object
   * @throws AssertionError if the timer is {@code null}
   * @throws AssertionError if the condition rejects the timer's total time
   */
  public TimerAssert hasTotalTime(TimeUnit timeUnit, Condition<? super Double> condition) {
    isNotNull();
    double totalTime = actual.totalTime(timeUnit);
    if (!condition.matches(totalTime)) {
      failWithMessage("Expected timer to have total time (%s) <%s> but total time was <%s>",
          timeUnit, condition, totalTime);
    }
    return myself;
  }
}
