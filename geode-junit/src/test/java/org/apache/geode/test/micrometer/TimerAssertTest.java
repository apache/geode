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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Timer;
import org.assertj.core.api.Condition;
import org.junit.Test;

public class TimerAssertTest {
  @SuppressWarnings("unchecked")
  private final Condition<Long> countCondition = mock(Condition.class, "count condition");
  @SuppressWarnings("unchecked")
  private final Condition<Double> totalTimeCondition = mock(Condition.class, "time condition");
  private final Timer timer = mock(Timer.class);

  @Test
  public void hasCount_doesNotThrow_ifConditionAcceptsCount() {
    long acceptableCount = 92L;

    when(timer.count()).thenReturn(acceptableCount);
    when(countCondition.matches(acceptableCount)).thenReturn(true);

    assertThatCode(() -> assertThat(timer).hasCount(countCondition))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasCount_failsDescriptively_ifConditionRejectsCount() {
    long unacceptableCount = 92L;

    when(timer.count()).thenReturn(unacceptableCount);
    when(countCondition.matches(unacceptableCount)).thenReturn(false);

    assertThatThrownBy(() -> assertThat(timer).hasCount(countCondition))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(countCondition.toString())
        .hasMessageContaining(String.valueOf(unacceptableCount));
  }

  @Test
  public void hasTotalTime_doesNotThrow_ifConditionAcceptsTotalTime() {
    double acceptableCount = 92.0;
    TimeUnit timeUnit = SECONDS;

    when(timer.totalTime(timeUnit)).thenReturn(acceptableCount);
    when(totalTimeCondition.matches(acceptableCount)).thenReturn(true);

    assertThatCode(() -> assertThat(timer).hasTotalTime(timeUnit, totalTimeCondition))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasTotalTime_failsDescriptively_ifConditionRejectsTotalTime() {
    double unacceptableCount = 92.0;
    TimeUnit timeUnit = SECONDS;

    when(timer.totalTime(timeUnit)).thenReturn(unacceptableCount);
    when(totalTimeCondition.matches(unacceptableCount)).thenReturn(false);

    assertThatThrownBy(() -> assertThat(timer).hasTotalTime(timeUnit, totalTimeCondition))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(totalTimeCondition.toString())
        .hasMessageContaining(String.valueOf(timeUnit))
        .hasMessageContaining(String.valueOf(unacceptableCount));
  }
}
