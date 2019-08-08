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

import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Gauge;
import org.assertj.core.api.Condition;
import org.junit.Test;

public class GaugeAssertTest {
  @SuppressWarnings("unchecked")
  private final Condition<Double> valueCondition = mock(Condition.class, "value condition");
  private final Gauge gauge = mock(Gauge.class);

  @Test
  public void hasValue_doesNotThrow_ifConditionAcceptsValue() {
    double acceptableCount = 92.0;

    when(gauge.value()).thenReturn(acceptableCount);
    when(valueCondition.matches(acceptableCount)).thenReturn(true);

    assertThatCode(() -> assertThat(gauge).hasValue(valueCondition))
        .doesNotThrowAnyException();
  }

  @Test
  public void hasValue_failsDescriptively_ifConditionRejectsValue() {
    double unacceptableCount = 92.0;

    when(gauge.value()).thenReturn(unacceptableCount);
    when(valueCondition.matches(unacceptableCount)).thenReturn(false);

    assertThatThrownBy(() -> assertThat(gauge).hasValue(valueCondition))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(valueCondition.toString())
        .hasMessageContaining(String.valueOf(unacceptableCount));
  }
}
