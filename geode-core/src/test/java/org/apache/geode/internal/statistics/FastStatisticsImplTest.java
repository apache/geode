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

package org.apache.geode.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.StatisticDescriptor;

public class FastStatisticsImplTest {

  private FastStatisticsImpl stats;

  @Before
  public void setup() {
    StatisticDescriptor intCounterDescriptor = mock(StatisticDescriptor.class);
    when(intCounterDescriptor.getId()).thenReturn(0);
    when(intCounterDescriptor.getType()).thenReturn((Class) int.class);
    when(intCounterDescriptor.isCounter()).thenReturn(true);

    StatisticDescriptor longCounterDescriptor = mock(StatisticDescriptor.class);
    when(longCounterDescriptor.getId()).thenReturn(0);
    when(longCounterDescriptor.getType()).thenReturn((Class) long.class);
    when(longCounterDescriptor.isCounter()).thenReturn(true);

    StatisticDescriptor doubleCounterDescriptor = mock(StatisticDescriptor.class);
    when(doubleCounterDescriptor.getId()).thenReturn(0);
    when(doubleCounterDescriptor.getType()).thenReturn((Class) double.class);
    when(doubleCounterDescriptor.isCounter()).thenReturn(true);

    StatisticDescriptor intGaugeDescriptor = mock(StatisticDescriptor.class);
    when(intGaugeDescriptor.getId()).thenReturn(1);
    when(intGaugeDescriptor.getType()).thenReturn((Class) int.class);
    when(intGaugeDescriptor.isCounter()).thenReturn(false);

    StatisticDescriptor longGaugeDescriptor = mock(StatisticDescriptor.class);
    when(longGaugeDescriptor.getId()).thenReturn(1);
    when(longGaugeDescriptor.getType()).thenReturn((Class) long.class);
    when(longGaugeDescriptor.isCounter()).thenReturn(false);

    StatisticDescriptor doubleGaugeDescriptor = mock(StatisticDescriptor.class);
    when(doubleGaugeDescriptor.getId()).thenReturn(1);
    when(doubleGaugeDescriptor.getType()).thenReturn((Class) double.class);
    when(doubleGaugeDescriptor.isCounter()).thenReturn(false);

    StatisticsTypeImpl type = mock(StatisticsTypeImpl.class);
    when(type.getIntStatCount()).thenReturn(2);
    when(type.getDoubleStatCount()).thenReturn(2);
    when(type.getLongStatCount()).thenReturn(2);
    when(type.getStatistics()).thenReturn(
        new StatisticDescriptor[] {intCounterDescriptor, longCounterDescriptor,
            doubleCounterDescriptor, intGaugeDescriptor, longGaugeDescriptor,
            doubleGaugeDescriptor});

    String textId = null;
    long numbericId = 0;
    long uniqueId = 0;
    int osStatFlags = 0;
    boolean atomicIncrements = false;
    StatisticsManager system = mock(StatisticsManager.class);

    stats = new FastStatisticsImpl(type, textId, numbericId, uniqueId, system);
  }

  @Test
  public void intCounterCanIncrement() {
    int expected = 0;

    for (int i = 0; i < 10; i++) {
      expected += 1;
      stats.incInt(0, 1);
    }
    assertThat(stats.getInt(0)).isEqualTo(expected);

    for (int i = 0; i < 10; i++) {
      expected -= 1;
      stats.incInt(0, -1);
    }
    assertThat(stats.getInt(0)).isEqualTo(expected);
  }

  @Test
  public void intCounterCantBeSet() {
    assertThatThrownBy(() -> stats.setInt(0, 1)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Counters cannot be set");
  }

  @Test
  public void intGaugeCanBeSet() {
    stats.setInt(1, 1);
    assertThat(stats.getInt(1)).isEqualTo(1);

    stats.setInt(1, Integer.MAX_VALUE);
    assertThat(stats.getInt(1)).isEqualTo(Integer.MAX_VALUE);

    stats.setInt(1, Integer.MIN_VALUE);
    assertThat(stats.getInt(1)).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void intGaugeCanIncrement() {
    int expected = 0;

    for (int i = 0; i < 10; i++) {
      expected += 1;
      stats.incInt(0, 1);
    }
    assertThat(stats.getInt(0)).isEqualTo(expected);

    for (int i = 0; i < 20; i++) {
      expected -= 1;
      stats.incInt(0, -1);
    }
    assertThat(stats.getInt(0)).isEqualTo(expected);
  }


  @Test
  public void longCounterCanIncrement() {
    long expected = 0;

    for (long i = 0; i < 10; i++) {
      expected += 1;
      stats.incLong(0, 1);
    }
    assertThat(stats.getLong(0)).isEqualTo(expected);

    for (long i = 0; i < 10; i++) {
      expected -= 1;
      stats.incLong(0, -1);
    }
    assertThat(stats.getLong(0)).isEqualTo(expected);
  }

  @Test
  public void longCounterCantBeSet() {
    assertThatThrownBy(() -> stats.setLong(0, 1)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Counters cannot be set");
  }

  @Test
  public void longGaugeCanBeSet() {
    stats.setLong(1, 1);
    assertThat(stats.getLong(1)).isEqualTo(1);

    stats.setLong(1, Long.MAX_VALUE);
    assertThat(stats.getLong(1)).isEqualTo(Long.MAX_VALUE);

    stats.setLong(1, Long.MIN_VALUE);
    assertThat(stats.getLong(1)).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  public void longGaugeCanIncrement() {
    long expected = 0;

    for (long i = 0; i < 10; i++) {
      expected += 1;
      stats.incLong(0, 1);
    }
    assertThat(stats.getLong(0)).isEqualTo(expected);

    for (long i = 0; i < 20; i++) {
      expected -= 1;
      stats.incLong(0, -1);
    }
    assertThat(stats.getLong(0)).isEqualTo(expected);
  }


  @Test
  public void doubleCounterCanIncrement() {
    double expected = 0;

    for (int i = 0; i < 10; i++) {
      expected += 0.1;
      stats.incDouble(0, 0.1);
    }
    assertThat(stats.getDouble(0)).isEqualTo(expected, Offset.offset(0.0));

    for (int i = 0; i < 20; i++) {
      expected -= 0.1;
      stats.incDouble(0, -0.1);
    }
    assertThat(stats.getDouble(0)).isEqualTo(expected, Offset.offset(0.0));
  }

  @Test
  public void doubleCounterCantBeSet() {
    double expected = 3.14;
    assertThatThrownBy(() -> stats.setDouble(0, expected))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Counters cannot be set");
  }

  @Test
  public void doubleGaugeCanBeSet() {
    stats.setDouble(1, 3.14159);
    assertThat(stats.getDouble(1)).isEqualTo(3.14159);

    stats.setDouble(1, Double.MAX_VALUE);
    assertThat(stats.getDouble(1)).isEqualTo(Double.MAX_VALUE);

    stats.setDouble(1, Double.MIN_VALUE);
    assertThat(stats.getDouble(1)).isEqualTo(Double.MIN_VALUE);
  }

  @Test
  public void doubleGaugeCanIncrement() {
    double expected = 0;

    for (int i = 0; i < 10; i++) {
      expected += 0.1;
      stats.incDouble(0, 0.1);
    }
    assertThat(stats.getDouble(0)).isEqualTo(expected, Offset.offset(0.0));

    for (int i = 0; i < 20; i++) {
      expected -= 0.1;
      stats.incDouble(0, -0.1);
    }
    assertThat(stats.getDouble(0)).isEqualTo(expected, Offset.offset(0.0));
  }

}
