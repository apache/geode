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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.StatisticsImpl.StatisticsLogger;

/**
 * Unit tests for {@link StatisticsImpl}.
 */
public class StatisticsImplTest {

  // arbitrary values for constructing a StatisticsImpl
  private static final String ANY_TEXT_ID = null;
  private static final long ANY_NUMERIC_ID = 0;
  private static final long ANY_UNIQUE_ID = 0;
  private static final int ANY_OS_STAT_FLAGS = 0;

  private StatisticsManager statisticsManager;
  private StatisticsTypeImpl statisticsType;

  private StatisticsImpl statistics;

  @Before
  public void createStats() {
    statisticsManager = mock(StatisticsManager.class);

    statisticsType = mock(StatisticsTypeImpl.class);
    when(statisticsType.isValidLongId(anyInt())).thenReturn(true);
    when(statisticsType.isValidDoubleId(anyInt())).thenReturn(true);

    statistics = new SimpleStatistics(statisticsType, ANY_TEXT_ID, ANY_NUMERIC_ID, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager);
  }

  @Test
  public void invokeIntSuppliersShouldUpdateStats() {
    IntSupplier intSupplier = mock(IntSupplier.class);
    when(intSupplier.getAsInt()).thenReturn(23);
    statistics.setIntSupplier(4, intSupplier);
    assertThat(statistics.invokeSuppliers()).isEqualTo(0);

    verify(intSupplier).getAsInt();
    assertThat(statistics.getInt(4)).isEqualTo(23);
  }

  @Test
  public void invokeLongSuppliersShouldUpdateStats() {
    LongSupplier longSupplier = mock(LongSupplier.class);
    when(longSupplier.getAsLong()).thenReturn(23L);
    statistics.setLongSupplier(4, longSupplier);
    assertThat(statistics.invokeSuppliers()).isEqualTo(0);

    verify(longSupplier).getAsLong();
    assertThat(statistics.getLong(4)).isEqualTo(23L);
  }

  @Test
  public void invokeDoubleSuppliersShouldUpdateStats() {
    DoubleSupplier doubleSupplier = mock(DoubleSupplier.class);
    when(doubleSupplier.getAsDouble()).thenReturn(23.3);
    statistics.setDoubleSupplier(4, doubleSupplier);
    assertThat(statistics.invokeSuppliers()).isEqualTo(0);

    verify(doubleSupplier).getAsDouble();
    assertThat(statistics.getDouble(4)).isEqualTo(23.3);
  }

  @Test
  public void getSupplierCountShouldReturnCorrectCount() {
    IntSupplier intSupplier = mock(IntSupplier.class);
    statistics.setIntSupplier(4, intSupplier);
    assertThat(statistics.getSupplierCount()).isEqualTo(1);
  }

  @Test
  public void invokeSuppliersShouldCatchSupplierErrorsAndReturnCount() {
    IntSupplier throwingSupplier = mock(IntSupplier.class);
    when(throwingSupplier.getAsInt()).thenThrow(NullPointerException.class);
    statistics.setIntSupplier(4, throwingSupplier);
    assertThat(statistics.invokeSuppliers()).isEqualTo(1);

    verify(throwingSupplier).getAsInt();
  }

  @Test
  public void invokeSuppliersShouldLogErrorOnlyOnce() {
    StatisticsLogger statisticsLogger = mock(StatisticsLogger.class);
    statistics = new SimpleStatistics(statisticsType, ANY_TEXT_ID, ANY_NUMERIC_ID, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager, statisticsLogger);

    IntSupplier throwingSupplier = mock(IntSupplier.class);
    when(throwingSupplier.getAsInt()).thenThrow(NullPointerException.class);
    statistics.setIntSupplier(4, throwingSupplier);
    assertThat(statistics.invokeSuppliers()).isEqualTo(1);

    // String message, Object p0, Object p1, Object p2
    verify(statisticsLogger).logWarning(anyString(), isNull(), anyInt(),
        isA(NullPointerException.class));

    assertThat(statistics.invokeSuppliers()).isEqualTo(1);

    // Make sure the logger isn't invoked again
    verify(statisticsLogger).logWarning(anyString(), isNull(), anyInt(),
        isA(NullPointerException.class));
  }

  @Test
  public void badSupplierParamShouldThrowError() {
    IntSupplier intSupplier = mock(IntSupplier.class);
    when(intSupplier.getAsInt()).thenReturn(23);
    when(statisticsType.isValidLongId(anyInt())).thenReturn(false);

    Throwable thrown = catchThrowable(() -> statistics.setIntSupplier(23, intSupplier));

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void nonEmptyTextId_usesGivenTextId() {
    String nonEmptyTextId = "non-empty-text-id";

    statistics = new SimpleStatistics(statisticsType, nonEmptyTextId, ANY_NUMERIC_ID, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager);

    assertThat(statistics.getTextId()).isEqualTo(nonEmptyTextId);
  }

  @Test
  public void nullTextId_usesNameFromStatisticsManager() {
    String nameFromStatisticsManager = "statistics-manager-name";
    when(statisticsManager.getName()).thenReturn(nameFromStatisticsManager);
    String nullTextId = null;

    statistics = new SimpleStatistics(statisticsType, nullTextId, ANY_NUMERIC_ID, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager);

    assertThat(statistics.getTextId()).isEqualTo(nameFromStatisticsManager);
  }

  @Test
  public void emptyTextId_usesNameFromStatisticsManager() {
    String nameFromStatisticsManager = "statistics-manager-name";
    when(statisticsManager.getName()).thenReturn(nameFromStatisticsManager);
    String emptyTextId = "";

    statistics = new SimpleStatistics(statisticsType, emptyTextId, ANY_NUMERIC_ID, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager);

    assertThat(statistics.getTextId()).isEqualTo(nameFromStatisticsManager);
  }

  @Test
  public void positiveNumericId_usesGivenNumericId() {
    int positiveNumericId = 21;

    statistics = new SimpleStatistics(statisticsType, ANY_TEXT_ID, positiveNumericId, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager);

    assertThat(statistics.getNumericId()).isEqualTo(positiveNumericId);
  }

  @Test
  public void negativeNumericId_usesGivenNumericId() {
    int negativeNumericId = -21;

    statistics = new SimpleStatistics(statisticsType, ANY_TEXT_ID, negativeNumericId, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager);

    assertThat(statistics.getNumericId()).isEqualTo(negativeNumericId);
  }

  @Test
  public void zeroNumericId_usesPidFromStatisticsManager() {
    int pidFromStatisticsManager = 42;
    when(statisticsManager.getPid()).thenReturn(pidFromStatisticsManager);
    int zeroNumericId = 0;

    statistics = new SimpleStatistics(statisticsType, ANY_TEXT_ID, zeroNumericId, ANY_UNIQUE_ID,
        ANY_OS_STAT_FLAGS, statisticsManager);

    assertThat(statistics.getNumericId()).isEqualTo(pidFromStatisticsManager);
  }

  private static class SimpleStatistics extends StatisticsImpl {

    private final Map<Number, Number> values = new HashMap<>();

    SimpleStatistics(StatisticsType type, String textId, long numericId, long uniqueId,
        int osStatFlags, StatisticsManager statisticsManager) {
      super(type, textId, numericId, uniqueId, osStatFlags, statisticsManager);
    }

    SimpleStatistics(StatisticsType type, String textId, long numericId, long uniqueId,
        int osStatFlags, StatisticsManager statisticsManager, StatisticsLogger statisticsLogger) {
      super(type, textId, numericId, uniqueId, osStatFlags, statisticsManager, statisticsLogger);
    }

    @Override
    public boolean isAtomic() {
      return false;
    }

    @Override
    protected void _setLong(int offset, long value) {
      values.put(offset, value);
    }

    @Override
    protected void _setDouble(int offset, double value) {
      values.put(offset, value);
    }

    @Override
    protected long _getLong(int offset) {
      return (long) values.get(offset);
    }

    @Override
    protected double _getDouble(int offset) {
      return (double) values.get(offset);
    }

    @Override
    protected void _incLong(int offset, long delta) {
      values.put(offset, (long) values.get(delta) + 1);
    }

    @Override
    protected void _incDouble(int offset, double delta) {
      values.put(offset, (double) values.get(delta) + 1);
    }
  }
}
