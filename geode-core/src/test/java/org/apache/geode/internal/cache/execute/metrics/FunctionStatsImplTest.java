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
package org.apache.geode.internal.cache.execute.metrics;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.geode.internal.cache.execute.metrics.FunctionStatsImpl.functionExecutionExceptionsId;
import static org.apache.geode.internal.cache.execute.metrics.FunctionStatsImpl.functionExecutionsCompletedId;
import static org.apache.geode.internal.cache.execute.metrics.FunctionStatsImpl.functionExecutionsCompletedProcessingTimeId;
import static org.apache.geode.internal.cache.execute.metrics.FunctionStatsImpl.functionExecutionsHasResultCompletedProcessingTimeId;
import static org.apache.geode.internal.cache.execute.metrics.FunctionStatsImpl.functionExecutionsHasResultRunningId;
import static org.apache.geode.internal.cache.execute.metrics.FunctionStatsImpl.functionExecutionsRunningId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.Statistics;

public class FunctionStatsImplTest {
  private static final String FUNCTION_ID = "functionId";

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private Statistics statistics;

  @Mock
  private FunctionServiceStats functionServiceStats;

  private MeterRegistry meterRegistry;

  @Before
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void constructor_registersSuccessTimer() {
    new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    assertThat(successTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=true", FUNCTION_ID)
        .isNotNull();
    assertThat(successTimer().getId().getDescription())
        .as("success timer description")
        .isEqualTo("Count and total time of successful function executions");
  }

  @Test
  public void constructor_registersFailureTimer() {
    new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    assertThat(failureTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=false", FUNCTION_ID)
        .isNotNull();
    assertThat(failureTimer().getId().getDescription())
        .as("failure timer description")
        .isEqualTo("Count and total time of failed function executions");
  }

  @Test
  public void constructor_throws_ifMeterRegistryIsNull() {
    Throwable thrown = catchThrowable(
        () -> new FunctionStatsImpl(FUNCTION_ID, null, statistics, functionServiceStats));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void recordSuccessfulExecution_incrementsSuccessTimerCount() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    functionStats.recordSuccessfulExecution(5, NANOSECONDS, false);

    assertThat(successTimer().count())
        .as("Success timer count")
        .isEqualTo(1);
  }

  @Test
  public void recordSuccessfulExecution_incrementsSuccessTimerTotalTime() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    long elapsedNanos = 5;
    functionStats.recordSuccessfulExecution(elapsedNanos, NANOSECONDS, false);

    assertThat(successTimer().totalTime(NANOSECONDS))
        .as("Success timer total time")
        .isEqualTo(elapsedNanos);
  }

  @Test
  public void recordSuccessfulExecution_noResult_clockStatsDisabled_incrementsStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats, 0L,
            false);

    functionStats.recordSuccessfulExecution(5, NANOSECONDS, false);

    verify(statistics)
        .incInt(functionExecutionsCompletedId(), 1);
    verify(statistics)
        .incInt(functionExecutionsRunningId(), -1);
    verify(statistics, never())
        .incLong(eq(functionExecutionsCompletedProcessingTimeId()), anyLong());
    verify(statistics, never())
        .incInt(eq(functionExecutionsHasResultRunningId()), anyInt());
    verify(statistics, never())
        .incLong(eq(functionExecutionsHasResultCompletedProcessingTimeId()), anyLong());
  }

  @Test
  public void recordSuccessfulExecution_hasResult_clockStatsDisabled_incrementsStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats, 0L,
            false);

    functionStats.recordSuccessfulExecution(5, NANOSECONDS, true);

    verify(statistics)
        .incInt(functionExecutionsCompletedId(), 1);
    verify(statistics)
        .incInt(functionExecutionsRunningId(), -1);
    verify(statistics, never())
        .incLong(eq(functionExecutionsCompletedProcessingTimeId()), anyLong());
    verify(statistics)
        .incInt(functionExecutionsHasResultRunningId(), -1);
    verify(statistics, never())
        .incLong(eq(functionExecutionsHasResultCompletedProcessingTimeId()), anyLong());
  }

  @Test
  public void recordSuccessfulExecution_noResult_clockStatsEnabled_incrementsStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats, 0L,
            true);

    long elapsedNanos = 5;
    functionStats.recordSuccessfulExecution(elapsedNanos, NANOSECONDS, false);

    verify(statistics)
        .incInt(functionExecutionsCompletedId(), 1);
    verify(statistics)
        .incInt(functionExecutionsRunningId(), -1);
    verify(statistics)
        .incLong(functionExecutionsCompletedProcessingTimeId(), elapsedNanos);
    verify(statistics, never())
        .incInt(eq(functionExecutionsHasResultRunningId()), anyInt());
    verify(statistics, never())
        .incLong(eq(functionExecutionsHasResultCompletedProcessingTimeId()), anyLong());
  }

  @Test
  public void recordSuccessfulExecution_hasResult_clockStatsEnabled_incrementsStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats, 0L,
            true);

    long elapsedNanos = 5;
    functionStats.recordSuccessfulExecution(elapsedNanos, NANOSECONDS, true);

    verify(statistics)
        .incInt(functionExecutionsCompletedId(), 1);
    verify(statistics)
        .incInt(functionExecutionsRunningId(), -1);
    verify(statistics)
        .incLong(functionExecutionsCompletedProcessingTimeId(), elapsedNanos);
    verify(statistics)
        .incInt(functionExecutionsHasResultRunningId(), -1);
    verify(statistics)
        .incLong(functionExecutionsHasResultCompletedProcessingTimeId(), elapsedNanos);
  }

  @Test
  public void recordSuccessfulExecution_incrementsAggregateStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    long elapsedNanos = 5;
    boolean haveResult = true;
    functionStats.recordSuccessfulExecution(elapsedNanos, NANOSECONDS, haveResult);

    verify(functionServiceStats)
        .endFunctionExecution(elapsedNanos, haveResult);
  }

  @Test
  public void recordFailedExecution_incrementsFailureTimerCount() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    long elapsedNanos = 5;
    functionStats.recordFailedExecution(elapsedNanos, NANOSECONDS, false);

    assertThat(failureTimer().count())
        .as("Failure timer count")
        .isEqualTo(1);
  }

  @Test
  public void recordFailedExecution_incrementsFailureTimerTotalTime() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    long elapsedNanos = 5;
    functionStats.recordFailedExecution(elapsedNanos, NANOSECONDS, false);

    assertThat(failureTimer().totalTime(NANOSECONDS))
        .as("Failure timer total time")
        .isEqualTo(elapsedNanos);
  }

  @Test
  public void recordFailedExecution_noResult_incrementsStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    functionStats.recordFailedExecution(5, NANOSECONDS, false);

    verify(statistics)
        .incInt(functionExecutionsRunningId(), -1);
    verify(statistics)
        .incInt(functionExecutionExceptionsId(), 1);
    verify(statistics, never())
        .incInt(eq(functionExecutionsHasResultRunningId()), anyInt());
  }

  @Test
  public void recordFailedExecution_hasResult_incrementsStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    functionStats.recordFailedExecution(5, NANOSECONDS, true);

    verify(statistics)
        .incInt(functionExecutionsRunningId(), -1);
    verify(statistics)
        .incInt(functionExecutionExceptionsId(), 1);
    verify(statistics)
        .incInt(functionExecutionsHasResultRunningId(), -1);
  }

  @Test
  public void recordFailedExecution_incrementsAggregateStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    boolean haveResult = true;
    functionStats.recordFailedExecution(5, NANOSECONDS, haveResult);

    verify(functionServiceStats)
        .endFunctionExecutionWithException(haveResult);
  }

  @Test
  public void close_removesSuccessTimer() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    functionStats.close();

    assertThat(successTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=true", FUNCTION_ID)
        .isNull();
  }

  @Test
  public void close_removesFailureTimer() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    functionStats.close();

    assertThat(failureTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=false", FUNCTION_ID)
        .isNull();
  }

  @Test
  public void close_closesTimers() {
    Timer successTimer = mock(Timer.class);
    Timer failureTimer = mock(Timer.class);
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, mock(MeterRegistry.class), statistics,
            functionServiceStats, 0L, false, successTimer, failureTimer);

    functionStats.close();

    verify(successTimer).close();
    verify(failureTimer).close();
  }

  @Test
  public void close_closesStats() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    functionStats.close();

    verify(statistics).close();
  }

  @Test
  public void isClosed_returnsFalse_ifCloseNotCalled() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    assertThat(functionStats.isClosed())
        .isFalse();
  }

  @Test
  public void isClosed_returnsTrue_ifCloseCalled() {
    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statistics, functionServiceStats);

    functionStats.close();

    assertThat(functionStats.isClosed())
        .isTrue();
  }

  @Test
  public void getStatistics_returnsGivenStatistics() {
    Statistics statisticsPassedToConstructor = mock(Statistics.class);

    FunctionStats functionStats =
        new FunctionStatsImpl(FUNCTION_ID, meterRegistry, statisticsPassedToConstructor,
            functionServiceStats);

    assertThat(functionStats.getStatistics())
        .isSameAs(statisticsPassedToConstructor);
  }

  private Timer successTimer() {
    return functionExecutionsTimer(true);
  }

  private Timer failureTimer() {
    return functionExecutionsTimer(false);
  }

  private Timer functionExecutionsTimer(boolean succeededTagValue) {
    return meterRegistry
        .find("geode.function.executions")
        .tag("function", FUNCTION_ID)
        .tag("succeeded", String.valueOf(succeededTagValue))
        .timer();
  }
}
