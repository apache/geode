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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class FunctionStatsImpl implements FunctionStats {

  private static final String STATISTICS_NAME = "FunctionStatistics";

  @Immutable
  private static final StatisticsType STATISTICS_TYPE;

  /**
   * Total number of completed function.execute() calls (aka invocations of a individual
   * function)
   */
  private static final String FUNCTION_EXECUTIONS_COMPLETED = "functionExecutionsCompleted";

  /**
   * Total time consumed for all completed invocations of a individual function
   */
  private static final String FUNCTION_EXECUTIONS_COMPLETED_PROCESSING_TIME =
      "functionExecutionsCompletedProcessingTime";

  /**
   * A gauge indicating the number of currently running invocations
   */
  private static final String FUNCTION_EXECUTIONS_RUNNING = "functionExecutionsRunning";

  /**
   * Total number of results sent to the ResultCollector
   */
  private static final String RESULTS_SENT_TO_RESULT_COLLECTOR = "resultsSentToResultCollector";

  /**
   * Total number of FunctionService...execute() calls
   */
  private static final String FUNCTION_EXECUTION_CALLS = "functionExecutionCalls";

  /**
   * Total time consumed for all completed execute() calls where hasResult() returns true
   */
  private static final String FUNCTION_EXECUTIONS_HAS_RESULT_COMPLETED_PROCESSING_TIME =
      "functionExecutionsHasResultCompletedProcessingTime";

  /**
   * A gauge indicating the number of currently active execute() calls for functions where
   * hasResult() returns true
   */
  private static final String FUNCTION_EXECUTIONS_HAS_RESULT_RUNNING =
      "functionExecutionsHasResultRunning";

  /**
   * Total number of results sent to the ResultCollector
   */
  private static final String RESULTS_RECEIVED = "resultsReceived";

  /**
   * Total number of exceptions occurred while executing function
   */
  private static final String FUNCTION_EXECUTION_EXCEPTIONS = "functionExecutionsExceptions";

  private static final int functionExecutionsCompletedId;
  private static final int functionExecutionsCompletedProcessingTimeId;
  private static final int functionExecutionsRunningId;
  private static final int resultsSentToResultCollectorId;
  private static final int functionExecutionCallsId;
  private static final int functionExecutionsHasResultCompletedProcessingTimeId;
  private static final int functionExecutionsHasResultRunningId;
  private static final int resultsReceivedId;
  private static final int functionExecutionExceptionsId;

  static {
    String statDescription = "This is the stats for the individual Function's Execution";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    STATISTICS_TYPE = f.createType(STATISTICS_NAME, statDescription, new StatisticDescriptor[] {
        f.createIntCounter(FUNCTION_EXECUTIONS_COMPLETED,
            "Total number of completed function.execute() calls for given function", "operations"),
        f.createLongCounter(FUNCTION_EXECUTIONS_COMPLETED_PROCESSING_TIME,
            "Total time consumed for all completed invocations of the given function",
            "nanoseconds"),
        f.createIntGauge(FUNCTION_EXECUTIONS_RUNNING,
            "number of currently running invocations of the given function", "operations"),
        f.createIntCounter(RESULTS_SENT_TO_RESULT_COLLECTOR,
            "Total number of results sent to the ResultCollector", "operations"),
        f.createIntCounter(RESULTS_RECEIVED,
            "Total number of results received and passed to the ResultCollector", "operations"),
        f.createIntCounter(FUNCTION_EXECUTION_CALLS,
            "Total number of FunctionService.execute() calls for given function", "operations"),
        f.createLongCounter(FUNCTION_EXECUTIONS_HAS_RESULT_COMPLETED_PROCESSING_TIME,
            "Total time consumed for all completed given function.execute() calls where hasResult() returns true.",
            "nanoseconds"),
        f.createIntGauge(FUNCTION_EXECUTIONS_HAS_RESULT_RUNNING,
            "A gauge indicating the number of currently active execute() calls for functions where hasResult() returns true.",
            "operations"),
        f.createIntCounter(FUNCTION_EXECUTION_EXCEPTIONS,
            "Total number of Exceptions Occurred while executing function", "operations"),
    });

    functionExecutionsCompletedId = STATISTICS_TYPE.nameToId(FUNCTION_EXECUTIONS_COMPLETED);
    functionExecutionsCompletedProcessingTimeId =
        STATISTICS_TYPE.nameToId(FUNCTION_EXECUTIONS_COMPLETED_PROCESSING_TIME);
    functionExecutionsRunningId = STATISTICS_TYPE.nameToId(FUNCTION_EXECUTIONS_RUNNING);
    resultsSentToResultCollectorId = STATISTICS_TYPE.nameToId(RESULTS_SENT_TO_RESULT_COLLECTOR);
    functionExecutionCallsId = STATISTICS_TYPE.nameToId(FUNCTION_EXECUTION_CALLS);
    functionExecutionsHasResultCompletedProcessingTimeId =
        STATISTICS_TYPE.nameToId(FUNCTION_EXECUTIONS_HAS_RESULT_COMPLETED_PROCESSING_TIME);
    functionExecutionsHasResultRunningId = STATISTICS_TYPE.nameToId(
        FUNCTION_EXECUTIONS_HAS_RESULT_RUNNING);
    functionExecutionExceptionsId = STATISTICS_TYPE.nameToId(FUNCTION_EXECUTION_EXCEPTIONS);
    resultsReceivedId = STATISTICS_TYPE.nameToId(RESULTS_RECEIVED);
  }

  private final MeterRegistry meterRegistry;
  private final Statistics statistics;
  private final FunctionServiceStats aggregateStatistics;
  private final LongSupplier clock;
  private final BooleanSupplier timeStatisticsEnabled;
  private final Timer successTimer;
  private final Timer failureTimer;
  private final AtomicBoolean isClosed;

  FunctionStatsImpl(String functionId, MeterRegistry meterRegistry, Statistics statistics,
      FunctionServiceStats functionServiceStats) {
    this(functionId, meterRegistry, statistics, functionServiceStats, NanoTimer::getTime,
        () -> DistributionStats.enableClockStats, FunctionStatsImpl::registerSuccessTimer,
        FunctionStatsImpl::registerFailureTimer);
  }

  @VisibleForTesting
  FunctionStatsImpl(String functionId, MeterRegistry meterRegistry, Statistics statistics,
      FunctionServiceStats aggregateStatistics, long clockResult,
      boolean timeStatisticsEnabledResult) {
    this(functionId, meterRegistry, statistics, aggregateStatistics, () -> clockResult,
        () -> timeStatisticsEnabledResult, FunctionStatsImpl::registerSuccessTimer,
        FunctionStatsImpl::registerFailureTimer);
  }

  @VisibleForTesting
  FunctionStatsImpl(String functionId, MeterRegistry meterRegistry, Statistics statistics,
      FunctionServiceStats aggregateStatistics, long clockResult,
      boolean timeStatisticsEnabledResult, Timer successTimerResult, Timer registerFailureResult) {
    this(functionId, meterRegistry, statistics, aggregateStatistics, () -> clockResult,
        () -> timeStatisticsEnabledResult, (a, b) -> successTimerResult,
        (a, b) -> registerFailureResult);
  }

  private FunctionStatsImpl(String functionId, MeterRegistry meterRegistry, Statistics statistics,
      FunctionServiceStats aggregateStatistics, LongSupplier clock,
      BooleanSupplier timeStatisticsEnabled,
      BiFunction<String, MeterRegistry, Timer> registerSuccessTimerFunction,
      BiFunction<String, MeterRegistry, Timer> registerFailureTimerFunction) {

    requireNonNull(meterRegistry);

    this.meterRegistry = meterRegistry;
    this.statistics = statistics;
    this.aggregateStatistics = aggregateStatistics;
    this.clock = clock;
    this.timeStatisticsEnabled = timeStatisticsEnabled;

    isClosed = new AtomicBoolean(false);

    successTimer = registerSuccessTimerFunction.apply(functionId, meterRegistry);
    failureTimer = registerFailureTimerFunction.apply(functionId, meterRegistry);
  }

  @Override
  public void close() {
    meterRegistry.remove(successTimer);
    successTimer.close();

    meterRegistry.remove(failureTimer);
    failureTimer.close();

    statistics.close();

    isClosed.set(true);
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public int getFunctionExecutionsCompleted() {
    return statistics.getInt(functionExecutionsCompletedId);
  }

  @Override
  public int getFunctionExecutionsRunning() {
    return statistics.getInt(functionExecutionsRunningId);
  }

  @Override
  public void incResultsReturned() {
    statistics.incInt(resultsSentToResultCollectorId, 1);
    aggregateStatistics.incResultsReturned();
  }

  @Override
  public int getResultsReceived() {
    return statistics.getInt(resultsReceivedId);
  }

  @Override
  public void incResultsReceived() {
    statistics.incInt(resultsReceivedId, 1);
    aggregateStatistics.incResultsReceived();
  }

  @Override
  public int getFunctionExecutionCalls() {
    return statistics.getInt(functionExecutionCallsId);
  }

  @Override
  public long startFunctionExecution(boolean haveResult) {
    statistics.incInt(functionExecutionCallsId, 1);
    statistics.incInt(functionExecutionsRunningId, 1);

    if (haveResult) {
      statistics.incInt(functionExecutionsHasResultRunningId, 1);
    }

    aggregateStatistics.startFunctionExecution(haveResult);

    return clock.getAsLong();
  }

  @Override
  public void endFunctionExecution(long startTime, boolean haveResult) {
    long elapsedNanos = clock.getAsLong() - startTime;

    successTimer.record(elapsedNanos, NANOSECONDS);

    statistics.incInt(functionExecutionsCompletedId, 1);
    statistics.incInt(functionExecutionsRunningId, -1);

    if (timeStatisticsEnabled.getAsBoolean()) {
      statistics.incLong(functionExecutionsCompletedProcessingTimeId, elapsedNanos);
    }

    if (haveResult) {
      statistics.incInt(functionExecutionsHasResultRunningId, -1);

      if (timeStatisticsEnabled.getAsBoolean()) {
        statistics.incLong(functionExecutionsHasResultCompletedProcessingTimeId, elapsedNanos);
      }
    }

    aggregateStatistics.endFunctionExecutionWithElapsedTime(elapsedNanos, haveResult);
  }

  @Override
  public void endFunctionExecutionWithException(long startTime, boolean haveResult) {
    long elapsedNanos = clock.getAsLong() - startTime;

    failureTimer.record(elapsedNanos, NANOSECONDS);

    statistics.incInt(functionExecutionsRunningId, -1);
    statistics.incInt(functionExecutionExceptionsId, 1);

    if (haveResult) {
      statistics.incInt(functionExecutionsHasResultRunningId, -1);
    }

    aggregateStatistics.endFunctionExecutionWithException(haveResult);
  }

  @Override
  @VisibleForTesting
  public Statistics getStatistics() {
    return statistics;
  }

  @Override
  @VisibleForTesting
  public MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }

  public static StatisticsType getStatisticsType() {
    return STATISTICS_TYPE;
  }

  @VisibleForTesting
  static int functionExecutionsCompletedId() {
    return functionExecutionsCompletedId;
  }

  @VisibleForTesting
  static int functionExecutionsRunningId() {
    return functionExecutionsRunningId;
  }

  @VisibleForTesting
  static int functionExecutionsHasResultRunningId() {
    return functionExecutionsHasResultRunningId;
  }

  @VisibleForTesting
  static int functionExecutionsCompletedProcessingTimeId() {
    return functionExecutionsCompletedProcessingTimeId;
  }

  @VisibleForTesting
  static int functionExecutionsHasResultCompletedProcessingTimeId() {
    return functionExecutionsHasResultCompletedProcessingTimeId;
  }

  @VisibleForTesting
  static int functionExecutionExceptionsId() {
    return functionExecutionExceptionsId;
  }

  private static Timer registerSuccessTimer(String functionId, MeterRegistry meterRegistry) {
    return Timer.builder("geode.function.executions")
        .description("Count and total time of successful function executions")
        .tag("function", functionId)
        .tag("succeeded", TRUE.toString())
        .register(meterRegistry);
  }

  private static Timer registerFailureTimer(String functionId, MeterRegistry meterRegistry) {
    return Timer.builder("geode.function.executions")
        .description("Count and total time of failed function executions")
        .tag("function", functionId)
        .tag("succeeded", FALSE.toString())
        .register(meterRegistry);
  }
}
