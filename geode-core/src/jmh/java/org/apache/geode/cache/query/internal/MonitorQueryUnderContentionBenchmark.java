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
package org.apache.geode.cache.query.internal;

import static org.mockito.Mockito.mock;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

@State(Scope.Thread)
@Fork(1)
public class MonitorQueryUnderContentionBenchmark {

  /*
   * All times in milliseconds.
   *
   * The "mode" is the center of the "hump" of the Gaussian distribution.
   */

  private static final long QUERY_MAX_EXECUTION_TIME = 6;

  /*
   * Delay, before starting a simulated query task
   */
  private static final int START_DELAY_RANGE_MILLIS = 100;

  /*
   * Delay, from time startOneSimulatedQuery() is called, until monitorQueryThread() is called.
   */
  private static final int QUERY_INITIAL_DELAY = 0;

  private static final int FAST_QUERY_COMPLETION_MODE = 1;
  private static final int SLOW_QUERY_COMPLETION_MODE = 1000000;

  /*
   * Dictates how often we start each query type.
   *
   * Starting them more frequently leads to heavier load.
   *
   * They're separated so we can play with different mixes.
   */
  private static final int START_FAST_QUERY_PERIOD = 1;
  private static final int START_SLOW_QUERY_PERIOD = 1;

  /*
   * After load is established, how many measurements shall we take?
   */
  private static final double BENCHMARK_ITERATIONS = 1e4;

  private static final int TIME_TO_QUIESCE_BEFORE_SAMPLING = 240000;

  private static final int THREAD_POOL_PROCESSOR_MULTIPLE = 2;


  private static final int RANDOM_SEED = 151;

  private QueryMonitor queryMonitor;
  private DefaultQuery query;
  private Random random;
  private ScheduledThreadPoolExecutor loadGenerationExecutorService;
  private org.apache.logging.log4j.Level originalBaseLogLevel;

  @Setup(Level.Trial)
  public void trialSetup() throws InterruptedException {

    originalBaseLogLevel = LogService.getBaseLogLevel();
    LogService.setBaseLogLevel(org.apache.logging.log4j.Level.OFF);

    queryMonitor =
        new QueryMonitor(() -> (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1),
            mock(InternalCache.class), QUERY_MAX_EXECUTION_TIME);

    final int numberOfThreads =
        THREAD_POOL_PROCESSOR_MULTIPLE * Runtime.getRuntime().availableProcessors();

    loadGenerationExecutorService =
        (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
            numberOfThreads);

    System.out.println(String.format("Pool has %d threads", numberOfThreads));

    loadGenerationExecutorService.setRemoveOnCancelPolicy(true);

    random = new Random(RANDOM_SEED);

    query = createDefaultQuery();

    generateLoad(
        loadGenerationExecutorService, () -> startOneFastQuery(loadGenerationExecutorService),
        START_FAST_QUERY_PERIOD);

    generateLoad(
        loadGenerationExecutorService, () -> startOneSlowQuery(loadGenerationExecutorService),
        START_SLOW_QUERY_PERIOD);

    // allow system to quiesce
    Thread.sleep(TIME_TO_QUIESCE_BEFORE_SAMPLING);

    System.out.println(
        "Queries in flight prior to test: " + loadGenerationExecutorService.getQueue().size());
  }

  @TearDown(Level.Trial)
  public void trialTeardown() {
    loadGenerationExecutorService.shutdownNow();
    queryMonitor.stopMonitoring();

    LogService.setBaseLogLevel(originalBaseLogLevel);
  }

  @Benchmark
  @Measurement(iterations = (int) BENCHMARK_ITERATIONS)
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  // @Warmup we don't warm up because our @Setup warms us up
  public void monitorQuery() {
    queryMonitor.monitorQueryThread(query);
    queryMonitor.stopMonitoringQueryThread(query);
  }

  private void generateLoad(final ScheduledExecutorService executorService,
      final Runnable queryStarter, int startPeriod) {
    executorService.scheduleAtFixedRate(queryStarter,
        QUERY_INITIAL_DELAY,
        startPeriod,
        TimeUnit.MILLISECONDS);
  }

  private void startOneFastQuery(ScheduledExecutorService executorService) {
    startOneSimulatedQuery(executorService, START_DELAY_RANGE_MILLIS, FAST_QUERY_COMPLETION_MODE);
  }

  private void startOneSlowQuery(ScheduledExecutorService executorService) {
    startOneSimulatedQuery(executorService, START_DELAY_RANGE_MILLIS, SLOW_QUERY_COMPLETION_MODE);
  }

  private void startOneSimulatedQuery(ScheduledExecutorService executorService,
      int startDelayRangeMillis, int completeDelayRangeMillis) {
    executorService.schedule(() -> {
      final DefaultQuery query = createDefaultQuery();
      queryMonitor.monitorQueryThread(query);
      executorService.schedule(() -> {
        queryMonitor.stopMonitoringQueryThread(query);
      },
          gaussianLong(completeDelayRangeMillis),
          TimeUnit.MILLISECONDS);
    },
        gaussianLong(startDelayRangeMillis),
        TimeUnit.MILLISECONDS);
  }

  private long gaussianLong(int range) {
    return (long) (random.nextGaussian() * range);
  }

  private DefaultQuery createDefaultQuery() {
    return mock(DefaultQuery.class);
  }
}
