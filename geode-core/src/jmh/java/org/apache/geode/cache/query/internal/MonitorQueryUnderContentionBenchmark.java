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


import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

@State(Scope.Thread)
@Fork(1)
public class MonitorQueryUnderContentionBenchmark {

  /*
   * all times in milliseconds
   */

  private static final long QueryMaxExecutionTime = 6;

  /*
   * Delay, from time startOneSimulatedQuery() is called, until monitorQueryThread() is called.
   */
  public static final int QueryInitialDelay = 0;

  /*
   * The mode is the center of the "hump" of the Gaussian distribution.
   *
   * We usually want to arrange the two humps equidistant from QueryMaxExecutionTime.
   */
  private static final int FastQueryCompletionMode = 1;
  private static final int SlowQueryCompletionMode = 1000000;

  /*
   * How often should we start a query of each type?
   *
   * Starting them more frequently leads to heavier load.
   *
   * They're separated so we can play with different mixes.
   */
  private static final int StartFastQueryPeriod = 1;
  private static final int StartSlowQueryPeriod = 1;

  /*
   * After load is established, how many measurements shall we take?
   */
  private static final double BenchmarkIterations = 1e4;

  public static final int TimeToQuiesceBeforeSampling = 240000;

  public static final int ThreadPoolProcessorMultiple = 2;


  public static final int RandomSeed = 151;

  private QueryMonitor monitor;
  private Thread thread;
  private DefaultQuery query;
  private InternalCache cache;
  private Random random;
  private ScheduledThreadPoolExecutor executorService;

  @Setup(Level.Trial)
  public void trialSetup() throws InterruptedException {
    cache = mock(InternalCache.class);
    monitor =
        new QueryMonitor(() -> (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1),
            cache, QueryMaxExecutionTime);
    thread = mock(Thread.class);

    final int numberOfThreads =
        ThreadPoolProcessorMultiple * Runtime.getRuntime().availableProcessors();

    executorService =
        (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
            numberOfThreads);

    System.out.println(String.format("Pool has %d threads", numberOfThreads));

    executorService.setRemoveOnCancelPolicy(true);

    random = new Random(RandomSeed);

    query = createDefaultQuery();

    generateLoad(executorService, () -> startOneFastQuery(executorService),
        StartFastQueryPeriod);

    generateLoad(executorService, () -> startOneSlowQuery(executorService),
        StartSlowQueryPeriod);

    // allow system to quiesce
    Thread.sleep(TimeToQuiesceBeforeSampling);

    System.out.println("Queries in flight prior to test: " + executorService.getQueue().size());
  }

  @TearDown(Level.Trial)
  public void trialTeardown() {
    executorService.shutdownNow();
    monitor.stopMonitoring();
  }

  @Benchmark
  @Measurement(iterations = (int) BenchmarkIterations)
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  // @Warmup we don't warm up because our @Setup warms us up
  public void monitorQuery() {
    monitor.monitorQueryThread(query);
    monitor.stopMonitoringQueryThread(query);
  }

  private ScheduledFuture<?> generateLoad(final ScheduledExecutorService executorService,
      final Runnable queryStarter, int startPeriod) {
    return executorService.scheduleAtFixedRate(() -> {
      queryStarter.run();
    },
        QueryInitialDelay,
        startPeriod,
        TimeUnit.MILLISECONDS);
  }

  private void startOneFastQuery(ScheduledExecutorService executorService) {
    startOneSimulatedQuery(executorService, 100, FastQueryCompletionMode);
  }

  private void startOneSlowQuery(ScheduledExecutorService executorService) {
    startOneSimulatedQuery(executorService, 100, SlowQueryCompletionMode);
  }

  private void startOneSimulatedQuery(ScheduledExecutorService executorService,
      int startDelayRangeMillis, int completeDelayRangeMillis) {
    executorService.schedule(() -> {
      final Thread thread = mock(Thread.class);
      final DefaultQuery query = createDefaultQuery();
      monitor.monitorQueryThread(query);
      executorService.schedule(() -> {
        monitor.stopMonitoringQueryThread(query);
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

  private String querySpeed(int completeDelayRangeMillis) {
    return completeDelayRangeMillis == FastQueryCompletionMode
        ? "FAST" : "(slow)";
  }

  private DefaultQuery createDefaultQuery() {
    // we can reuse this because it doesn't affect lookup or equality in the collection(s)
    final DefaultQuery defaultQuery = mock(DefaultQuery.class);
    doReturn(new AtomicBoolean(false)).when(defaultQuery).getQueryCompletedForMonitoring();
    return defaultQuery;
  }
}
