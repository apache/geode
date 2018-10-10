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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;

public class CallbackSampler {
  private static final Logger logger = LogService.getLogger();
  private StatisticsManager statisticsManager;
  private final CancelCriterion cancelCriterion;
  private long sampleIntervalNanos;
  private ScheduledExecutorService executor;
  private final StatSamplerStats statSamplerStats;

  public CallbackSampler(final CancelCriterion cancelCriterion,
      final StatSamplerStats statSamplerStats) {
    this.cancelCriterion = cancelCriterion;
    this.statSamplerStats = statSamplerStats;
  }

  public void start(StatisticsManager statisticsManager, int sampleInterval, TimeUnit timeUnit) {
    ScheduledExecutorService executor =
        LoggingExecutors.newSingleThreadScheduledExecutor("CallbackSampler");
    start(executor, statisticsManager, sampleInterval, timeUnit);
  }

  void start(ScheduledExecutorService executor, StatisticsManager statisticsManager,
      int sampleInterval, TimeUnit timeUnit) {
    stop();
    this.statisticsManager = statisticsManager;
    this.executor = executor;

    executor.scheduleAtFixedRate(() -> sampleCallbacks(), sampleInterval, sampleInterval, timeUnit);
  }

  private void sampleCallbacks() {
    if (cancelCriterion.isCancelInProgress()) {
      executor.shutdown();
    }
    int errors = 0;
    int suppliers = 0;
    long start = System.nanoTime();
    try {
      for (Statistics stats : statisticsManager.getStatsList()) {
        StatisticsImpl statistics = (StatisticsImpl) stats;
        errors += statistics.invokeSuppliers();
        suppliers += statistics.getSupplierCount();
      }
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
    } catch (Throwable throwable) {
      logger.error("Error invoking statistic suppliers", throwable);
    } finally {
      long end = System.nanoTime();
      statSamplerStats.incSampleCallbackDuration(TimeUnit.NANOSECONDS.toMillis(end - start));
      statSamplerStats.incSampleCallbackErrors(errors);
      statSamplerStats.setSampleCallbacks(suppliers);
    }

  }

  public void stop() {
    if (executor != null) {
      this.executor.shutdown();
    }
  }
}
