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

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.Statistics;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.LogService;

public class CallbackSampler {
  private static final Logger logger = LogService.getLogger();
  private final StatisticsManager statisticsManager;
  private final StatSamplerStats statSamplerStats;

  public CallbackSampler(StatisticsManager statisticsManager, StatSamplerStats statSamplerStats) {
    this.statisticsManager = statisticsManager;
    this.statSamplerStats = statSamplerStats;
  }

  public void sampleCallbacks() {
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
}
