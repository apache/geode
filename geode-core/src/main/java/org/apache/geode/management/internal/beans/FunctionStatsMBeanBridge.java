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
package org.apache.geode.management.internal.beans;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.ManagementStrings;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.logging.log4j.Logger;

/**
 * @param <internalCache>
 * 
 * 
 */
public class FunctionStatsMBeanBridge<internalCache> {

  private Function functionDetails;

  private MBeanStatsMonitor monitor;

  /**
   * Internal distributed system
   */
  private FunctionStats stats;

  private InternalDistributedSystem system;

  private InternalCache internalCache;

  public FunctionStatsMBeanBridge(@SuppressWarnings("rawtypes") Function function,
      InternalCache internalCache) {
    this.internalCache = internalCache;
    final Logger logger = LogService.getLogger();
    this.functionDetails = (Function) function;
    this.monitor =
        new MBeanStatsMonitor(ManagementStrings.FUNCTION_STATS_MONITOR.toLocalizedString());
    this.system = (InternalDistributedSystem) internalCache.getDistributedSystem();
    this.stats = system.getFunctionStats(functionDetails.getId());
  }

  public FunctionStatsMBeanBridge() {
    this.monitor =
        new MBeanStatsMonitor(ManagementStrings.FUNCTION_STATS_MONITOR.toLocalizedString());
  }

  public void stopMonitor() {

    if (monitor != null) {
      monitor.stopListener();
    }
  }

  public String getId() {
    return this.functionDetails.getId();
  }

  private Number getStatistic(String statName) {
    if (monitor != null) {
      return monitor.getStatistic(statName);
    } else {
      return 0;
    }
  }

  public int getFunctionExecutionsHasResultRunning() {
    return stats.getFunctionExecutionHasResultRunning();
  }

  public int getFunctionExecutionsHasResultCompletedProcessingTime() {
    return stats.getFunctionExecutionHasResultCompleteProcessingTime();
  }

  public int getFunctionExecutionCalls() {
    return stats.getFunctionExecutionCalls();
  }

  public int getResultsSentToResultCollector() {
    return stats.getResultsSentToResultCollector();
  }

  public int getFunctionExecutionsRunning() {
    return stats.getFunctionExecutionsRunning();
  }

  public long getFunctionExecutionsCompletedProcessingTime() {
    return stats.getFunctionExecutionCompleteProcessingTime();
  }

  public int getFunctionExecutionsCompleted() {
    return stats.getFunctionExecutionsCompleted();
  }

  public int getResultsReceived() {
    return stats.getResultsReceived();

  }

  public int getFunctionExecutionExceptions() {
    return stats.getFunctionExecutionExceptions();
  }
}
