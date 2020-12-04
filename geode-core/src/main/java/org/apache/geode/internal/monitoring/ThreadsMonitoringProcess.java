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

package org.apache.geode.internal.monitoring;

import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.ResourceManagerStats;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;
import org.apache.geode.logging.internal.log4j.api.LogService;


public class ThreadsMonitoringProcess extends TimerTask {

  private static final Logger logger = LogService.getLogger();

  private final ThreadsMonitoring threadsMonitoring;
  private final int timeLimitMillis;
  private final InternalDistributedSystem internalDistributedSystem;

  private ResourceManagerStats resourceManagerStats = null;

  protected ThreadsMonitoringProcess(ThreadsMonitoring tMonitoring,
      InternalDistributedSystem iDistributedSystem, int timeLimitMillis) {
    this.timeLimitMillis = timeLimitMillis;
    this.threadsMonitoring = tMonitoring;
    this.internalDistributedSystem = iDistributedSystem;
  }

  @VisibleForTesting
  /**
   * Returns true if a stuck thread was detected
   */
  public boolean mapValidation() {
    int numOfStuck = 0;
    for (AbstractExecutor executor : threadsMonitoring.getMonitorMap().values()) {
      if (executor.isMonitoringSuspended()) {
        continue;
      }
      final long startTime = executor.getStartTime();
      final long currentTime = System.currentTimeMillis();
      if (startTime == 0) {
        executor.setStartTime(currentTime);
        continue;
      }
      long threadId = executor.getThreadID();
      logger.trace("Checking thread {}", threadId);
      long delta = currentTime - startTime;
      if (delta >= timeLimitMillis) {
        numOfStuck++;
        logger.warn("Thread {} (0x{}) is stuck", threadId, Long.toHexString(threadId));
        executor.handleExpiry(delta);
      }
    }
    updateNumThreadStuckStatistic(numOfStuck);
    if (numOfStuck == 0) {
      logger.trace("There are no stuck threads in the system");
    } else if (numOfStuck != 1) {
      logger.warn("There are {} stuck threads in this node", numOfStuck);
    } else {
      logger.warn("There is 1 stuck thread in this node");
    }
    return numOfStuck != 0;
  }

  private void updateNumThreadStuckStatistic(int numOfStuck) {
    ResourceManagerStats stats = getResourceManagerStats();
    if (stats != null) {
      stats.setNumThreadStuck(numOfStuck);
    }
  }

  @Override
  public void run() {
    mapValidation();
  }

  @VisibleForTesting
  public ResourceManagerStats getResourceManagerStats() {
    ResourceManagerStats result = resourceManagerStats;
    if (result == null) {
      try {
        if (internalDistributedSystem == null || !internalDistributedSystem.isConnected()) {
          return null;
        }
        DistributionManager distributionManager =
            internalDistributedSystem.getDistributionManager();
        InternalCache cache = distributionManager.getExistingCache();
        result = cache.getInternalResourceManager().getStats();
        resourceManagerStats = result;
      } catch (CacheClosedException e1) {
        logger.trace("could not update statistic since cache is closed");
      }
    }
    return result;
  }
}
