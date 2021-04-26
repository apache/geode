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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

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
    final long currentTime = System.currentTimeMillis();
    final List<AbstractExecutor> stuckThreads = new ArrayList<>();
    final List<Long> stuckThreadIds = new ArrayList<>();
    checkForStuckThreads(threadsMonitoring.getMonitorMap().values(), currentTime,
        (executor, stuckTime) -> {
          final long threadId = executor.getThreadID();
          stuckThreads.add(executor);
          stuckThreadIds.add(threadId);
          rememberLockOwnerThreadId(stuckThreadIds, threadId);
        });

    final Map<Long, ThreadInfo> threadInfoMap = createThreadInfoMap(stuckThreadIds);
    final AtomicInteger numOfStuck = new AtomicInteger();
    checkForStuckThreads(stuckThreads, currentTime, (executor, stuckTime) -> {
      long threadId = executor.getThreadID();
      logger.warn("Thread {} (0x{}) is stuck", threadId, Long.toHexString(threadId));
      executor.handleExpiry(stuckTime, threadInfoMap);
      numOfStuck.incrementAndGet();
    });

    updateNumThreadStuckStatistic(numOfStuck.get());
    if (numOfStuck.get() == 0) {
      logger.trace("There are no stuck threads in the system");
    } else if (numOfStuck.get() != 1) {
      logger.warn("There are {} stuck threads in this node", numOfStuck.get());
    } else {
      logger.warn("There is 1 stuck thread in this node");
    }
    return numOfStuck.get() != 0;
  }

  private interface StuckAction {
    void run(AbstractExecutor executor, long stuckTime);
  }

  /**
   * Iterate over "executors" calling "action" on each one that is stuck.
   */
  private void checkForStuckThreads(Collection<AbstractExecutor> executors, long currentTime,
      StuckAction action) {
    for (AbstractExecutor executor : executors) {
      if (executor.isMonitoringSuspended()) {
        continue;
      }
      final long startTime = executor.getStartTime();
      if (startTime == 0) {
        executor.setStartTime(currentTime);
        continue;
      }
      long delta = currentTime - startTime;
      if (delta >= timeLimitMillis) {
        action.run(executor, delta);
      }
    }
  }

  @VisibleForTesting
  public static Map<Long, ThreadInfo> createThreadInfoMap(List<Long> stuckThreadIds) {
    long[] ids = new long[stuckThreadIds.size()];
    int idx = 0;
    for (long id : stuckThreadIds) {
      ids[idx] = id;
      idx++;
    }
    ThreadInfo[] threadInfos = ManagementFactory.getThreadMXBean().getThreadInfo(ids, true, true);
    Map<Long, ThreadInfo> result = new HashMap<>();
    for (ThreadInfo threadInfo : threadInfos) {
      if (threadInfo != null) {
        result.put(threadInfo.getThreadId(), threadInfo);
      }
    }
    return result;
  }

  private void rememberLockOwnerThreadId(List<Long> stuckThreadIds, long threadId) {
    final long lockOwnerId = getLockOwnerId(threadId);
    if (lockOwnerId != -1) {
      stuckThreadIds.add(lockOwnerId);
    }
  }

  private long getLockOwnerId(long threadId) {
    final ThreadInfo threadInfo = ManagementFactory.getThreadMXBean().getThreadInfo(threadId, 0);
    if (threadInfo != null) {
      return threadInfo.getLockOwnerId();
    }
    return -1;
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
