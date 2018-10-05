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

import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.ResourceManagerStats;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;


public class ThreadsMonitoringProcess extends TimerTask {

  private final ThreadsMonitoring threadsMonitoring;
  private ResourceManagerStats resourceManagerStats = null;
  private static final Logger logger = LogService.getLogger();
  private final int timeLimit;
  private final InternalDistributedSystem internalDistributedSystem;

  private final Properties nonDefault = new Properties();
  private final DistributionConfigImpl distributionConfigImpl =
      new DistributionConfigImpl(nonDefault);

  protected ThreadsMonitoringProcess(ThreadsMonitoring tMonitoring,
      InternalDistributedSystem iDistributedSystem) {
    this.timeLimit = this.distributionConfigImpl.getThreadMonitorTimeLimit();
    this.threadsMonitoring = tMonitoring;
    this.internalDistributedSystem = iDistributedSystem;
  }

  public boolean mapValidation() {
    boolean isStuck = false;
    int numOfStuck = 0;
    for (Entry<Long, AbstractExecutor> entry1 : this.threadsMonitoring.getMonitorMap().entrySet()) {
      logger.trace("Checking thread {}", entry1.getKey());
      long currentTime = System.currentTimeMillis();
      long delta = currentTime - entry1.getValue().getStartTime();
      if (delta >= this.timeLimit) {
        isStuck = true;
        numOfStuck++;
        logger.warn("Thread <{}> is stuck", entry1.getKey());
        entry1.getValue().handleExpiry(delta);
      }
    }
    if (!isStuck) {
      if (this.resourceManagerStats != null)
        this.resourceManagerStats.setNumThreadStuck(0);
      logger.trace("There are no stuck threads in the system");
      return false;
    } else {
      if (this.resourceManagerStats != null)
        this.resourceManagerStats.setNumThreadStuck(numOfStuck);
      if (numOfStuck != 1) {
        logger.warn("There are {} stuck threads in this node", numOfStuck);
      } else {
        logger.warn("There is 1 stuck thread in this node");
      }
      return true;
    }
  }

  @Override
  public void run() {
    if (this.resourceManagerStats == null) {
      try {
        if (this.internalDistributedSystem == null || !this.internalDistributedSystem.isConnected())
          return;
        DistributionManager distributionManager =
            this.internalDistributedSystem.getDistributionManager();
        InternalCache cache = distributionManager.getExistingCache();
        this.resourceManagerStats = cache.getInternalResourceManager().getStats();
      } catch (CacheClosedException e1) {
        logger.trace("No cache exists yet - process will run on next iteration");
      }
    } else
      mapValidation();
  }

  public ResourceManagerStats getResourceManagerStats() {
    return this.resourceManagerStats;
  }
}
