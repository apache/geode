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

package org.apache.geode.admin.internal;

import java.util.Date;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.ResourceManagerStats;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.statistics.AbstractExecutorGroup;


public class ThreadMonitoringProcess extends TimerTask {

  private ResourceManagerStats resourceManagerStats = null;
  private static final Logger logger = LogService.getLogger();
  private static int timeLimit;

  Properties nonDefault = new Properties();
  DistributionConfigImpl dcI = new DistributionConfigImpl(nonDefault);

  protected ThreadMonitoringProcess() {
    timeLimit = dcI.getThreadMonitorTimeLimit();
  }

  public boolean mapValidation() {
    boolean isStuck = false;
    int numOfStuck = 0;
    long delta;
    Date date = new Date();
    long currentTime = date.getTime();
    for (Entry<Long, AbstractExecutorGroup> entry1 : ThreadMonitoringProvider.getInstance()
        .getMonitorMap().entrySet()) {
      logger.trace("Checking Thread {}\n", entry1.getKey());
      delta = currentTime - entry1.getValue().getStartTime();
      if (delta >= timeLimit) {
        isStuck = true;
        numOfStuck++;
        logger.warn("Thread {} is stuck , initiating handleExpiry\n", entry1.getKey());
        entry1.getValue().handleExpiry(delta);
      }
    }
    if (!isStuck) {
      if (resourceManagerStats != null)
        resourceManagerStats.setIsThreadStuck(0);
      logger.trace("There are NO stuck threads in the system\n");
      return false;
    } else {
      if (resourceManagerStats != null)
        resourceManagerStats.setIsThreadStuck(numOfStuck);
      logger.warn("There are <{}> stuck threads in the system\n", numOfStuck);
      return true;
    }
  }

  @Override
  public void run() {
    if (resourceManagerStats == null) {
      try {
        InternalCache cache = GemFireCacheImpl.getExisting();
        this.resourceManagerStats = cache.getInternalResourceManager().getStats();
      } catch (CacheClosedException e1) {
        e1.printStackTrace();
      }
    } else
      mapValidation();
  }

}
