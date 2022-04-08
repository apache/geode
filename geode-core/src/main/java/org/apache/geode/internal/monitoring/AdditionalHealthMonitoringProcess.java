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

import java.util.Collection;
import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class AdditionalHealthMonitoringProcess extends TimerTask {

  private static final Logger logger = LogService.getLogger();

  private final AdditionalHealthMonitoringImpl healthMonitoring;
  private final int timeLimitMillis;

  protected AdditionalHealthMonitoringProcess(AdditionalHealthMonitoringImpl healthMonitoring,
      int timeLimitMillis) {
    this.timeLimitMillis = timeLimitMillis;
    this.healthMonitoring = healthMonitoring;
  }

  private void checkAsyncWriter() {
    final long currentTime = System.currentTimeMillis();

    if (healthMonitoring.getInternalCache() == null
        || healthMonitoring.getInternalCache().isClosed()) {
      return;
    }
    Collection<DiskStore> stores = healthMonitoring.getInternalCache().listDiskStores();
    if (stores.isEmpty()) {
      return;
    }
    for (DiskStore diskStore : stores) {
      DiskStoreImpl dsi = (DiskStoreImpl) diskStore;
      if (dsi.isOffline() || dsi.isClosing()) {
        continue;
      }

      final long startTime = dsi.getAsyncWriterTime();
      long delta = currentTime - startTime;
      if (delta >= timeLimitMillis) {
        logger.fatal("AsyncWriter in DiskStore {} on member {} is stuck for {} seconds",
            dsi.getName(), healthMonitoring.getInternalCache().getMyId().getName(), (delta / 1000));
        logger.warn("AsyncWriter queue size is {}", dsi.getAsyncQueueSize());
      }
    }
  }

  @Override
  public void run() {
    if (healthMonitoring.isClosed()) {
      return;
    }
    checkAsyncWriter();
  }

}
