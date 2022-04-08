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

import java.util.Timer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class AdditionalHealthMonitoringImpl {
  private static final Logger logger = LogService.getLogger();

  private final InternalCache internalCache;
  private final ThreadsMonitoring threadsMonitoring;
  private AdditionalHealthMonitoringProcess healthProcess;

  private boolean isClosed;

  private final Timer timer;

  public AdditionalHealthMonitoringImpl(InternalCache internalCache, int timeLimitMillis,
      int timeIntervalMillis, ThreadsMonitoring threadsMonitoring) {
    isClosed = false;

    this.internalCache = internalCache;
    this.threadsMonitoring = threadsMonitoring;
    timer = new Timer("AdditionalHealthMonitor", true);
    healthProcess = new AdditionalHealthMonitoringProcess(this, timeLimitMillis);
    timer.schedule(healthProcess, 30000, timeIntervalMillis);
  }

  protected InternalCache getInternalCache() {
    return internalCache;
  }

  protected ThreadsMonitoring getThreadsMonitoring() {
    return threadsMonitoring;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public void close() {
    if (isClosed) {
      return;
    }

    isClosed = true;
    if (timer != null) {
      timer.cancel();
      healthProcess = null;
    }
  }
}
