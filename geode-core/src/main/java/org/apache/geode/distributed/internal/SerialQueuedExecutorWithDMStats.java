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
package org.apache.geode.distributed.internal;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * This class is used for the DM's serial executor. The only thing it currently does is increment
 * stats.
 */
public class SerialQueuedExecutorWithDMStats extends ThreadPoolExecutor {

  private final PoolStatHelper poolStatHelper;
  private final ThreadsMonitoring threadsMonitoring;

  public SerialQueuedExecutorWithDMStats(BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory, PoolStatHelper poolStatHelper,
      ThreadsMonitoring threadsMonitoring) {
    super(1, 1, 60, SECONDS, workQueue, threadFactory,
        new PooledExecutorWithDMStats.BlockHandler());

    this.poolStatHelper = poolStatHelper;
    this.threadsMonitoring = threadsMonitoring;
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (poolStatHelper != null) {
      poolStatHelper.startJob();
    }
    if (threadsMonitoring != null) {
      threadsMonitoring.startMonitor(ThreadsMonitoring.Mode.SerialQueuedExecutor);
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    if (poolStatHelper != null) {
      poolStatHelper.endJob();
    }
    if (threadsMonitoring != null) {
      threadsMonitoring.endMonitor();
    }
  }

  @VisibleForTesting
  public PoolStatHelper getPoolStatHelper() {
    return poolStatHelper;
  }
}
