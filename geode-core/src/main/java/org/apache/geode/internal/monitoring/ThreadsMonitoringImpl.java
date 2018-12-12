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


import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;
import org.apache.geode.internal.monitoring.executor.FunctionExecutionPooledExecutorGroup;
import org.apache.geode.internal.monitoring.executor.GatewaySenderEventProcessorGroup;
import org.apache.geode.internal.monitoring.executor.OneTaskOnlyExecutorGroup;
import org.apache.geode.internal.monitoring.executor.PooledExecutorGroup;
import org.apache.geode.internal.monitoring.executor.ScheduledThreadPoolExecutorWKAGroup;
import org.apache.geode.internal.monitoring.executor.SerialQueuedExecutorGroup;

public class ThreadsMonitoringImpl implements ThreadsMonitoring {

  private final ConcurrentMap<Long, AbstractExecutor> monitorMap;

  /** Monitors the health of the entire distributed system */
  private ThreadsMonitoringProcess tmProcess = null;

  private final Properties nonDefault = new Properties();
  private final DistributionConfigImpl distributionConfigImpl =
      new DistributionConfigImpl(nonDefault);

  private final Timer timer =
      new Timer("ThreadsMonitor", true);

  /** Is this ThreadsMonitoringImpl closed?? */
  private boolean isClosed = true;

  public ThreadsMonitoringImpl(InternalDistributedSystem iDistributedSystem) {
    this.monitorMap = new ConcurrentHashMap<>();
    this.isClosed = false;
    setThreadsMonitoringProcess(iDistributedSystem);
  }

  @Override
  public ConcurrentMap<Long, AbstractExecutor> getMonitorMap() {
    return monitorMap;
  }

  public boolean isClosed() {
    return this.isClosed;
  }

  @Override
  public void close() {
    if (isClosed)
      return;

    isClosed = true;
    if (tmProcess != null) {
      this.timer.cancel();
      this.tmProcess = null;
    }
    this.monitorMap.clear();
  }

  /** Starts a new {@link org.apache.geode.internal.monitoring.ThreadsMonitoringProcess} */
  private void setThreadsMonitoringProcess(InternalDistributedSystem iDistributedSystem) {
    this.tmProcess = new ThreadsMonitoringProcess(this, iDistributedSystem);
    this.timer.schedule(tmProcess, 0, distributionConfigImpl.getThreadMonitorInterval());
  }

  public ThreadsMonitoringProcess getThreadsMonitoringProcess() {
    return this.tmProcess;
  }

  @Override
  public boolean startMonitor(Mode mode) {
    AbstractExecutor absExtgroup;
    switch (mode) {
      case FunctionExecutor:
        absExtgroup = new FunctionExecutionPooledExecutorGroup(this);
        break;
      case PooledExecutor:
        absExtgroup = new PooledExecutorGroup(this);
        break;
      case SerialQueuedExecutor:
        absExtgroup = new SerialQueuedExecutorGroup(this);
        break;
      case OneTaskOnlyExecutor:
        absExtgroup = new OneTaskOnlyExecutorGroup(this);
        break;
      case ScheduledThreadExecutor:
        absExtgroup = new ScheduledThreadPoolExecutorWKAGroup(this);
        break;
      case AGSExecutor:
        absExtgroup = new GatewaySenderEventProcessorGroup(this);
        break;
      default:
        return false;
    }
    this.monitorMap.put(Thread.currentThread().getId(), absExtgroup);
    return true;
  }

  @Override
  public void endMonitor() {
    this.monitorMap.remove(Thread.currentThread().getId());
  }

  public Timer getTimer() {
    return this.timer;
  }

}
