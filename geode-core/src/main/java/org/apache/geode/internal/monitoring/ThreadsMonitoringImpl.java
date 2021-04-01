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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;
import org.apache.geode.internal.monitoring.executor.FunctionExecutionPooledExecutorGroup;
import org.apache.geode.internal.monitoring.executor.GatewaySenderEventProcessorGroup;
import org.apache.geode.internal.monitoring.executor.OneTaskOnlyExecutorGroup;
import org.apache.geode.internal.monitoring.executor.P2PReaderExecutorGroup;
import org.apache.geode.internal.monitoring.executor.PooledExecutorGroup;
import org.apache.geode.internal.monitoring.executor.ScheduledThreadPoolExecutorWKAGroup;
import org.apache.geode.internal.monitoring.executor.SerialQueuedExecutorGroup;
import org.apache.geode.internal.monitoring.executor.ServerConnectionExecutorGroup;

public class ThreadsMonitoringImpl implements ThreadsMonitoring {

  private final ConcurrentMap<Long, AbstractExecutor> monitorMap;

  /** Monitors the health of the entire distributed system */
  private ThreadsMonitoringProcess tmProcess = null;

  private final Timer timer;

  /** Is this ThreadsMonitoringImpl closed?? */
  private boolean isClosed = true;

  public ThreadsMonitoringImpl(InternalDistributedSystem iDistributedSystem, int timeIntervalMillis,
      int timeLimitMillis) {
    this(iDistributedSystem, timeIntervalMillis, timeLimitMillis, true);
  }

  @VisibleForTesting
  ThreadsMonitoringImpl(InternalDistributedSystem iDistributedSystem, int timeIntervalMillis,
      int timeLimitMillis, boolean startThread) {
    this.monitorMap = new ConcurrentHashMap<>();
    this.isClosed = false;
    if (startThread) {
      timer = new Timer("ThreadsMonitor", true);
      tmProcess = new ThreadsMonitoringProcess(this, iDistributedSystem, timeLimitMillis);
      timer.schedule(tmProcess, 0, timeIntervalMillis);
    } else {
      timer = null;
    }
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
    if (timer != null) {
      this.timer.cancel();
      this.tmProcess = null;
    }
    this.monitorMap.clear();
  }

  public ThreadsMonitoringProcess getThreadsMonitoringProcess() {
    return this.tmProcess;
  }

  @Override
  public void updateThreadStatus() {
    AbstractExecutor executor = monitorMap.get(Thread.currentThread().getId());
    if (executor != null) {
      executor.setStartTime(System.currentTimeMillis());
    }
  }

  @Override
  public boolean startMonitor(Mode mode) {
    AbstractExecutor executor = createAbstractExecutor(mode);
    executor.setStartTime(System.currentTimeMillis());
    return register(executor);
  }

  @Override
  public void endMonitor() {
    monitorMap.remove(Thread.currentThread().getId());
  }

  @VisibleForTesting
  boolean isMonitoring() {
    AbstractExecutor executor = monitorMap.get(Thread.currentThread().getId());
    if (executor == null) {
      return false;
    }
    return !executor.isMonitoringSuspended();
  }

  @Override
  public AbstractExecutor createAbstractExecutor(Mode mode) {
    switch (mode) {
      case FunctionExecutor:
        return new FunctionExecutionPooledExecutorGroup();
      case PooledExecutor:
        return new PooledExecutorGroup();
      case SerialQueuedExecutor:
        return new SerialQueuedExecutorGroup();
      case OneTaskOnlyExecutor:
        return new OneTaskOnlyExecutorGroup();
      case ScheduledThreadExecutor:
        return new ScheduledThreadPoolExecutorWKAGroup();
      case AGSExecutor:
        return new GatewaySenderEventProcessorGroup();
      case P2PReaderExecutor:
        return new P2PReaderExecutorGroup();
      case ServerConnectionExecutor:
        return new ServerConnectionExecutorGroup();
      default:
        throw new IllegalStateException("Unhandled mode=" + mode);
    }
  }

  @Override
  public boolean register(AbstractExecutor executor) {
    monitorMap.put(executor.getThreadID(), executor);
    return true;
  }

  @Override
  public void unregister(AbstractExecutor executor) {
    monitorMap.remove(executor.getThreadID());
  }

  @VisibleForTesting
  boolean isMonitoring(AbstractExecutor executor) {
    if (executor.isMonitoringSuspended()) {
      return false;
    }
    return monitorMap.containsKey(executor.getThreadID());
  }

  @VisibleForTesting
  Timer getTimer() {
    return this.timer;
  }

}
