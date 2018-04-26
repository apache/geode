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


import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.admin.ThreadMonitoring;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.statistics.AbstractExecutorGroup;
import org.apache.geode.internal.statistics.FunctionExecutionPooledExecutorGroup;
import org.apache.geode.internal.statistics.GatewaySenderEventProcessorGroup;
import org.apache.geode.internal.statistics.OneTaskOnlyExecutorGroup;
import org.apache.geode.internal.statistics.PooledExecutorGroup;
import org.apache.geode.internal.statistics.ScheduledThreadPoolExecutorWKAGroup;
import org.apache.geode.internal.statistics.SerialQueuedExecutorGroup;

public class ThreadMonitoringImpl implements ThreadMonitoring {

  private static ConcurrentMap<Long, AbstractExecutorGroup> monitorMap;

  /** Monitors the health of the entire distributed system */
  private ThreadMonitoringProcess tmProcess = null;

  static Properties nonDefault = new Properties();
  static DistributionConfigImpl dcI = new DistributionConfigImpl(nonDefault);

  Timer timer = new Timer(LocalizedStrings.THREAD_MONITOR_NAME.toLocalizedString(), true);

  /** Is this ThreadMonitoringImpl closed? */
  private boolean isClosed = true;

  protected ThreadMonitoringImpl() {
    monitorMap = new ConcurrentHashMap<>();
    this.isClosed = false;
    setThreadMonitoringProcess();
  }

  public static boolean isEnabled() {
    return dcI.getThreadMonitorEnabled();
  }

  @Override
  public ConcurrentMap<Long, AbstractExecutorGroup> getMonitorMap() {
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
      timer.cancel();
      tmProcess = null;
    }
    monitorMap.clear();
    // ThreadMonitoringProvider.deleteInstance();
  }

  /*
   * private static void deleteInstance() { instance = null; }
   */

  /** Starts a new {@link org.apache.geode.admin.internal.ThreadMonitoringProcess} */
  public void setThreadMonitoringProcess() {

    tmProcess = new ThreadMonitoringProcess();
    timer.schedule(tmProcess, 0, dcI.getThreadMonitorInterval());
  }

  @Override
  public boolean startMonitor(Mode mode) {
    AbstractExecutorGroup absExtgroup;
    switch (mode) {
      case FunctionEx:
        absExtgroup = new FunctionExecutionPooledExecutorGroup();
        break;
      case PooledEx:
        absExtgroup = new PooledExecutorGroup();
        break;
      case SerialQueuedEx:
        absExtgroup = new SerialQueuedExecutorGroup();
        break;
      case OneTaskOnlyEx:
        absExtgroup = new OneTaskOnlyExecutorGroup();
        break;
      case ScheduledThreadEx:
        absExtgroup = new ScheduledThreadPoolExecutorWKAGroup();
        break;
      case AGSEx:
        absExtgroup = new GatewaySenderEventProcessorGroup();
        break;
      default:
        return false;
    }
    monitorMap.put(Thread.currentThread().getId(), absExtgroup);
    return true;
  }

  @Override
  public void endMonitor() {
    monitorMap.remove(Thread.currentThread().getId());
  }

}
