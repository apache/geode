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
package org.apache.geode.internal.statistics;

import java.util.List;

import org.apache.geode.internal.concurrent.ConcurrentHashSet;

/**
 * TODO: define another addStatistic for StatisticDescriptor which will enable static monitoring
 * that will fire for all instances even ones that may not yet be created at the time this monitor
 * is defined
 *
 * @since GemFire 7.0
 */
public abstract class StatisticsMonitor {

  private final Object mutex = new Object();

  private final ConcurrentHashSet<StatisticsListener> listeners = new ConcurrentHashSet<>();

  private final ConcurrentHashSet<StatisticId> statisticIds = new ConcurrentHashSet<>();

  public StatisticsMonitor() {}

  public StatisticsMonitor addStatistic(final StatisticId statId) {
    if (statId == null) {
      throw new NullPointerException("StatisticId is null");
    }
    if (!this.statisticIds.contains(statId)) {
      this.statisticIds.add(statId);
    }
    return this;
  }

  public StatisticsMonitor removeStatistic(final StatisticId statId) {
    if (statId == null) {
      throw new NullPointerException("StatisticId is null");
    }
    if (this.statisticIds.contains(statId)) {
      this.statisticIds.remove(statId);
    }
    return this;
  }

  public void addListener(final StatisticsListener listener) {
    if (listener == null) {
      throw new NullPointerException("StatisticsListener is null");
    }
    synchronized (this.mutex) {
      if (!this.listeners.contains(listener)) {
        this.listeners.add(listener);
        getStatMonitorHandler().addMonitor(this);
      }
    }
  }

  public void removeListener(final StatisticsListener listener) {
    if (listener == null) {
      throw new NullPointerException("StatisticsListener is null");
    }
    synchronized (this.mutex) {
      if (this.listeners.contains(listener)) {
        this.listeners.remove(listener);
        if (this.listeners.isEmpty()) {
          try {
            getStatMonitorHandler().removeMonitor(this);
          } catch (IllegalStateException ignore) {
            // sample collector and handlers were closed (ok on removal)
          }
        }
      }
    }
  }

  /**
   * This method may be overridden but please ensure that you invoke super.monitor(long, List) from
   * this method in the subclass.
   *
   * @param millisTimeStamp the real time in millis of the sample
   * @param resourceInstances resources with one or more updated values
   */
  protected void monitor(final long millisTimeStamp,
      final List<ResourceInstance> resourceInstances) {
    monitorStatisticIds(millisTimeStamp, resourceInstances);
  }

  private void monitorStatisticIds(final long millisTimeStamp,
      final List<ResourceInstance> resourceInstances) {
    if (!this.statisticIds.isEmpty()) {
      // TODO:
    }
  }

  protected void notifyListeners(final StatisticsNotification notification) {
    for (StatisticsListener listener : this.listeners) {
      listener.handleNotification(notification);
    }
  }

  protected Object mutex() {
    return this.mutex;
  }

  StatMonitorHandler getStatMonitorHandler() {
    return SampleCollector.getStatMonitorHandler();
  }

  /** For testing only */
  ConcurrentHashSet<StatisticsListener> getStatisticsListenersSnapshot() {
    return this.listeners;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("statisticIds=").append(this.statisticIds);
    sb.append(", listeners=").append(this.listeners);
    final StringBuilder toAppend = appendToString();
    if (toAppend == null) {
      sb.append(", ").append(toAppend);
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Override to append to toString()
   */
  protected StringBuilder appendToString() {
    return null;
  }
}
