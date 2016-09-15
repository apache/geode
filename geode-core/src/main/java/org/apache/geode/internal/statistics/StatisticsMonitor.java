/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TODO: define another addStatistic for StatisticDescriptor which will enable 
 * static monitoring that will fire for all instances even ones that may not 
 * yet be created at the time this monitor is defined
 * 
 * @since GemFire 7.0
 */
public abstract class StatisticsMonitor {

  private final Object mutex = new Object();
  
  private volatile List<StatisticsListener> listeners = 
        Collections.<StatisticsListener>emptyList();

  private volatile List<StatisticId> statisticIds = 
        Collections.<StatisticId>emptyList();

  public StatisticsMonitor() {
  }

  public StatisticsMonitor addStatistic(StatisticId statId) {
    if (statId == null) {
      throw new NullPointerException("StatisticId is null");
    }
    synchronized (this.mutex) {
      List<StatisticId> oldStatisticIds = this.statisticIds;
      if (!oldStatisticIds.contains(statId)) {
        List<StatisticId> newStatisticIds = new ArrayList<StatisticId>(oldStatisticIds);
        newStatisticIds.add(statId);
        this.statisticIds = Collections.unmodifiableList(newStatisticIds);
      }
    }
    return this;
  }
  
  public StatisticsMonitor removeStatistic(StatisticId statId) {
    if (statId == null) {
      throw new NullPointerException("StatisticId is null");
    }
    synchronized (this.mutex) {
      List<StatisticId> oldStatisticIds = this.statisticIds;
      if (oldStatisticIds.contains(statId)) {
        List<StatisticId> newStatisticIds = new ArrayList<StatisticId>(oldStatisticIds);
        newStatisticIds.remove(statId);
        this.statisticIds = Collections.unmodifiableList(newStatisticIds);
      }
    }
    return this;
  }

  public final void addListener(StatisticsListener listener) {
    if (listener == null) {
      throw new NullPointerException("StatisticsListener is null");
    }
    synchronized (this.mutex) {
      List<StatisticsListener> oldListeners = this.listeners;
      if (!oldListeners.contains(listener)) {
        List<StatisticsListener> newListeners = new ArrayList<StatisticsListener>(oldListeners);
        newListeners.add(listener);
        this.listeners = Collections.unmodifiableList(newListeners);
        getStatMonitorHandler().addMonitor(this);
      }
    }
  }

  public final void removeListener(StatisticsListener listener) {
    if (listener == null) {
      throw new NullPointerException("StatisticsListener is null");
    }
    synchronized (this.mutex) {
      List<StatisticsListener> oldListeners = this.listeners;
      if (oldListeners.contains(listener)) {
        List<StatisticsListener> newListeners = new ArrayList<StatisticsListener>(oldListeners);
        newListeners.remove(listener);
        if (newListeners.isEmpty()) {
          try {
            getStatMonitorHandler().removeMonitor(this);
          } catch (IllegalStateException ignore) {
            // sample collector and handlers were closed (ok on removal)
          }
        }
        this.listeners = Collections.unmodifiableList(newListeners);
      }
    }
  }
  
  /**
   * This method may be overridden but please ensure that you invoke 
   * super.monitor(long, List) from this method in the subclass.
   * 
   * @param millisTimeStamp the real time in millis of the sample
   * @param resourceInstances resources with one or more updated values
   */
  protected void monitor(long millisTimeStamp, List<ResourceInstance> resourceInstances) {
    monitorStatisticIds(millisTimeStamp, resourceInstances);
  }
  
  private final void monitorStatisticIds(long millisTimeStamp, List<ResourceInstance> resourceInstances) {
    List<StatisticId> statisticIdsToMonitor = statisticIds;
    if (!statisticIdsToMonitor.isEmpty()) {
      // TODO:
    }
  }
  
  protected final void notifyListeners(StatisticsNotification notification) {
    List<StatisticsListener> listenersToNotify = this.listeners;    
    for (StatisticsListener listener : listenersToNotify) {
      listener.handleNotification(notification);
    }
  }
  
  protected final Object mutex() {
    return this.mutex;
  }

  StatMonitorHandler getStatMonitorHandler() {
    return SampleCollector.getStatMonitorHandler();
  }
  
  /** For testing only */
  List<StatisticsListener> getStatisticsListenersSnapshot() {
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
  
  /** Override to append to toString() */
  protected StringBuilder appendToString() {
    return null;
  }
}
