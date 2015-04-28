/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TODO: define another addStatistic for StatisticDescriptor which will enable 
 * static monitoring that will fire for all instances even ones that may not 
 * yet be created at the time this monitor is defined
 * 
 * @author Kirk Lund
 * @since 7.0
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
