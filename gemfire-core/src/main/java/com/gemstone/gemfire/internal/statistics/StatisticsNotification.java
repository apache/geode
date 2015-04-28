/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.util.Iterator;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public interface StatisticsNotification extends Iterable<StatisticId> {
  
  public static enum Type {
    /** CounterMonitor threshold was exceeded */
    THRESHOLD_VALUE_EXCEEDED,
    /** GaugeMonitor low-threshold was exceeded */
    THRESHOLD_LOW_VALUE_EXCEEDED,
    /** GaugeMonitor high-threshold was exceeded */
    THRESHOLD_HIGH_VALUE_EXCEEDED,
    /** ValueMonitor expected value was matched */
    VALUE_MATCHED,
    /** ValueMonitor expected value was differed */
    VALUE_DIFFERED,
    /** ValueMonitor value(s) changed */
    VALUE_CHANGED
  }

  /** Returns the timestamp of the stat sample */
  public long getTimeStamp();

  /** Returns the notification type */
  public Type getType();

  /** Returns an iterator of all the stat instances that met the monitor's criteria */
  public Iterator<StatisticId> iterator();

  /** Returns an iterator of all the stat instances for the specified descriptor that met the monitor's criteria */
  public Iterator<StatisticId> iterator(StatisticDescriptor statDesc);

  /** Returns an iterator of all the stat instances for the specified statistics instance that met the monitor's criteria */
  public Iterator<StatisticId> iterator(Statistics statistics);

  /** Returns an iterator of all the stat instances for the specified statistics type that met the monitor's criteria */
  public Iterator<StatisticId> iterator(StatisticsType statisticsType);

  /** Returns the value for the specified stat instance */
  public Number getValue(StatisticId statId) throws StatisticNotFoundException;
}
