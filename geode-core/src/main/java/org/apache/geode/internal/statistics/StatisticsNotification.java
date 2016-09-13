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

import java.util.Iterator;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;

/**
 * @since GemFire 7.0
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
