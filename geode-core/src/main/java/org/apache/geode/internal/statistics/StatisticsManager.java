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

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;

/**
 * Defines the contract that a statistics factory must implement for its Statistics instances. This
 * is an internal implementation specific interface.
 */
public interface StatisticsManager extends StatisticsFactory, OsStatisticsFactory {
  /**
   * Called when the Statistics instance <code>s</code> is closed.
   */
  void destroyStatistics(Statistics s);

  /**
   * Returns a name that can be used to identify the manager.
   */
  String getName();

  /**
   * Returns the pid for this process.
   */
  int getPid();

  /**
   * Returns the start time of this manager.
   */
  long getStartTime();

  /**
   * Returns a value that changes any time a Statistics instance is added or removed from this
   * manager.
   */
  int getStatListModCount();

  /**
   * Returns a list of all the Statistics this manager is currently managing.
   */
  List<Statistics> getStatsList();

  /**
   * Returns the current number of statistics instances.
   */
  int getStatisticsCount();

  /**
   * Returns the statistics resource instance given its id.
   */
  Statistics findStatisticsByUniqueId(long uniqueId);

  /**
   * Returns true if the specified statistic resource still exists.
   */
  boolean statisticsExists(long id);

  /**
   * Returns an array of all the current statistic resource instances.
   */
  Statistics[] getStatistics();
}
