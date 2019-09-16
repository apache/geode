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
package org.apache.geode.cache.asyncqueue.internal;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class AsyncEventQueueStats extends GatewaySenderStats {

  public static final String typeName = "AsyncEventQueueStatistics";

  /** The <code>StatisticsType</code> of the statistics */
  @Immutable
  public static final StatisticsType type;


  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = createType(f, typeName, "Stats for activity in the AsyncEventQueue");
  }

  /**
   * Constructor.
   *
   * @param f The <code>StatisticsFactory</code> which creates the <code>Statistics</code> instance
   * @param asyncQueueId The id of the <code>AsyncEventQueue</code> used to generate the name of the
   *        <code>Statistics</code>
   */
  public AsyncEventQueueStats(StatisticsFactory f, String asyncQueueId,
      StatisticsClock statisticsClock) {
    super(f, "asyncEventQueueStats-", asyncQueueId, type, statisticsClock);
  }
}
