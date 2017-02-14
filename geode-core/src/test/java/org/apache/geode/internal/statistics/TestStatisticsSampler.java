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

/**
 * @since GemFire 7.0
 */
@SuppressWarnings("unchecked")
public class TestStatisticsSampler implements StatisticsSampler {

  private final StatisticsManager manager;

  public TestStatisticsSampler(final StatisticsManager manager) {
    this.manager = manager;
  }

  @Override
  public int getStatisticsModCount() {
    return this.manager.getStatListModCount();
  }

  @Override
  public Statistics[] getStatistics() {
    List<Statistics> statsList = this.manager.getStatsList();
    synchronized (statsList) {
      return statsList.toArray(new Statistics[statsList.size()]);
    }
  }

  @Override
  public boolean waitForSample(final long timeout) throws InterruptedException {
    return false;
  }

  @Override
  public SampleCollector waitForSampleCollector(final long timeout) throws InterruptedException {
    return null;
  }
}
