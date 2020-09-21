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
package org.apache.geode.management.internal.beans.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.StatisticsTest;

@Category(StatisticsTest.class)
public class GCStatsMonitorTest {
  @Rule
  public TestName testName = new TestName();

  private GCStatsMonitor gcStatsMonitor;

  @Before
  public void setUp() {
    gcStatsMonitor = new GCStatsMonitor(testName.getMethodName());
    assertThat(gcStatsMonitor).isNotNull();
    assertThat(gcStatsMonitor.getStatistic("collections")).isEqualTo(0L);
    assertThat(gcStatsMonitor.getStatistic("collectionTime")).isEqualTo(0L);
  }

  @Test
  public void getStatisticShouldReturnZeroForUnknownStatistics() {
    assertThat(gcStatsMonitor.getStatistic("unknownStatistic")).isEqualTo(0);
  }


}
