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
package org.apache.geode.management.internal.beans;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.statistics.SampleCollector;
import org.apache.geode.internal.statistics.StatArchiveHandlerConfig;
import org.apache.geode.internal.statistics.StatisticsSampler;
import org.apache.geode.internal.statistics.TestStatisticsManager;
import org.apache.geode.internal.statistics.TestStatisticsSampler;
import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class GatewaySenderMBeanBridgeTest {
  private GatewaySender gatewaySender;
  private GatewaySenderMBeanBridge bridge;

  @Before
  public void before() throws Exception {
    gatewaySender = mock(AbstractGatewaySender.class);
    GatewaySenderStats gwstats = mock(GatewaySenderStats.class);
    when(((AbstractGatewaySender) gatewaySender).getStatistics()).thenReturn(gwstats);
    Statistics stats = mock(Statistics.class);
    when(gwstats.getStats()).thenReturn(stats);

    final long startTime = System.currentTimeMillis();
    TestStatisticsManager manager =
        new TestStatisticsManager(1, getClass().getSimpleName(), startTime);
    StatArchiveHandlerConfig mockStatArchiveHandlerConfig = mock(StatArchiveHandlerConfig.class,
        getClass().getSimpleName() + "$" + StatArchiveHandlerConfig.class.getSimpleName());
    when(mockStatArchiveHandlerConfig.getArchiveFileName()).thenReturn(new File(""));
    when(mockStatArchiveHandlerConfig.getArchiveFileSizeLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getArchiveDiskSpaceLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemId()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemStartTime()).thenReturn(startTime);
    when(mockStatArchiveHandlerConfig.getSystemDirectoryPath()).thenReturn("");
    when(mockStatArchiveHandlerConfig.getProductDescription())
        .thenReturn(getClass().getSimpleName());

    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    SampleCollector sampleCollector = new SampleCollector(sampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime(),
        new MainWithChildrenRollingFileHandler());

    StatisticsType type = mock(StatisticsType.class);
    when(stats.getType()).thenReturn(type);
    StatisticDescriptor[] descriptors = {};
    when(type.getStatistics()).thenReturn(descriptors);

    bridge = new GatewaySenderMBeanBridge(gatewaySender);
  }

  @Test
  public void testStart() {
    bridge.start();
    verify(gatewaySender).start();
  }

  @Test
  public void testStartWithCleanQueue() {
    bridge.startWithCleanQueue();
    verify(gatewaySender).startWithCleanQueue();
  }

}
