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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.statistics.TestSampleHandler.Info;
import org.apache.geode.internal.statistics.TestSampleHandler.ResourceInstanceInfo;
import org.apache.geode.internal.statistics.TestSampleHandler.ResourceTypeInfo;
import org.apache.geode.internal.statistics.TestSampleHandler.SampledInfo;

/**
 * Unit tests for {@link SampleCollector}.
 *
 * @since GemFire 7.0
 */
public class SampleCollectorTest {

  private TestStatisticsManager manager;
  private SampleCollector sampleCollector;

  @Before
  public void setUp() throws Exception {
    final long startTime = System.currentTimeMillis();
    this.manager = new TestStatisticsManager(1, getClass().getSimpleName(), startTime);

    final StatArchiveHandlerConfig mockStatArchiveHandlerConfig =
        mock(StatArchiveHandlerConfig.class,
            getClass().getSimpleName() + "$" + StatArchiveHandlerConfig.class.getSimpleName());
    when(mockStatArchiveHandlerConfig.getArchiveFileName()).thenReturn(new File(""));
    when(mockStatArchiveHandlerConfig.getArchiveFileSizeLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getArchiveDiskSpaceLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemId()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemStartTime()).thenReturn(startTime);
    when(mockStatArchiveHandlerConfig.getSystemDirectoryPath()).thenReturn("");
    when(mockStatArchiveHandlerConfig.getProductDescription())
        .thenReturn(getClass().getSimpleName());

    final StatisticsSampler sampler = new TestStatisticsSampler(manager);
    this.sampleCollector = new SampleCollector(sampler);
    this.sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime(),
        new MainWithChildrenRollingFileHandler());
  }

  @After
  public void tearDown() throws Exception {
    if (this.sampleCollector != null) {
      this.sampleCollector.close();
      this.sampleCollector = null;
    }
    this.manager = null;
  }

  @Test
  public void testAddHandlerBeforeSample() {
    TestSampleHandler handler = new TestSampleHandler();
    this.sampleCollector.addSampleHandler(handler);

    StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        manager.createIntCounter("ST1_1_name", "ST1_1_desc", "ST1_1_units")};
    StatisticsType ST1 = manager.createType("ST1_name", "ST1_desc", statsST1);
    Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1_text", 1);

    this.sampleCollector.sample(NanoTimer.getTime());

    assertEquals(3, handler.getNotificationCount());
    List<Info> notifications = handler.getNotifications();

    // validate the allocatedResourceType notification
    assertTrue(notifications.get(0) instanceof ResourceTypeInfo);
    ResourceTypeInfo allocatedResourceTypeInfo = (ResourceTypeInfo) notifications.get(0);
    assertNotNull(allocatedResourceTypeInfo);
    assertEquals("allocatedResourceType", allocatedResourceTypeInfo.getName());
    ResourceType resourceType = allocatedResourceTypeInfo.getResourceType();
    assertNotNull(resourceType);
    assertEquals(0, resourceType.getId());
    assertEquals(1, resourceType.getStatisticDescriptors().length);
    StatisticsType statisticsType = resourceType.getStatisticsType();
    assertNotNull(statisticsType);
    assertTrue(statisticsType == ST1);
    assertEquals("ST1_name", statisticsType.getName());
    assertEquals("ST1_desc", statisticsType.getDescription());
    assertEquals(1, statisticsType.getStatistics().length);

    // validate the allocatedResourceInstance notification
    assertTrue(notifications.get(1) instanceof ResourceInstanceInfo);
    ResourceInstanceInfo allocatedResourceInstanceInfo =
        (ResourceInstanceInfo) notifications.get(1);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    ResourceInstance resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(0, resourceInstance.getId());
    assertEquals(1, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    Statistics statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st1_1);
    assertEquals(1, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st1_1_text", statistics.getTextId());
    assertEquals("ST1_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());

    // validate the sampled notification
    assertTrue(notifications.get(2) instanceof SampledInfo);
    SampledInfo sampledInfo = (SampledInfo) notifications.get(2);
    assertNotNull(sampledInfo);
    assertEquals("sampled", sampledInfo.getName());
    assertEquals(1, sampledInfo.getResourceCount());
  }

  @Test
  public void testAddHandlerAfterSamples() {
    StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        manager.createIntCounter("ST1_1_name", "ST1_1_desc", "ST1_1_units")};
    StatisticsType ST1 = manager.createType("ST1_name", "ST1_desc", statsST1);
    Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1_text", 1);
    Statistics st1_2 = manager.createAtomicStatistics(ST1, "st1_2_text", 1);

    StatisticDescriptor[] statsST2 = new StatisticDescriptor[] {
        manager.createIntCounter("ST2_1_name", "ST2_1_desc", "ST2_1_units")};
    StatisticsType ST2 = manager.createType("ST2_name", "ST2_desc", statsST2);
    Statistics st2_1 = manager.createAtomicStatistics(ST2, "st2_1_text", 1);

    st1_1.incInt("ST1_1_name", 1);
    st1_2.incInt("ST1_1_name", 1);
    st2_1.incInt("ST2_1_name", 1);
    long sampleTime = NanoTimer.getTime();
    this.sampleCollector.sample(sampleTime);

    st1_1.incInt("ST1_1_name", 2);
    st2_1.incInt("ST2_1_name", 1);
    sampleTime += NanoTimer.millisToNanos(1000);
    this.sampleCollector.sample(sampleTime);

    st1_1.incInt("ST1_1_name", 1);
    st1_1.incInt("ST1_1_name", 2);
    sampleTime += NanoTimer.millisToNanos(1000);
    this.sampleCollector.sample(sampleTime);

    TestSampleHandler handler = new TestSampleHandler();
    this.sampleCollector.addSampleHandler(handler);

    assertEquals("TestSampleHandler = " + handler, 0, handler.getNotificationCount());

    st1_2.incInt("ST1_1_name", 1);
    st2_1.incInt("ST2_1_name", 1);
    sampleTime += NanoTimer.millisToNanos(1000);
    this.sampleCollector.sample(sampleTime);

    assertEquals(6, handler.getNotificationCount());
    List<Info> notifications = handler.getNotifications();

    // validate the allocatedResourceType notification for ST1
    int notificationIdx = 0;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceTypeInfo);
    ResourceTypeInfo allocatedResourceTypeInfo =
        (ResourceTypeInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceTypeInfo);
    assertEquals("allocatedResourceType", allocatedResourceTypeInfo.getName());
    ResourceType resourceType = allocatedResourceTypeInfo.getResourceType();
    assertNotNull(resourceType);
    assertEquals(0, resourceType.getId());
    assertEquals(1, resourceType.getStatisticDescriptors().length);
    StatisticsType statisticsType = resourceType.getStatisticsType();
    assertNotNull(statisticsType);
    assertTrue(statisticsType == ST1);
    assertEquals("ST1_name", statisticsType.getName());
    assertEquals("ST1_desc", statisticsType.getDescription());
    assertEquals(1, statisticsType.getStatistics().length);

    // validate the allocatedResourceInstance notification for st1_1
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceInstanceInfo);
    ResourceInstanceInfo allocatedResourceInstanceInfo =
        (ResourceInstanceInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    ResourceInstance resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(0, resourceInstance.getId());
    assertEquals(1, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    Statistics statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st1_1);
    assertEquals(1, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st1_1_text", statistics.getTextId());
    assertEquals("ST1_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());

    // validate the allocatedResourceInstance notification for st1_2
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceInstanceInfo);
    allocatedResourceInstanceInfo = (ResourceInstanceInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(1, resourceInstance.getId());
    assertEquals(1, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st1_2);
    assertEquals(2, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st1_2_text", statistics.getTextId());
    assertEquals("ST1_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());

    // validate the allocatedResourceType notification for ST2
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceTypeInfo);
    allocatedResourceTypeInfo = (ResourceTypeInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceTypeInfo);
    assertEquals("allocatedResourceType", allocatedResourceTypeInfo.getName());
    resourceType = allocatedResourceTypeInfo.getResourceType();
    assertNotNull(resourceType);
    assertEquals(1, resourceType.getId());
    assertEquals(1, resourceType.getStatisticDescriptors().length);
    statisticsType = resourceType.getStatisticsType();
    assertNotNull(statisticsType);
    assertTrue(statisticsType == ST2);
    assertEquals("ST2_name", statisticsType.getName());
    assertEquals("ST2_desc", statisticsType.getDescription());
    assertEquals(1, statisticsType.getStatistics().length);

    // validate the allocatedResourceInstance notification for st2_1
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceInstanceInfo);
    allocatedResourceInstanceInfo = (ResourceInstanceInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(2, resourceInstance.getId());
    assertEquals(1, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st2_1);
    assertEquals(3, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st2_1_text", statistics.getTextId());
    assertEquals("ST2_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());

    // validate the sampled notification
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof SampledInfo);
    SampledInfo sampledInfo = (SampledInfo) notifications.get(notificationIdx);
    assertNotNull(sampledInfo);
    assertEquals("sampled", sampledInfo.getName());
    assertEquals(3, sampledInfo.getResourceCount());
  }

  @Test
  public void testGetStatMonitorHandler() {
    StatMonitorHandler handler = SampleCollector.getStatMonitorHandler();
    assertNotNull(handler);
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    assertNull(handler.getStatMonitorNotifier());
  }

  @Test
  public void testGetStatMonitorHandlerAfterClose() {
    this.sampleCollector.close();
    try {
      /* StatMonitorHandler handler = */ SampleCollector.getStatMonitorHandler();
      fail(
          "getStatMonitorHandler should throw IllegalStateException when SampleCollector is closed");
    } catch (IllegalStateException expected) {
      // passed
    }
  }

  @Test
  public void testGetStatMonitorHandlerBeforeAndAfterClose() {
    StatMonitorHandler handler = SampleCollector.getStatMonitorHandler();
    assertNotNull(handler);
    this.sampleCollector.close();
    try {
      handler = SampleCollector.getStatMonitorHandler();
      fail(
          "getStatMonitorHandler should throw IllegalStateException when SampleCollector is closed");
    } catch (IllegalStateException expected) {
      // passed
    }
  }

  @Test
  public void testGetStatArchiveHandler() {
    StatArchiveHandler handler = this.sampleCollector.getStatArchiveHandler();
    assertNotNull(handler);
  }
}
