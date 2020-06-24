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
package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.statistics.StatisticsManagerFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;

/**
 * Unit tests for {@link InternalDistributedSystem}.
 */
public class InternalDistributedSystemStatisticsManagerTest {

  private static final String STATISTIC_NAME = "statistic-name";
  private static final String STATISTIC_DESCRIPTION = "statistic-description";
  private static final String STATISTIC_UNITS = "statistic-units";
  private static final StatisticsType STATISTICS_TYPE = mock(StatisticsType.class);
  private static final String STATISTICS_TEXT_ID = "statistics-text-id";
  private static final long STATISTICS_NUMERIC_ID = 2349;

  @Mock(name = "autowiredDistributionManager")
  private DistributionManager distributionManager;

  @Mock(name = "autowiredStatisticsManagerFactory")
  private StatisticsManagerFactory statisticsManagerFactory;

  @Mock(name = "autowiredStatisticsManager")
  private StatisticsManager statisticsManager;

  private InternalDistributedSystem internalDistributedSystem;

  @Before
  public void setUp() {
    initMocks(this);
    when(statisticsManagerFactory.create(any(), anyLong(), anyBoolean()))
        .thenReturn(statisticsManager);
    internalDistributedSystem =
        new InternalDistributedSystem.BuilderForTesting(new Properties(),
            new ServiceLoaderModuleService(LogService.getLogger()))
                .setDistributionManager(distributionManager)
                .setStatisticsManagerFactory(statisticsManagerFactory)
                .build();
  }

  @Test
  public void createsStatisticsManagerViaFactory() {
    StatisticsManagerFactory statisticsManagerFactory =
        mock(StatisticsManagerFactory.class, "statisticsManagerFactory");
    StatisticsManager statisticsManagerCreatedByFactory =
        mock(StatisticsManager.class, "statisticsManagerCreatedByFactory");
    when(statisticsManagerFactory
        .create(any(), anyLong(), eq(false)))
            .thenReturn(statisticsManagerCreatedByFactory);

    InternalDistributedSystem result =
        new InternalDistributedSystem.BuilderForTesting(new Properties(),
            new ServiceLoaderModuleService(LogService.getLogger()))
                .setDistributionManager(distributionManager)
                .setStatisticsManagerFactory(statisticsManagerFactory)
                .build();

    assertThat(result.getStatisticsManager())
        .isSameAs(statisticsManagerCreatedByFactory);
  }

  @Test
  public void delegatesCreateTypeToStatisticsManager() {
    String typeName = "type-name";
    String typeDescription = "type-description";
    StatisticDescriptor[] descriptors = {};
    StatisticsType typeReturnedByManager = mock(StatisticsType.class);
    when(statisticsManager.createType(typeName, typeDescription, descriptors))
        .thenReturn(typeReturnedByManager);

    StatisticsType result =
        internalDistributedSystem.createType(typeName, typeDescription, descriptors);

    assertThat(result)
        .isSameAs(typeReturnedByManager);
  }

  @Test
  public void delegatesCreateTypeFromXmlToStatisticsManager() throws IOException {
    Reader reader = new StringReader("<arbitrary-xml/>");
    StatisticsType[] typesReturnedByManager = {};

    when(statisticsManager.createTypesFromXml(same(reader)))
        .thenReturn(typesReturnedByManager);

    StatisticsType[] result = internalDistributedSystem.createTypesFromXml(reader);

    assertThat(result)
        .isSameAs(typesReturnedByManager);
  }

  @Test
  public void delegatesFindTypeToStatisticsManager() {
    String soughtTypeName = "the-name";
    StatisticsType typeReturnedByManager = mock(StatisticsType.class);
    when(statisticsManager.findType(soughtTypeName))
        .thenReturn(typeReturnedByManager);

    StatisticsType result = internalDistributedSystem.findType(soughtTypeName);

    assertThat(result)
        .isSameAs(typeReturnedByManager);
  }

  @Test
  public void delegatesCreateIntCounterToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(
        statisticsManager.createLongCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createIntCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLongCounterToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createLongCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createLongCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateDoubleCounterToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createDoubleCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createDoubleCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateIntGaugeToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createLongGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createIntGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLongGaugeToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager.createLongGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS))
        .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createLongGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateDoubleGaugeToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createDoubleGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createDoubleGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLargerBetterIntCounterToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createLongCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createIntCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLargerBetterLongCounterToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createLongCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createLongCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLargerBetterDoubleCounterToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createDoubleCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createDoubleCounter(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLargerBetterIntGaugeToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createLongGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createIntGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLargerBetterLongGaugeToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createLongGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createLongGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateLargerBetterDoubleGaugeToStatisticsManager() {
    StatisticDescriptor descriptorReturnedByManager = mock(StatisticDescriptor.class);
    when(statisticsManager
        .createDoubleGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false))
            .thenReturn(descriptorReturnedByManager);

    StatisticDescriptor result = internalDistributedSystem
        .createDoubleGauge(STATISTIC_NAME, STATISTIC_DESCRIPTION, STATISTIC_UNITS, false);

    assertThat(result)
        .isSameAs(descriptorReturnedByManager);
  }

  @Test
  public void delegatesCreateStatisticsToStatisticsManager() {
    Statistics statisticsReturnedByManager = mock(Statistics.class);
    when(statisticsManager.createStatistics(STATISTICS_TYPE))
        .thenReturn(statisticsReturnedByManager);

    Statistics result = internalDistributedSystem
        .createStatistics(STATISTICS_TYPE);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesCreateStatisticsWithTextIdToStatisticsManager() {
    Statistics statisticsReturnedByManager = mock(Statistics.class);
    when(statisticsManager.createStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID))
        .thenReturn(statisticsReturnedByManager);

    Statistics result = internalDistributedSystem
        .createStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesCreateStatisticsWithNumericIdToStatisticsManager() {
    Statistics statisticsReturnedByManager = mock(Statistics.class);
    when(statisticsManager
        .createStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID))
            .thenReturn(statisticsReturnedByManager);

    Statistics result = internalDistributedSystem
        .createStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesCreateAtomicStatisticsToStatisticsManager() {
    Statistics statisticsReturnedByManager = mock(Statistics.class);
    when(statisticsManager.createAtomicStatistics(STATISTICS_TYPE))
        .thenReturn(statisticsReturnedByManager);

    Statistics result = internalDistributedSystem
        .createAtomicStatistics(STATISTICS_TYPE);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesCreateAtomicStatisticsWithTextIdToStatisticsManager() {
    Statistics statisticsReturnedByManager = mock(Statistics.class);
    when(statisticsManager.createAtomicStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID))
        .thenReturn(statisticsReturnedByManager);

    Statistics result = internalDistributedSystem
        .createAtomicStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesCreateAtomicStatisticsWithNumericIdToStatisticsManager() {
    Statistics statisticsReturnedByManager = mock(Statistics.class);
    when(statisticsManager
        .createAtomicStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID))
            .thenReturn(statisticsReturnedByManager);

    Statistics result = internalDistributedSystem
        .createAtomicStatistics(STATISTICS_TYPE, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesFindStatisticsByTypeToStatisticsManager() {
    Statistics[] statisticsReturnedByManager = {};
    when(statisticsManager.findStatisticsByType(STATISTICS_TYPE))
        .thenReturn(statisticsReturnedByManager);

    Statistics[] result = internalDistributedSystem
        .findStatisticsByType(STATISTICS_TYPE);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesFindStatisticsByTextIdToStatisticsManager() {
    Statistics[] statisticsReturnedByManager = {};
    String soughtTextId = "sought-text-id";
    when(statisticsManager.findStatisticsByTextId(soughtTextId))
        .thenReturn(statisticsReturnedByManager);

    Statistics[] result = internalDistributedSystem
        .findStatisticsByTextId(soughtTextId);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }

  @Test
  public void delegatesFindStatisticsByNumericIdToStatisticsManager() {
    Statistics[] statisticsReturnedByManager = {};
    long soughtNumericId = 836282;

    when(statisticsManager.findStatisticsByNumericId(soughtNumericId))
        .thenReturn(statisticsReturnedByManager);

    Statistics[] result = internalDistributedSystem
        .findStatisticsByNumericId(soughtNumericId);

    assertThat(result)
        .isSameAs(statisticsReturnedByManager);
  }
}
