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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.statistics.StatisticsManagerFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;

public class InternalDistributedSystemBuilderForTestingIntegrationTest {

  @Test
  public void builderForTesting() {
    Properties configProperties = new Properties();

    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    StatisticsManager statisticsManager = mock(StatisticsManager.class);
    StatisticsManagerFactory statisticsManagerFactory = mock(StatisticsManagerFactory.class);
    when(statisticsManagerFactory.create(any(), anyLong(), anyBoolean()))
        .thenReturn(statisticsManager);

    InternalDistributedSystem system =
        new InternalDistributedSystem.BuilderForTesting(configProperties,
            new ServiceLoaderModuleService(LogService.getLogger()))
                .setDistributionManager(distributionManager)
                .setStatisticsManagerFactory(statisticsManagerFactory)
                .build();

    assertThat(system.isConnected()).isTrue();
    assertThat(system.getDistributionManager()).isSameAs(distributionManager);
    assertThat(system.getStatisticsManager()).isSameAs(statisticsManager);
  }
}
