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
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.internal.metrics.MemberMetricsSession;
import org.apache.geode.internal.statistics.StatisticsManagerFactory;

public class InternalDistributedSystemMemberMetricsSessionTest {
  @Mock
  public StatisticsManagerFactory statisticsManagerFactory;

  @Mock
  public DistributionManager distributionManager;

  @Before
  public void setup() {
    initMocks(this);
  }

  @Test
  public void delegatesGetMeterRegistryToMemberMetricsSession() {
    Properties memberProperties = new Properties();
    ConnectionConfig memberConfig = new ConnectionConfigImpl(memberProperties);
    MemberMetricsSession metricsSession =
        new MemberMetricsSession(memberConfig.distributionConfig());

    InternalDistributedSystem internalDistributedSystem =
        InternalDistributedSystem.newInstanceForTesting(distributionManager, memberProperties,
            statisticsManagerFactory, metricsSession);

    assertThat(internalDistributedSystem.getMeterRegistry())
        .isSameAs(metricsSession.meterRegistry());
  }
}
