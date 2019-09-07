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

package org.apache.geode.pdx.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.geode.internal.cache.TXManagerImpl;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.pdx.PdxInitializationException;

public class PeerTypeRegistrationTest {

  private final PeerTypeRegistration peerTypeRegistration;

  public PeerTypeRegistrationTest() throws IOException, ClassNotFoundException {
    final InternalDistributedSystem internalDistributedSystem =
        mock(InternalDistributedSystem.class);
    final DistributionManager distributionManager = mock(DistributionManager.class);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    final StatisticsManager statisticsManager = mock(StatisticsManager.class);
    final StatisticsType statisticsType = mock(StatisticsType.class);
    when(statisticsManager.createType(any(), any(), any())).thenReturn(statisticsType);
    final Statistics statistics = mock(Statistics.class);
    when(statisticsManager.createAtomicStatistics(any(), any())).thenReturn(statistics);
    when(internalDistributedSystem.getStatisticsManager()).thenReturn(statisticsManager);
    final InternalCache internalCache = mock(InternalCache.class);
    when(internalCache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    @SuppressWarnings("unchecked")
    final Region<Object, Object> region = mock(Region.class);
    when(region.size()).thenReturn(1);
    when(internalCache.createVMRegion(eq(PeerTypeRegistration.REGION_NAME), any(), any()))
        .thenReturn(region);
    when(region.getRegionService()).thenReturn(internalCache);
    TXManagerImpl txManager = mock(TXManagerImpl.class);
    when(internalCache.getCacheTransactionManager()).thenReturn(txManager);
    peerTypeRegistration = new PeerTypeRegistration(internalCache);
  }

  @Test
  public void getLocalSizeThrowsWhenNotInitialized() {
    assertThatThrownBy(peerTypeRegistration::getLocalSize)
        .isInstanceOf(PdxInitializationException.class);
  }

  @Test
  public void getLocalSizeReturnsValueAfterInitialized() {
    peerTypeRegistration.initialize();
    assertThat(peerTypeRegistration.getLocalSize()).isEqualTo(1);
  }

}
