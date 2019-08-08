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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.function.Function;

import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.internal.cache.LocalRegion.RegionMapConstructor;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.test.fake.Fakes;

/**
 * Unit tests for {@link AbstractRegion}.
 *
 * @since GemFire 8.1
 */
public class AbstractRegionJUnitTest {

  /**
   * Test method for {@link AbstractRegion#getExtensionPoint()}.
   *
   * Assert that method returns a {@link SimpleExtensionPoint} instance and assume that
   * {@link org.apache.geode.internal.cache.extension.SimpleExtensionPointJUnitTest} has covered the
   * rest.
   */
  @Test
  public void extensionPointIsSimpleExtensionPointByDefault() {
    AbstractRegion region = spy(AbstractRegion.class);
    ExtensionPoint<Region<?, ?>> extensionPoint = region.getExtensionPoint();
    assertThat(extensionPoint).isNotNull().isInstanceOf(SimpleExtensionPoint.class);
  }

  @Test
  public void getAllGatewaySenderIdsReturnsEmptySet() {
    AbstractRegion region = createTestableAbstractRegion();

    Set<String> result = region.getAllGatewaySenderIds();

    assertThat(result).isEmpty();
  }

  @Test
  public void getAllGatewaySenderIdsIncludesAsyncEventQueueId() {
    AbstractRegion region = createTestableAbstractRegion();
    region.addAsyncEventQueueId("asyncQueueId", false);
    String asyncQueueId = AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId("asyncQueueId");

    Set<String> result = region.getAllGatewaySenderIds();

    assertThat(result).containsExactlyInAnyOrder(asyncQueueId);
  }

  @Test
  public void getAllGatewaySenderIdsIncludesGatewaySenderIds() {
    AbstractRegion region = createTestableAbstractRegion();
    region.addGatewaySenderId("gatewaySenderId");

    Set<String> result = region.getAllGatewaySenderIds();

    assertThat(result).containsExactlyInAnyOrder("gatewaySenderId");
  }

  @Test
  public void getAllGatewaySenderIdsIncludesBothGatewaySenderIdsAndAsyncQueueIds() {
    AbstractRegion region = createTestableAbstractRegion();
    region.addGatewaySenderId("gatewaySenderId");
    region.addAsyncEventQueueId("asyncQueueId", false);
    String asyncQueueId = AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId("asyncQueueId");

    Set<String> result = region.getAllGatewaySenderIds();

    assertThat(result).containsExactlyInAnyOrder("gatewaySenderId", asyncQueueId);
  }

  private AbstractRegion createTestableAbstractRegion() {
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(regionAttributes.getDataPolicy()).thenReturn(DataPolicy.DEFAULT);
    EvictionAttributes evictionAttributes = mock(EvictionAttributes.class);
    when(evictionAttributes.getAction()).thenReturn(EvictionAction.NONE);
    when(regionAttributes.getEvictionAttributes()).thenReturn(evictionAttributes);
    ExpirationAttributes expirationAttributes = mock(ExpirationAttributes.class);
    when(regionAttributes.getRegionTimeToLive()).thenReturn(expirationAttributes);
    when(regionAttributes.getRegionIdleTimeout()).thenReturn(expirationAttributes);
    when(regionAttributes.getEntryTimeToLive()).thenReturn(expirationAttributes);
    when(regionAttributes.getEntryIdleTimeout()).thenReturn(expirationAttributes);
    DiskWriteAttributes diskWriteAttributes = mock(DiskWriteAttributes.class);
    when(regionAttributes.getDiskWriteAttributes()).thenReturn(diskWriteAttributes);
    RegionMapConstructor regionMapConstructor = mock(RegionMapConstructor.class);
    Function<LocalRegion, RegionPerfStats> regionPerfStatsFactory =
        (localRegion) -> mock(RegionPerfStats.class);
    AbstractRegion region = new LocalRegion("regionName", regionAttributes, null, Fakes.cache(),
        new InternalRegionArguments(), null, regionMapConstructor, null, null, null,
        regionPerfStatsFactory, disabledClock());
    return region;
  }
}
