/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.api.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.cache.ha.HARegionQueue;

public class HARegionTest {
  private HARegion region;

  private final InternalCache cache = mock(InternalCache.class, RETURNS_DEEP_STUBS);
  private final RegionAttributes attributes = mock(RegionAttributes.class, RETURNS_DEEP_STUBS);
  private final EvictionAttributes evictionAttributes =
      mock(EvictionAttributes.class, RETURNS_DEEP_STUBS);

  @Before
  public void setup() {
    when(attributes.getDataPolicyEnum()).thenReturn(DataPolicy.REPLICATE);
    when(attributes.getEvictionAttributes()).thenReturn(evictionAttributes);
    when(attributes.getLoadFactor()).thenReturn(0.75f);
    when(attributes.getConcurrencyLevel()).thenReturn(16);
    when(evictionAttributes.getAlgorithm()).thenReturn(EvictionAlgorithm.NONE);
    when(evictionAttributes.getAction()).thenReturn(EvictionAction.NONE);
    Set<String> asyncEventQueueIds = Collections.singleton("id");
    when(attributes.getAsyncEventQueueIds()).thenReturn(asyncEventQueueIds);
    region = new HARegion("HARegionTest_region", attributes, null, cache, disabledClock());
  }

  @Test
  public void getOwnerWithWaitReturnsHARegionQueueIfInitializedWithWait() {
    long timeout = 1;
    HARegionQueue queue = mock(HARegionQueue.class);
    when(queue.isQueueInitializedWithWait(timeout)).thenReturn(true);

    region.setOwner(queue);

    assertThat(region.getOwnerWithWait(timeout)).isEqualTo(queue);
  }

  @Test
  public void getOwnerWithWaitReturnsNullIfNotInitializedWithWait() {
    long timeout = 1;
    HARegionQueue queue = mock(HARegionQueue.class);
    when(queue.isQueueInitializedWithWait(timeout)).thenReturn(false);

    region.setOwner(queue);

    assertThat(region.getOwnerWithWait(timeout)).isEqualTo(null);
  }

}
