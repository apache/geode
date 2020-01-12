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
package org.apache.geode.cache.query.internal.index;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionEntry;

public class IndexManagerTest {
  private IndexManager indexManager;

  @Before
  public void setUp() {
    Region region = mock(Region.class);
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(regionAttributes.getIndexMaintenanceSynchronous()).thenReturn(true);
    when(regionAttributes.getEvictionAttributes()).thenReturn(mock(EvictionAttributes.class));
    when(regionAttributes.getEvictionAttributes().getAction())
        .thenReturn(EvictionAction.DEFAULT_EVICTION_ACTION);
    when(region.getAttributes()).thenReturn(regionAttributes);

    indexManager = new IndexManager(mock(InternalCache.class), region);
  }

  @Test
  public void addIndexMappingShouldMarkIndexAsInvalidWhenAddMappingOperationFails()
      throws IMQException {
    RegionEntry mockEntry = mock(RegionEntry.class);
    AbstractIndex mockIndex = mock(AbstractIndex.class);
    mockIndex.prIndex = mock(AbstractIndex.class);
    when(mockIndex.addIndexMapping(any())).thenThrow(new IMQException("Mock Exception"));

    assertThatCode(() -> indexManager.addIndexMapping(mockEntry, mockIndex))
        .doesNotThrowAnyException();
    verify(mockIndex, times(1)).markValid(false);
    verify((AbstractIndex) mockIndex.prIndex, times(1)).markValid(false);
  }

  @Test
  public void removeIndexMappingShouldMarkIndexAsInvalidWhenRemoveMappingOperationFails()
      throws IMQException {
    RegionEntry mockEntry = mock(RegionEntry.class);
    AbstractIndex mockIndex = mock(AbstractIndex.class);
    mockIndex.prIndex = mock(AbstractIndex.class);
    when(mockIndex.removeIndexMapping(mockEntry, 1)).thenThrow(new IMQException("Mock Exception"));

    assertThatCode(() -> indexManager.removeIndexMapping(mockEntry, mockIndex, 1))
        .doesNotThrowAnyException();
    verify(mockIndex, times(1)).markValid(false);
    verify((AbstractIndex) mockIndex.prIndex, times(1)).markValid(false);
  }
}
