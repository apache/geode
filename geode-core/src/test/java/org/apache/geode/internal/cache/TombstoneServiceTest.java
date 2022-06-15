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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.api.DataPolicy;
import org.apache.geode.distributed.internal.CacheTime;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;

public class TombstoneServiceTest {
  CacheTime cacheTime;
  CachePerfStats stats;
  CancelCriterion cancelCriterion;
  ExecutorService executor;
  RegionMap regionMap;
  RegionEntry entry;
  DistributedRegion region;
  VersionTag destroyedVersion;
  private TombstoneService.ReplicateTombstoneSweeper replicateTombstoneSweeper;
  private TombstoneService.Tombstone tombstone;


  @Before
  public void setUp() throws Exception {
    cacheTime = mock(CacheTime.class);
    stats = mock(CachePerfStats.class);
    cancelCriterion = mock(CancelCriterion.class);
    executor = mock(ExecutorService.class);
    regionMap = mock(RegionMap.class);
    entry = mock(RegionEntry.class);
    region = mock(DistributedRegion.class);
    destroyedVersion = mock(VersionTag.class);
    replicateTombstoneSweeper = new TombstoneService.ReplicateTombstoneSweeper(cacheTime, stats,
        cancelCriterion, executor);
    tombstone = new TombstoneService.Tombstone(entry, region, destroyedVersion);
    tombstone.entry = entry;
  }

  @Test
  public void validateThatRemoveIsNotCalledOnTombstoneInRegionThatIsNotInitialized() {
    when(region.isInitialized()).thenReturn(false);
    when(region.getRegionMap()).thenReturn(regionMap);

    replicateTombstoneSweeper.expireTombstone(tombstone);
    replicateTombstoneSweeper.expireBatch();
    verify(regionMap, Mockito.never()).removeTombstone(tombstone.entry, tombstone);
  }

  @Test
  public void validateThatRemoveIsCalledOnTombstoneInRegionThatIsInitialized() {
    RegionVersionVector regionVersionVector = mock(RegionVersionVector.class);

    when(region.isInitialized()).thenReturn(true);
    when(region.getRegionMap()).thenReturn(regionMap);
    when(region.getVersionVector()).thenReturn(regionVersionVector);
    when(region.getDataPolicyEnum()).thenReturn(DataPolicy.PERSISTENT_REPLICATE);
    when(region.getDiskRegion()).thenReturn(mock(DiskRegion.class));


    replicateTombstoneSweeper.expireTombstone(tombstone);
    replicateTombstoneSweeper.expireBatch();
    verify(regionMap).removeTombstone(tombstone.entry, tombstone);
  }
}
