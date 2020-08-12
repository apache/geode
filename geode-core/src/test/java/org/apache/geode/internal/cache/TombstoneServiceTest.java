package org.apache.geode.internal.cache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.distributed.internal.CacheTime;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;

public class TombstoneServiceTest {
  CacheTime cacheTime = mock(CacheTime.class);
  CachePerfStats stats = mock(CachePerfStats.class);
  CancelCriterion cancelCriterion = mock(CancelCriterion.class);
  ExecutorService executor = mock(ExecutorService.class);
  RegionMap regionMap = mock(RegionMap.class);
  RegionEntry entry = mock(RegionEntry.class);
  DistributedRegion region = mock(DistributedRegion.class);
  VersionTag destroyedVersion = mock(VersionTag.class);

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void validateThatRemoveIsNotCalledOnTombstoneInRegionThatIsNotInitialized() {

    when(region.isInitialized()).thenReturn(false);
    when(region.getRegionMap()).thenReturn(regionMap);
    TombstoneService.ReplicateTombstoneSweeper replicateTombstoneSweeper =
        new TombstoneService.ReplicateTombstoneSweeper(cacheTime, stats,
            cancelCriterion, executor);
    TombstoneService.Tombstone tombstone =
        new TombstoneService.Tombstone(entry, region, destroyedVersion);
    tombstone.entry = entry;

    replicateTombstoneSweeper.expireTombstone(tombstone);
    replicateTombstoneSweeper.expireBatch();
    verify(regionMap, Mockito.never()).removeTombstone(tombstone.entry, destroyedVersion, false,
        true);
  }

  @Test
  public void validateThatRemoveIsCalledOnTombstoneInRegionThatIsInitialized() {
    RegionVersionVector regionVersionVector = mock(RegionVersionVector.class);

    when(region.isInitialized()).thenReturn(true);
    when(region.getRegionMap()).thenReturn(regionMap);
    when(region.getVersionVector()).thenReturn(regionVersionVector);
    when(region.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_REPLICATE);
    when(region.getDiskRegion()).thenReturn(mock(DiskRegion.class));

    TombstoneService.ReplicateTombstoneSweeper replicateTombstoneSweeper =
        new TombstoneService.ReplicateTombstoneSweeper(cacheTime, stats,
            cancelCriterion, executor);
    TombstoneService.Tombstone tombstone =
        new TombstoneService.Tombstone(entry, region, destroyedVersion);
    tombstone.entry = entry;

    replicateTombstoneSweeper.expireTombstone(tombstone);
    replicateTombstoneSweeper.expireBatch();
    verify(region, Mockito.atLeastOnce()).getVersionVector();
    verify(regionVersionVector, Mockito.atLeastOnce()).recordGCVersion(tombstone.getMemberID(),
        tombstone.getRegionVersion());


  }
}
