package org.apache.geode.internal.cache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
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
    verify(regionMap, Mockito.never()).removeTombstone(tombstone.entry, tombstone, false,
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


    replicateTombstoneSweeper.expireTombstone(tombstone);
    replicateTombstoneSweeper.expireBatch();
    verify(regionMap, Mockito.times(1)).removeTombstone(tombstone.entry, tombstone, false,
        true);
  }
}
