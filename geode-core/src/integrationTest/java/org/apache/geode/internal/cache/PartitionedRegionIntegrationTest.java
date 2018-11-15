package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class PartitionedRegionIntegrationTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withNoCacheServer().withAutoStart();

  @Test
  public void bucketSorterShutdownAfterRegionDestroy() {
    PartitionedRegion region =
        (PartitionedRegion) server.createRegion(RegionShortcut.PARTITION, "PR1",
            f -> f.setEvictionAttributes(
                EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY)));

    ScheduledExecutorService bucketSorter = region.getBucketSorter();
    assertThat(bucketSorter).isNotNull();

    region.destroyRegion();

    assertThat(bucketSorter.isShutdown()).isTrue();
  }

  @Test
  public void bucketSorterIsNotCreatedIfNoEviction() {
    PartitionedRegion region =
        (PartitionedRegion) server.createRegion(RegionShortcut.PARTITION, "PR1",
            rf -> rf.setOffHeap(false));
    ScheduledExecutorService bucketSorter = region.getBucketSorter();
    assertThat(bucketSorter).isNull();
  }
}
