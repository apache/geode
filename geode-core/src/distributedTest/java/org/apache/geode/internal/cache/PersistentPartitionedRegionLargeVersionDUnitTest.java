package org.apache.geode.internal.cache;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

public class PersistentPartitionedRegionLargeVersionDUnitTest extends CacheTestCase {

  /**
   * Test that we can recover from disk files with versions larger than Integer.MAX_VALUE
   */
  @Test
  public void canRecoverWithLargeGCVersionInFiles() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    vm0.invoke(() -> {
      createRegion();
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region");
      region.put(0, 0);

      // Manually set the version in bucket 0 to be greater than Integer.MAX_VALUE
      RegionVersionVector vectorVector = region.getBucketRegion(0).getVersionVector();
      vectorVector.recordVersion(vectorVector.getOwnerId(), ((long) Integer.MAX_VALUE) + 10L);

      // Do an update, which will pick up the large version
      region.put(1, 0);

      // Do a destroy, and a tombstone gc, which will set the gc version to the large version
      region.destroy(1);
      getCache().getTombstoneService().forceBatchExpirationForTests(1);
    });

    // Start a second member to copy the bucket
    vm1.invoke(() -> {
      createRegion();
      getCache().getResourceManager().createRebalanceFactory().start().getResults();
    });

    // Shutdown both members
    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    // Restart both members
    vm1.invoke(() -> createRegion());
    vm0.invoke(() -> createRegion());
  }

  private void createRegion() {
    Cache cache = getCache();
    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(1).create())
        .create("region");
  }
}
