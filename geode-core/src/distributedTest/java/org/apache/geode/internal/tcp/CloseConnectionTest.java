package org.apache.geode.internal.tcp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

public class CloseConnectionTest extends CacheTestCase {

  @Test(timeout = 60_000)
  public void sharedSenderShouldRecoverFromClosedSocket() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    // Create a region in each member. VM0 has a proxy region, so state must be in VM1
    vm0.invoke(() -> {
      getCache().createRegionFactory(RegionShortcut.REPLICATE_PROXY).create("region");
    });
    vm1.invoke(() -> {
      getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
    });


    // Force VM1 to close it's connections.
    vm1.invoke(() -> {
      ConnectionTable conTable = getConnectionTable();
      assertThat(conTable.getNumberOfReceivers()).isEqualTo(2);
      conTable.closeReceivers(false);
      assertThat(conTable.getNumberOfReceivers()).isEqualTo(0);
    });

    // See if VM0 noticed the closed connections. Try to do a couple of region
    // operations
    vm0.invoke(() -> {
      Region<Object, Object> region = getCache().getRegion("region");
      region.put("1", "1");

      assertThat(region.get("1")).isEqualTo("1");
    });

    // Make sure connections were reestablished
    vm1.invoke(() -> {
      ConnectionTable conTable = getConnectionTable();
      assertThat(conTable.getNumberOfReceivers()).isEqualTo(2);
    });
  }

  private ConnectionTable getConnectionTable() {
    ClusterDistributionManager cdm =
        (ClusterDistributionManager) getSystem().getDistributionManager();
    DistributionImpl distribution = (DistributionImpl) cdm.getDistribution();
    return distribution.getDirectChannel().getConduit().getConTable();
  }


}
