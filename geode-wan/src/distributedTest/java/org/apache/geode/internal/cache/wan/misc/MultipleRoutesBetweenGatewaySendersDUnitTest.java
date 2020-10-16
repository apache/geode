package org.apache.geode.internal.cache.wan.misc;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.junit.categories.WanTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({ WanTest.class})
@RunWith(JUnitParamsRunner.class)
public class MultipleRoutesBetweenGatewaySendersDUnitTest extends WANTestBase {

  public MultipleRoutesBetweenGatewaySendersDUnitTest() {
    super();
  }

  @Test
  @Parameters({"true", "false"})
  public void testMultipleRoutesBetweenSendersStopOneRoute(boolean isParallel) {
    // Start locators
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm2.invoke(() -> createFirstRemoteLocator(3, lnPort));

    String regionName = getTestMethodName() + "_PR";
    int numPuts = 1000;

    // Configure ln site member
    // Note: Sender tk is not started
    vm3.invoke(() -> createCache(lnPort));
    vm3.invoke(() -> createReceiver());
    vm3.invoke(() -> createSender("ny", 2, isParallel, 100, 10, false, false, null, false));
    vm3.invoke(() -> createSender("tk", 3, isParallel, 100, 10, false, false, null, true));
    vm3.invoke(() -> createPartitionedRegion(regionName, "ny,tk", 0, 113, false));

    // Configure ny site member
    vm4.invoke(() -> createCache(nyPort));
    vm4.invoke(() -> createReceiver());
    vm4.invoke(() -> createSender("ln", 1, isParallel, 100, 10, false, false, null, false));
    vm4.invoke(() -> createSender("tk", 3, isParallel, 100, 10, false, false, null, false));
    vm4.invoke(() -> createPartitionedRegion(regionName, "ln,tk", 0, 113, false));

    // Configure tk site member
    vm5.invoke(() -> createCache(tkPort));
    vm5.invoke(() -> createReceiver());
    vm5.invoke(() -> createSender("ln", 1, isParallel, 100, 10, false, false, null, false));
    vm5.invoke(() -> createSender("ny", 2, isParallel, 100, 10, false, false, null, false));
    vm5.invoke(() -> createPartitionedRegion(regionName, "ln,ny", 0, 113, false));

    // Do puts
    vm3.invoke(() -> doPuts(regionName, numPuts));

    // Wait for ln site member queue to be empty
    // Note: Sender tk is not started
    vm3.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ny"));

    // Verify region sizes in all sites
    vm3.invoke(() -> validateRegionSize(regionName, numPuts));
    vm4.invoke(() -> validateRegionSize(regionName, numPuts));
    vm5.invoke(() -> validateRegionSize(regionName, numPuts));
  }
}
