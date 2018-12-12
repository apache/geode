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
/*
 * Created on Sep 13, 2005
 *
 *
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.cache.InitialImageOperation.slowImageSleeps;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class MapClearGIIDUnitTest extends JUnit4CacheTestCase {

  protected static boolean wasGIIInProgressDuringClear = false;

  static volatile Region region;

  public static boolean checkImageStateFlag() throws Exception {
    Region rgn = new MapClearGIIDUnitTest().getCache().getRegion("/map");
    if (rgn == null) {
      fail("Map region not yet created");
    }
    if (((LocalRegion) rgn).getImageState().getClearRegionFlag()) {
      fail("The image state clear region flag should have been cleared" + " (region size="
          + rgn.size() + ")." + " Hence failing");
    }
    if (!wasGIIInProgressDuringClear) {
      fail("The clear operation invoked from VM1 reached VM0 after the "
          + "GII completed, or it reached VM0 even before the region in "
          + " VM0 got inserted in the subregion Map" + " (region size=" + rgn.size() + ")."
          + " Hence failing");
    }
    if (rgn.size() != 0) {
      fail("The clear operation invoked from VM1 should have made the "
          + "size of region zero. Hence failing. Size = " + rgn.size());
    }
    return true;
  }

  public static void createRegionInVm0() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    RegionAttributes attr = factory.create();

    region = new MapClearGIIDUnitTest().getCache().createRegion("map", attr);

    LogWriterUtils.getLogWriter().info("Region in VM0 created ");
  }

  public static void clearRegionInVm1() {
    // wait for profile of getInitialImage cache to show up
    final org.apache.geode.internal.cache.CacheDistributionAdvisor adv =
        ((org.apache.geode.internal.cache.DistributedRegion) region).getCacheDistributionAdvisor();
    final int expectedProfiles = 1;
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        int numProfiles;
        numProfiles = adv.adviseReplicates().size();
        return numProfiles == expectedProfiles;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    region.clear();
    assertEquals(0, region.size());
  }

  // test methods
  @Test
  public void testClearImageStateFlag() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    // vm0.invoke(() -> MapClearGIIDUnitTest.createCacheVM0());

    vm0.invoke(new CacheSerializableRunnable("createCacheVM0") {
      public void run2() throws CacheException {
        InitialImageOperation.slowImageProcessing = 10;
        InitialImageOperation.slowImageSleeps = 0;
        Properties mprops = new Properties();
        // mprops.setProperty(DistributionConfig.SystemConfigurationProperties.MCAST_PORT, "7777");

        getSystem(mprops);
        // ds = DistributedSystem.connect(props);
        getCache();
        CacheObserverImpl observer = new CacheObserverImpl();
        CacheObserverHolder.setInstance(observer);
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      }
    });
    vm1.invoke(new CacheSerializableRunnable("createCacheVM1") {
      public void run2() throws CacheException {
        Properties mprops = new Properties();
        // mprops.setProperty(DistributionConfig.SystemConfigurationProperties.MCAST_PORT, "7777");
        getSystem(mprops);
        // ds = DistributedSystem.connect(null);
        getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        factory.setConcurrencyChecksEnabled(true);
        RegionAttributes attr = factory.create();
        region = createRootRegion("map", attr);
        // region = region.createSubregion("map",attr);
        for (int i = 0; i < 10000; ++i) {
          region.put("" + i, "" + i);
        }
      }
    });
    LogWriterUtils.getLogWriter().info("Cache created in VM1 successfully");
    try {
      AsyncInvocation asyncGII = vm0.invokeAsync(() -> MapClearGIIDUnitTest.createRegionInVm0());
      // wait until vm0's gii has done 20 slow image sleeps (10ms*20 = 200ms)
      // before starting the clear
      vm0.invoke(new CacheSerializableRunnable("wait for sleeps") {
        public void run2() throws CacheException {
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return slowImageSleeps >= 20;
            }

            public String description() {
              return null;
            }
          };
          GeodeAwaitility.await().untilAsserted(ev);
        }
      });
      // now that the gii has received some entries do the clear
      vm1.invoke(() -> MapClearGIIDUnitTest.clearRegionInVm1());
      // wait for GII to complete
      ThreadUtils.join(asyncGII, 30 * 1000);
      if (asyncGII.exceptionOccurred()) {
        Throwable t = asyncGII.getException();
        Assert.fail("createRegionInVM0 failed", t);
      }
      assertTrue(vm0.invoke(() -> MapClearGIIDUnitTest.checkImageStateFlag()));

      if (asyncGII.exceptionOccurred()) {
        Assert.fail("asyncGII failed", asyncGII.getException());
      }


    } catch (Exception e) {
      Assert.fail("Test failed", e);
    } finally {
      vm0.invoke(new SerializableRunnable("Set fast image processing") {
        public void run() {
          InitialImageOperation.slowImageProcessing = 0;
          InitialImageOperation.slowImageSleeps = 0;
        }
      });

    }
  }// end of test case

  public static class CacheObserverImpl extends CacheObserverAdapter {

    public void afterRegionClear(RegionEvent event) {
      LogWriterUtils.getLogWriter().info("**********Received clear event in VM0 . ");
      Region rgn = event.getRegion();
      wasGIIInProgressDuringClear = ((LocalRegion) rgn).getImageState().wasRegionClearedDuringGII();
      InitialImageOperation.slowImageProcessing = 0;
      InitialImageOperation.slowImageSleeps = 0;
      LogWriterUtils.getLogWriter()
          .info("wasGIIInProgressDuringClear when clear event was received= "
              + wasGIIInProgressDuringClear);
    }
  }
}// end of test class
