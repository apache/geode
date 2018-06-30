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
 * ClearRvvLockingDUnitTest.java
 *
 * Created on September 6, 2005, 2:57 PM
 */
package org.apache.geode.internal.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Test class to verify proper locking interaction between transactions and the CLEAR region
 * operation.
 *
 * GEODE-1740: It was observed that operations performed within a transaction were not holding
 * region modification locks for the duration of commit processing. This lock is used to ensure
 * region consistency during CLEAR processing. By not holding the lock for the duration of commit
 * processing, a window was opened that allowed region operations such as clear to occur in
 * mid-commit.
 *
 * The fix for GEODE-1740 was to acquire and hold read locks for any region involved in the commit.
 * This forces CLEAR to wait until commit processing is complete.
 */
@SuppressWarnings("serial")
@Category(DistributedTest.class)
public class ClearTXLockingDUnitTest extends JUnit4CacheTestCase {

  @Rule
  public transient JUnitSoftAssertions softly = new JUnitSoftAssertions();
  /*
   * This test performs operations within a transaction and during commit processing schedules a
   * clear to be performed on the relevant region. The scheduled clear should wait until commit
   * processing is complete before clearing the region. Failure to do so, would result in region
   * inconsistencies.
   */
  private static final String THE_KEY = "theKey";
  private static final String THE_VALUE = "theValue";
  private static final int NUMBER_OF_PUTS = 2;
  private static final String REGION_NAME1 = "testRegion1";
  private static final String REGION_NAME2 = "testRegion2";

  static Cache cache;
  static CountDownLatch opsLatch;
  private static CountDownLatch regionLatch;
  private static CountDownLatch verifyLatch;

  private VM vm0;
  private VM vm1;
  private VM opsVM;
  private VM regionVM;

  @Before
  public void setup() {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    createCache(vm0);
    createCache(vm1);
  }

  @Test
  public void testPutWithClearSameVM() {
    setupRegions(vm0, vm0);
    setClearHook(REGION_NAME1, opsVM, regionVM);
    performTestAndCheckResults();
  }

  @Test
  public void testPutWithClearDifferentVM() {
    setupRegions(vm0, vm1);
    setClearHook(REGION_NAME1, opsVM, regionVM);
    performTestAndCheckResults();
  }

  /*
   * The CLOSE tests are ignored until the close operation has been updated to acquire a write lock
   * during processing.
   */
  @Ignore
  @Test
  public void testPutWithCloseSameVM() {
    setupRegions(vm0, vm0);
    setCloseHook(REGION_NAME1, opsVM, regionVM);
    performTestAndCheckResults();
  }

  @Ignore
  @Test
  public void testPutWithCloseDifferentVM() {
    setupRegions(vm0, vm1);
    setCloseHook(REGION_NAME1, opsVM, regionVM);
    performTestAndCheckResults();
  }

  /*
   * The DESTROY_REGION tests are ignored until the destroy operation has been updated to acquire a
   * write lock during processing.
   */
  @Ignore
  @Test
  public void testPutWithDestroyRegionSameVM() {
    setupRegions(vm0, vm0);
    setDestroyRegionHook(REGION_NAME1, opsVM, regionVM);
    performTestAndCheckResults();
  }

  @Ignore
  @Test
  public void testPutWithDestroyRegionDifferentVM() {
    setupRegions(vm0, vm1);
    setDestroyRegionHook(REGION_NAME1, opsVM, regionVM);
    performTestAndCheckResults();
  }

  // Local methods

  /*
   * This method executes a runnable test and then checks for region consistency
   */
  private void performTestAndCheckResults() {
    try {
      opsVM.invoke(this::putOperationsTest);
      checkForConsistencyErrors(REGION_NAME1);
      checkForConsistencyErrors(REGION_NAME2);
    } finally {
      opsVM.invoke(() -> resetArmHook(REGION_NAME1));
    }
  }

  /*
   * Set which vm will perform the transaction and which will perform the region operation and
   * create the regions on the vms
   */
  private void setupRegions(VM opsTarget, VM regionTarget) {
    opsVM = opsTarget;
    regionVM = regionTarget;
    vm0.invoke(() -> createRegion(REGION_NAME1));
    vm0.invoke(() -> createRegion(REGION_NAME2));
    vm1.invoke(() -> createRegion(REGION_NAME1));
    vm1.invoke(() -> createRegion(REGION_NAME2));
  }

  private void putOperationsTest() {
    opsVM.invoke(() -> doPuts(getCache(), regionVM));
  }

  /*
   * Set arm hook to detect when region operation is attempting to acquire write lock and stage the
   * clear that will be released half way through commit processing.
   */
  private void setClearHook(String rname, VM whereOps, VM whereClear) {
    whereOps.invoke(() -> setArmHook(rname));
    whereClear.invokeAsync(() -> stageClear(rname, whereOps));
  }

  // remote test methods

  /*
   * Wait to be notified and then execute the clear. Once the clear completes, notify waiter to
   * perform region verification.
   */
  private static void stageClear(String rname, VM whereOps) throws InterruptedException {
    regionLatch = new CountDownLatch(1);
    regionOperationWait(regionLatch);
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    r.clear();
    whereOps.invoke(() -> releaseVerify());
  }

  /*
   * Set and stage method for close and destroy are the same as clear
   */
  private void setCloseHook(String rname, VM whereOps, VM whereClear) {
    whereOps.invoke(() -> setArmHook(rname));
    whereClear.invokeAsync(() -> stageClose(rname, whereOps));
  }

  private static void stageClose(String rname, VM whereOps) throws InterruptedException {
    regionLatch = new CountDownLatch(1);
    regionOperationWait(regionLatch);
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    r.close();
    whereOps.invoke(() -> releaseVerify());
  }

  private void setDestroyRegionHook(String rname, VM whereOps, VM whereClear) {
    whereOps.invoke(() -> setArmHook(rname));
    whereClear.invokeAsync(() -> stageDestroyRegion(rname, whereOps));
  }

  private static void stageDestroyRegion(String rname, VM whereOps) throws InterruptedException {
    regionLatch = new CountDownLatch(1);
    regionOperationWait(regionLatch);
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    r.destroyRegion();
    whereOps.invoke(() -> releaseVerify());
  }

  /*
   * Set the abstract region map lock hook to detect attempt to acquire write lock by region
   * operation.
   */
  private void setArmHook(String rname) {
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    ArmLockHook theArmHook = new ArmLockHook();
    ((AbstractRegionMap) r.entries).setARMLockTestHook(theArmHook);
  }

  /*
   * Cleanup arm lock hook by setting it null
   */
  private void resetArmHook(String rname) {
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    ((AbstractRegionMap) r.entries).setARMLockTestHook(null);
  }

  /*
   * Wait to be notified it is time to perform region operation (i.e. CLEAR)
   */
  private static void regionOperationWait(CountDownLatch latch) throws InterruptedException {
    latch.await();
    /*
     * regionLatch = new CountDownLatch(1); regionLatch.await();
     */
  }

  /*
   * A simple transaction that will have a region operation execute during commit. opsLatch is used
   * to wait until region operation has been scheduled during commit and verifyLatch is used to
   * ensure commit and clear processing have both completed.
   */
  private static void doPuts(Cache cache, VM whereRegion) throws InterruptedException {
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();

    opsLatch = new CountDownLatch(1);
    verifyLatch = new CountDownLatch(1);

    txManager.begin();
    TXStateInterface txState = ((TXStateProxyImpl) txManager.getTXState()).getRealDeal(null, null);
    ((TXState) txState).setDuringApplyChanges(new CommitTestCallback(whereRegion));

    Region region1 = cache.getRegion(REGION_NAME1);
    Region region2 = cache.getRegion(REGION_NAME2);
    for (int i = 0; i < NUMBER_OF_PUTS; i++) {
      region1.put(REGION_NAME1 + THE_KEY + i, THE_VALUE + i);
      region2.put(REGION_NAME2 + THE_KEY + i, THE_VALUE + i);
    }

    txManager.commit();
    verifyLatch.await();
  }

  /*
   * Release the region operation that has been previously staged
   */
  private static void releaseRegionOperation(VM whereRegion) {
    whereRegion.invoke(() -> regionLatch.countDown());
  }

  /*
   * Region operation has been scheduled, now resume commit processing
   */
  private static void releaseOps() {
    opsLatch.countDown();
  }

  /*
   * Notify waiter it is time to verify region contents
   */
  private static void releaseVerify() {
    verifyLatch.countDown();
  }

  private InternalDistributedMember createCache(VM vm) {
    return vm.invoke(() -> {
      cache = getCache(new CacheFactory().set("conserve-sockets", "true"));
      return getSystem().getDistributedMember();
    });
  }

  private static void createRegion(String rgnName) {
    RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.create(rgnName);
  }

  /*
   * Get region contents from each member and verify they are consistent
   */
  private void checkForConsistencyErrors(String regionName) {
    Map<Object, Object> r0Contents = vm0.invoke(() -> getRegionContents(regionName));
    Map<Object, Object> r1Contents = vm1.invoke(() -> getRegionContents(regionName));

    for (int i = 0; i < NUMBER_OF_PUTS; i++) {
      String theKey = regionName + THE_KEY + i;
      if (r0Contents.containsKey(theKey)) {
        softly.assertThat(r1Contents.get(theKey))
            .as("region contents are not consistent for key %s", theKey)
            .isEqualTo(r0Contents.get(theKey));
      } else {
        softly.assertThat(r1Contents).as("expected containsKey for %s to return false", theKey)
            .doesNotContainKey(theKey);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private static Map<Object, Object> getRegionContents(String rname) {
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    Map<Object, Object> result = new HashMap<>();
    for (Iterator i = r.entrySet().iterator(); i.hasNext();) {
      Region.Entry e = (Region.Entry) i.next();
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }

  /*
   * Test callback called for each operation during commit processing. Half way through commit
   * processing, release the region operation.
   */
  static class CommitTestCallback implements Runnable {
    private VM whereRegionOperation;
    private int callCount;
    /* entered twice for each put lap since there are 2 regions */
    private int releasePoint = NUMBER_OF_PUTS;

    CommitTestCallback(VM whereRegion) {
      whereRegionOperation = whereRegion;
      callCount = 0;
    }

    public void run() {
      callCount++;
      if (callCount == releasePoint) {
        releaseRegionOperation(whereRegionOperation);
        try {
          opsLatch.await();
        } catch (InterruptedException e) {
        }
      }
    }
  }

  /*
   * The region operations attempt to acquire the write lock will hang while commit processing is
   * occurring. Before this occurs, resume commit processing.
   */
  public class ArmLockHook extends ARMLockTestHookAdapter {
    @Override
    public void beforeLock(InternalRegion owner, CacheEvent event) {
      if (event != null) {
        if (event.getOperation().isClear() || event.getOperation().isRegionDestroy()
            || event.getOperation().isClose()) {
          releaseOps();
        }
      }
    }
  }

}
