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

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Test class to verify proper locking interaction between transactions and the CLEAR region
 * operation.
 * 
 * GEODE-1740: It was observed that region operations performed within a transaction were not
 * acquiring region modification locks during commit processing. This lock is used to ensure region
 * consistency during CLEAR processing.
 * 
 * The fix for GEODE-1740 was to acquire cache modification locks for regions involved during commit
 * processing. A read lock is taken for each region involved thus allowing concurrent region
 * operations while preventing CLEAR from being able to acquire the write lock and thus waiting
 * until transaction processing is complete.
 * 
 */
@SuppressWarnings("serial")
@Category(DistributedTest.class)
public class ClearTXLockingDUnitTest extends JUnit4CacheTestCase {

  @Rule
  public transient JUnitSoftAssertions softly = new JUnitSoftAssertions();
  /*
   * This test performs region operations within a transaction and during commit processing
   * schedules a clear to be performed on the relevant region. The scheduled clear should wait until
   * commit processing is complete before clearing the region. Failure to do so, will result in
   * region inconsistencies.
   */
  VM vm0, vm1, opsVM, clearVM;

  static Cache cache;

  DistributedMember vm0ID, vm1ID;

  static CacheTransactionManager txmgr;
  static ArmBasicClearHook theHook;

  static final String THE_KEY = "theKey";
  static final String THE_VALUE = "theValue";
  static final int NUMBER_OF_PUTS = 5;

  static final String REGION_NAME1 = "testRegion1";
  static final String REGION_NAME2 = "testRegion2";

  static CountDownLatch clearLatch;

  private static final Logger logger = LogService.getLogger();

  // test methods

  @Test
  public void testPutOperationSameVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm0);
      opsVM.invoke(() -> setBasicHook(REGION_NAME1, opsVM, clearVM));
      runLockingTest(vm0, performPutOperation);
      checkForConsistencyErrors(REGION_NAME1);
      checkForConsistencyErrors(REGION_NAME2);
    } finally {
      opsVM.invoke(() -> resetHook(REGION_NAME1));
    }
  }

  @Test
  public void testPutOperationDifferentVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm1);
      opsVM.invoke(() -> setBasicHook(REGION_NAME1, opsVM, clearVM));
      runLockingTest(vm0, performPutOperation);
      checkForConsistencyErrors(REGION_NAME1);
      checkForConsistencyErrors(REGION_NAME2);
    } finally {
      opsVM.invoke(() -> resetHook(REGION_NAME1));
    }
  }

  // Local methods
  
  private void setOpAndClearVM(VM opsTarget, VM clearTarget) {
    opsVM = opsTarget;
    clearVM = clearTarget;
  }

  private void setupMembers() {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm0ID = createCache(vm0);
    vm1ID = createCache(vm1);
    vm0.invoke(() -> createRegion(REGION_NAME1));
    vm0.invoke(() -> createRegion(REGION_NAME2));
    vm1.invoke(() -> createRegion(REGION_NAME1));
    vm1.invoke(() -> createRegion(REGION_NAME2));
  }

  private void runLockingTest(VM vm, SerializableRunnableIF theTest) {
    vm.invoke(theTest);
  }

  private void checkForConsistencyErrors(String rname) {
    Map<Object, Object> r0Contents = (Map<Object, Object>) vm0.invoke(() -> getRegionContents(rname));
    Map<Object, Object> r1Contents = (Map<Object, Object>) vm1.invoke(() -> getRegionContents(rname));

    for (int i = 0; i < NUMBER_OF_PUTS; i++) {
      String theKey = rname + THE_KEY + i;
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

  /*
   * Test callback class used to hook the rvv locking mechanism with basic operations.
   */
  public class ArmBasicClearHook extends ARMLockTestHookAdapter {
    int txCalls = 0;
    int releasePoint = NUMBER_OF_PUTS / 2;
    CountDownLatch putLatch = new CountDownLatch(1);

    @Override
    public void beforeLock(LocalRegion owner, CacheEvent event) {
      if (event == null) {
        txCalls++;
        if (txCalls == releasePoint) {
          clearVM.invoke(() -> releaseClear());
          try {
            putLatch.await();
          } catch (InterruptedException e) {
          }
        }
      } else if (event.getOperation().isClear()) {
        putLatch.countDown();
      }
    }
  }

  SerializableRunnable performPutOperation = new SerializableRunnable("perform PUT") {
    @Override
    public void run() {
      opsVM.invoke(() -> doPuts(getCache()));
    }
  };

  // remote test methods

  public void setBasicHook(String rname, VM whereOps, VM whereClear) {
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    theHook = new ArmBasicClearHook();
    ((AbstractRegionMap) r.entries).setARMLockTestHook(theHook);
    whereClear.invokeAsync(() -> stageClear(rname));
  }

  public void resetHook(String rname) {
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    ((AbstractRegionMap) r.entries).setARMLockTestHook(null);
  }

  private static void doPuts(Cache cache) {
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();

    txManager.begin();

    Region region = cache.getRegion(REGION_NAME1);
    for (int i = 0; i < NUMBER_OF_PUTS; i++) {
      region.put(REGION_NAME1 + THE_KEY + i, THE_VALUE + i);
    }

    region = cache.getRegion(REGION_NAME2);
    for (int i = 0; i < NUMBER_OF_PUTS; i++) {
      region.put(REGION_NAME2 + THE_KEY + i, THE_VALUE + i);
    }

    txManager.commit();
  }

  private static void stageClear(String rname) throws InterruptedException {
    LocalRegion r = (LocalRegion) cache.getRegion(rname);
    clearLatch = new CountDownLatch(1);
    clearLatch.await();
    r.clear();
  }

  private static void releaseClear() {
    clearLatch.countDown();
  }

  private InternalDistributedMember createCache(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable<Object>() {
      public Object call() {
        cache = getCache(new CacheFactory().set("conserve-sockets", "true"));
        return getSystem().getDistributedMember();
      }
    });
  }

  private static void createRegion(String rgnName) {
    RegionFactory<Object, Object> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setConcurrencyChecksEnabled(true);
    rf.setScope(Scope.DISTRIBUTED_ACK);
    rf.create(rgnName);
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
}
