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

import static org.apache.geode.test.dunit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Test class to verify proper RVV locking interaction between entry operations such as PUT/REMOVE
 * and the CLEAR region operation
 *
 * GEODE-599: After an operation completed, it would unlock the RVV. This was occurring before the
 * operation was distributed to other members which created a window in which another operation
 * could be performed prior to that operation being distributed.
 *
 * The fix for GEODE-599 was to not release the lock until after distributing the operation to the
 * other members.
 *
 */

@SuppressWarnings("serial")

public class ClearRvvLockingDUnitTest extends JUnit4CacheTestCase {

  @Rule
  public transient JUnitSoftAssertions softly = new JUnitSoftAssertions();
  /*
   * The tests perform a single operation and a concurrent clear.
   *
   * opsVM determines where the single operation will be performed, null will perform the op on the
   * test VM (vm0) clearVM determines where the clear operation will be performed, null will perform
   * the clear on the test VM (vm0)
   *
   * Specifying NULL/NULL for opsVM and clearVM has the effect of performing both in the same thread
   * whereas specifying vm0/vm0 for example will run them both on the same vm, but different
   * threads. NULL/NULL is not tested here since the same thread performing a clear prior to
   * returning from a put is not possible using the public API.
   *
   * Each test is performed twice once with operation and clear on the same vm, once on different
   * vms.
   *
   */
  VM vm0, vm1, opsVM, clearVM;

  static Cache cache;
  static LocalRegion region;
  DistributedMember vm0ID, vm1ID;

  static AbstractRegionMap.ARMLockTestHook theHook;

  static final String THE_KEY = "theKey";
  static final String THE_VALUE = "theValue";

  private static final Logger logger = LogService.getLogger();

  // test methods

  @Test
  public void testPutOperationSameVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm0); // first arg is where to perform operation, second arg where to
                                 // perform clear
      opsVM.invoke(() -> setBasicHook(opsVM));
      runConsistencyTest(vm0, performPutOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testPutOperationDifferentVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm1); // first arg is where to perform operation, second arg where to
                                 // perform clear
      opsVM.invoke(() -> setBasicHook(clearVM));
      runConsistencyTest(vm0, performPutOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testPutOperationNoAck() {
    try {
      setupNoAckMembers();
      setOpAndClearVM(vm0, vm0);
      vm0.invoke(() -> setLocalNoAckHook(vm1));
      vm1.invoke(() -> setRemoteNoAckHook(vm0));
      vm0.invoke(() -> primeStep1(1));
      vm1.invoke(() -> primeStep2(1));
      runConsistencyTest(vm0, performNoAckPutOperation);
      checkForConsistencyErrors();
    } finally {
      vm0.invoke(this::resetHook);
      vm1.invoke(this::resetHook);
    }
  }

  @Test
  public void testRemoveOperationSameVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm0);
      opsVM.invoke(() -> setRemoveAndInvalidateHook(clearVM));
      runConsistencyTest(vm0, performRemoveOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testRemoveOperationDifferentVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm1);
      opsVM.invoke(() -> setRemoveAndInvalidateHook(clearVM));
      runConsistencyTest(vm0, performRemoveOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testInvalidateOperationSameVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm0);
      opsVM.invoke(() -> setRemoveAndInvalidateHook(clearVM));
      runConsistencyTest(vm0, performInvalidateOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testInvalidateOperationDifferentVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm1);
      opsVM.invoke(() -> setRemoveAndInvalidateHook(clearVM));
      runConsistencyTest(vm0, performInvalidateOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testPutAllOperationSameVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm0);
      opsVM.invoke(() -> setBulkHook(vm0));
      runConsistencyTest(vm0, performPutAllOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testPutAllOperationDifferentVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm1);
      opsVM.invoke(() -> setBulkHook(vm0));
      runConsistencyTest(vm0, performPutAllOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testRemoveAllOperationSameVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm0);
      opsVM.invoke(() -> setBulkHook(vm0));
      runConsistencyTest(vm0, performRemoveAllOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }

  @Test
  public void testRemoveAllOperationDifferentVM() {
    try {
      setupMembers();
      setOpAndClearVM(vm0, vm1);
      opsVM.invoke(() -> setBulkHook(vm0));
      runConsistencyTest(vm0, performRemoveAllOperation);
      checkForConsistencyErrors();
    } finally {
      opsVM.invoke(this::resetHook);
    }
  }


  private void invokePut(VM whichVM) {
    if (whichVM == null) {
      doPut();
    } else {
      whichVM.invoke(ClearRvvLockingDUnitTest::doPut);
    }
  }

  private void invokeRemove(VM whichVM) {
    if (whichVM == null) {
      doRemove();
    } else {
      whichVM.invoke(ClearRvvLockingDUnitTest::doRemove);
    }
  }

  private void invokeInvalidate(VM whichVM) {
    if (whichVM == null) {
      doInvalidate();
    } else {
      whichVM.invoke(ClearRvvLockingDUnitTest::doInvalidate);
    }
  }

  private void invokePutAll(VM whichVM) {
    if (whichVM == null) {
      doPutAll();
    } else {
      whichVM.invoke(ClearRvvLockingDUnitTest::doPutAll);
    }
  }

  private void invokeRemoveAll(VM whichVM) {
    if (whichVM == null) {
      doRemoveAll();
    } else {
      whichVM.invoke(ClearRvvLockingDUnitTest::doRemoveAll);
    }
  }

  private static void invokeClear(VM whichVM) {
    if (whichVM == null) {
      doClear();
    } else {
      whichVM.invoke(ClearRvvLockingDUnitTest::doClear);
    }
  }

  // remote test methods

  private static boolean doesRegionEntryExist(String key) {
    return region.getRegionEntry(key) != null;
  }

  private static void doPut() {
    region.put(THE_KEY, THE_VALUE);
  }

  private static void doRemove() {
    region.remove(THE_KEY);
  }

  private static void doInvalidate() {
    region.invalidate(THE_KEY);
  }

  private static void doPutAll() {
    Map<Object, Object> map = generateKeyValues();
    region.putAll(map, "putAllCallback");
  }

  private static void doRemoveAll() {
    Map<Object, Object> map = generateKeyValues();
    region.removeAll(map.keySet(), "removeAllCallback");
  }

  private static void doClear() {
    region.clear();
  }

  private static void primeStep1(int cnt) {
    primeStep1Latch(cnt);
  }

  private static void primeStep2(int cnt) {
    primeStep2Latch(cnt);
  }

  private static void releaseStep1() {
    decrementStep1Latch();
  }

  SerializableRunnable performPutOperation = new SerializableRunnable("perform PUT") {
    @Override
    public void run() {
      try {
        invokePut(opsVM);
      } catch (Exception e) {
        fail("while performing PUT", e);
      }
    }
  };

  SerializableRunnable performNoAckPutOperation = new SerializableRunnable("perform NoAckPUT") {
    @Override
    public void run() throws InterruptedException {
      Runnable putThread1 = () -> {
        DistributedSystem.setThreadsSocketPolicy(false);
        doPut();
        DistributedSystem.releaseThreadsSockets();
      };

      Runnable putThread2 = () -> {
        DistributedSystem.setThreadsSocketPolicy(false);
        awaitStep1Latch();
        doClear();
        DistributedSystem.releaseThreadsSockets();
      };

      Thread t1 = new Thread(putThread1);
      Thread t2 = new Thread(putThread2);
      t2.start();
      t1.start();
      t1.join();
      t2.join();
    }
  };

  SerializableRunnable performRemoveOperation = new SerializableRunnable("perform REMOVE") {
    @Override
    public void run() {
      try {
        invokePut(opsVM);
        invokeRemove(opsVM);
      } catch (Exception e) {
        fail("while performing REMOVE", e);
      }
    }
  };

  SerializableRunnable performInvalidateOperation = new SerializableRunnable("perform INVALIDATE") {
    @Override
    public void run() {
      try {
        invokePut(opsVM);
        invokeInvalidate(opsVM);
      } catch (Exception e) {
        fail("while performing INVALIDATE", e);
      }
    }
  };

  SerializableRunnable performPutAllOperation = new SerializableRunnable("perform PUTALL") {
    @Override
    public void run() {
      try {
        invokePutAll(opsVM);
      } catch (Exception e) {
        fail("while performing PUTALL", e);
      }
    }
  };

  SerializableRunnable performRemoveAllOperation = new SerializableRunnable("perform REMOVEALL") {
    @Override
    public void run() {
      try {
        invokePutAll(opsVM);
        invokeRemoveAll(opsVM);
      } catch (Exception e) {
        fail("while performing REMOVEALL", e);
      }
    }
  };

  // helper methods

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
    String testName = getName();
    vm0.invoke(() -> createRegion(testName));
    vm1.invoke(() -> createRegion(testName));
  }

  private void setupNoAckMembers() {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm0ID = createNoConserveSocketsCache(vm0);
    vm1ID = createNoConserveSocketsCache(vm1);
    String testName = getName();
    vm0.invoke(() -> createNOACKRegion(testName));
    vm1.invoke(() -> createNOACKRegion(testName));
  }

  private void runConsistencyTest(VM vm, SerializableRunnableIF theTest) {
    vm.invoke(theTest);
  }

  private void checkForConsistencyErrors() {
    Map<Object, Object> r0Contents = vm0.invoke(ClearRvvLockingDUnitTest::getRegionContents);
    Map<Object, Object> r1Contents = vm1.invoke(ClearRvvLockingDUnitTest::getRegionContents);

    String key = THE_KEY;
    softly.assertThat(r1Contents.get(key)).as("region contents are not consistent for key %s", key)
        .isEqualTo(r0Contents.get(key));
    softly.assertThat(checkRegionEntry(vm1, key))
        .as("region entries are not consistent for key %s", key)
        .isEqualTo(checkRegionEntry(vm0, key));

    for (int subi = 1; subi < 3; subi++) {
      String subkey = key + "-" + subi;
      if (r0Contents.containsKey(subkey)) {
        softly.assertThat(r1Contents.get(subkey))
            .as("region contents are not consistent for key %s", subkey)
            .isEqualTo(r0Contents.get(subkey));
      } else {
        softly.assertThat(r1Contents).as("expected containsKey for %s to return false", subkey)
            .doesNotContainKey(subkey);
      }
    }
  }

  public void resetHook() {
    ((AbstractRegionMap) region.entries).setARMLockTestHook(null);
  }

  public void setBasicHook(VM whichVM) {
    theOtherVM = whichVM;
    theHook = new ArmBasicClearHook();
    ((AbstractRegionMap) region.entries).setARMLockTestHook(theHook);
  }

  public void setRemoveAndInvalidateHook(VM whichVM) {
    theOtherVM = whichVM;
    theHook = new ArmRemoveAndInvalidateClearHook();
    ((AbstractRegionMap) region.entries).setARMLockTestHook(theHook);
  }

  public void setRemoteNoAckHook(VM whichVM) {
    theOtherVM = whichVM;
    theHook = new ArmNoAckRemoteHook();
    ((AbstractRegionMap) region.entries).setARMLockTestHook(theHook);
  }

  public void setLocalNoAckHook(VM whichVM) {
    theOtherVM = whichVM;
    theHook = new ArmNoAckLocalHook();
    ((AbstractRegionMap) region.entries).setARMLockTestHook(theHook);
  }

  public void setBulkHook(VM whichVM) {
    theOtherVM = whichVM;
    theHook = new ArmBulkClearHook();
    ((AbstractRegionMap) region.entries).setARMLockTestHook(theHook);
  }

  private InternalDistributedMember createCache(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        cache = getCache(new CacheFactory().set("conserve-sockets", "true"));
        return getSystem().getDistributedMember();
      }
    });
  }

  private InternalDistributedMember createNoConserveSocketsCache(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        cache = getCache(new CacheFactory().set("conserve-sockets", "false"));
        return getSystem().getDistributedMember();
      }
    });
  }

  private static void createRegion(String rgnName) {
    RegionFactory<Object, Object> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setConcurrencyChecksEnabled(true);
    rf.setScope(Scope.DISTRIBUTED_ACK);
    region = (LocalRegion) rf.create(rgnName);
  }

  private static void createNOACKRegion(String rgnName) {
    RegionFactory<Object, Object> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setConcurrencyChecksEnabled(true);
    rf.setScope(Scope.DISTRIBUTED_NO_ACK);
    region = (LocalRegion) rf.create(rgnName);
  }

  private static Map<Object, Object> generateKeyValues() {
    String key = THE_KEY;
    String value = THE_VALUE;
    Map<Object, Object> map = new HashMap<>();
    map.put(key, value);
    map.put(key + "-1", value + "-1");
    map.put(key + "-2", value + "-2");
    return map;
  }

  @SuppressWarnings("rawtypes")
  private static Map<Object, Object> getRegionContents() {
    Map<Object, Object> result = new HashMap<>();
    for (final Object o : region.entrySet()) {
      Region.Entry e = (Region.Entry) o;
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }

  private boolean checkRegionEntry(VM vm, String key) {
    boolean target = vm.invoke(() -> doesRegionEntryExist(key));
    return target;
  }

  static VM theOtherVM;
  static transient CountDownLatch step1Latch, step2Latch;

  public static void primeStep1Latch(int waitCount) {
    step1Latch = new CountDownLatch(waitCount);
  }

  public static void awaitStep1Latch() {
    try {
      step1Latch.await();
    } catch (InterruptedException ignored) {
    }
  }

  public static void decrementStep1Latch() {
    step1Latch.countDown();
  }

  public static void decrementRemoteStep1Latch() {
    theOtherVM.invoke(ClearRvvLockingDUnitTest::decrementStep1Latch);
  }

  public static void primeStep2Latch(int waitCount) {
    step2Latch = new CountDownLatch(waitCount);
  }

  public static void awaitStep2Latch() {
    try {
      step2Latch.await();
    } catch (InterruptedException ignored) {
    }
  }

  public static void decrementStep2Latch() {
    step2Latch.countDown();
  }

  public static void decrementRemoteStep2Latch() {
    theOtherVM.invoke(ClearRvvLockingDUnitTest::decrementStep2Latch);
  }

  /*
   * Test callback class used to hook the rvv locking mechanism with basic operations.
   */
  public static class ArmBasicClearHook extends ARMLockTestHookAdapter {
    @Override
    public void afterRelease(InternalRegion owner, CacheEvent event) {
      if ((event.getOperation().isCreate()) && owner.getName().startsWith("test")) {
        invokeClear(theOtherVM);
      }
    }
  }

  /*
   * Test callback class used to hook the rvv locking mechanism with basic operations.
   */
  public static class ArmRemoveAndInvalidateClearHook extends ARMLockTestHookAdapter {

    @Override
    public void afterRelease(InternalRegion owner, CacheEvent event) {
      if ((event.getOperation().isDestroy() || event.getOperation().isInvalidate())
          && owner.getName().startsWith("test")) {
        invokeClear(theOtherVM);
      }
    }
  }

  /*
   * Test callback class used to hook the rvv locking mechanism for NOACK testing.
   */
  public static class ArmNoAckRemoteHook extends ARMLockTestHookAdapter {
    @Override
    public void beforeLock(InternalRegion owner, CacheEvent event) {
      if (event.isOriginRemote() && event.getOperation().isCreate()
          && owner.getName().startsWith("test")) {
        theOtherVM.invoke(ClearRvvLockingDUnitTest::releaseStep1); // start clear
        awaitStep2Latch(); // wait for clear to complete
      }
    }
  }

  public static class ArmNoAckLocalHook extends ARMLockTestHookAdapter {
    @Override
    public void beforeStateFlushWait() {
      decrementRemoteStep2Latch();
    }
  }

  /*
   * Test callback class used to hook the rvv locking mechanism with bulk operations.
   */
  public static class ArmBulkClearHook extends ARMLockTestHookAdapter {
    @Override
    public void afterBulkRelease(InternalRegion region) {
      invokeClear(theOtherVM);
    }
  }
}
