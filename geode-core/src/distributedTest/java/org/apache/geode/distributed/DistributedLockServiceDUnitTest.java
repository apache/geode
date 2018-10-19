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
package org.apache.geode.distributed;

import static java.lang.Boolean.TRUE;
import static java.lang.Long.MAX_VALUE;
import static java.lang.System.out;
import static java.lang.Thread.sleep;
import static org.apache.geode.distributed.DistributedLockService.getServiceNamed;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.ThreadUtils.dumpAllStacks;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockGrantor;
import org.apache.geode.distributed.internal.locks.DLockRemoteToken;
import org.apache.geode.distributed.internal.locks.DLockRequestProcessor;
import org.apache.geode.distributed.internal.locks.DLockRequestProcessor.DLockRequestMessage;
import org.apache.geode.distributed.internal.locks.DLockRequestProcessor.DLockResponseMessage;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DLockToken;
import org.apache.geode.distributed.internal.locks.RemoteThread;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DLockTest;

/**
 * This class tests distributed ownership via the DistributedLockService api.
 */
@Category({DLockTest.class})
public final class DistributedLockServiceDUnitTest extends JUnit4DistributedTestCase {
  private static Logger logger = LogService.getLogger();

  private static DistributedSystem dlstSystem;
  private static DistributedLockBlackboard blackboard;
  private static Object monitor = new Object();

  private int hits = 0;
  private int completes = 0;
  private boolean done;
  private boolean got;

  /**
   * Returns a previously created (or new, if this is the first time this method is called in this
   * VM) distributed system which is somewhat configurable via hydra test parameters.
   */
  @Override
  public final void postSetUp() throws Exception {

    createBlackboard();
    Invoke.invokeInEveryVM(() -> createBlackboard());

    // Create a DistributedSystem in every VM
    connectDistributedSystem();

    Invoke.invokeInEveryVM(() -> connectDistributedSystem());
  }

  private void createBlackboard() throws Exception {
    if (blackboard == null) {
      blackboard = DistributedLockBlackboardImpl.getInstance();
    }
  }

  @Override
  public final void preTearDown() {
    Invoke.invokeInEveryVM(() -> destroyAllDLockServices());
  }

  @Override
  public void postTearDown() {
    disconnectAllFromDS();
  }

  public static void destroyAllDLockServices() {
    DLockService.destroyAll();
    dlstSystem = null;
  }

  /**
   * Connects a DistributedSystem, saves it in static variable "system"
   */
  protected static void connectDistributedSystem() {
    dlstSystem = (new DistributedLockServiceDUnitTest()).getSystem();
  }

  private static volatile boolean stop_testFairness;

  @Test
  public void testFairness() throws InterruptedException, ExecutionException {
    final int[] vmThreads = new int[] {1, 4, 8, 16};
    final int numVms = vmThreads.length;
    final int numThreads = Arrays.stream(vmThreads).sum();
    final List<VM> vms = new ArrayList<>();
    final List<Future> futures = new ArrayList<>();
    final String serviceName = "testFairness_" + getUniqueName();
    final Object lock = "lock";

    // get the lock and hold it until all threads are ready to go
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    assertThat(service.lock(lock, -1, -1)).isTrue();

    // create the lock service in all vms
    vms.addAll(forNumVMsInvoke(numVms, () -> remoteCreateService(serviceName)));
    Thread.sleep(100);

    Invoke.invokeInEveryVM(() -> {
      stop_testFairness = false;
    });

    // line up threads for the fairness race...
    for (int vm = 0; vm < numVms; vm++) {
      logger.info("[testFairness] lining up " + vmThreads[vm] + " threads in vm " + vm);

      for (int j = 0; j < vmThreads[vm]; j++) {
        SerializableCallable<Integer> fairnessRunnable = new SerializableCallable<Integer>() {
          public Integer call() {
            // lock, inc count, and unlock until stop_testFairness is set true
            final AtomicInteger lockCount = new AtomicInteger(0);
            try {
              DistributedLockService service =
                  DistributedLockService.getServiceNamed(serviceName);
              while (!stop_testFairness) {
                assertThat(service.lock(lock, -1, -1)).isTrue();
                lockCount.incrementAndGet();
                service.unlock(lock);
              }
            } catch (VirtualMachineError e) {
              SystemFailure.initiateFailure(e);
              throw e;
            } catch (Throwable t) {
              logger.warn(t);
              fail(t.getMessage());
            } finally {
              return lockCount.get();
            }
          }
        };

        futures.add(vms.get(vm).invokeAsync(() -> fairnessRunnable.call()));
      }
    }

    // wait for all threads to be ready to start the race
    for (int i = 0; i < numVms; i++) {
      final int vmId = i;

      vms.get(vmId).invoke(() -> {
        DLockService localService =
            (DLockService) DistributedLockService.getServiceNamed(serviceName);

        await()
            .untilAsserted(() -> assertThat(localService.getStats().getLockWaitsInProgress())
                .isEqualTo(vmThreads[vmId]));
      });
    }

    // start the race!
    service.unlock(lock);
    Thread.sleep(1000 * 5); // 5 seconds

    // stop the race...
    assertThat(service.lock(lock, -1, -1)).isTrue();
    for (VM vm : vms) {
      vm.invoke(new SerializableRunnable() {
        public void run() {
          stop_testFairness = true;
        }
      });
    }

    // release the lock and destroy the lock service
    service.unlock(lock);

    // calc total locks granted...
    Integer totalLocks = 0;
    Integer maxLocks = 0;
    Integer minLocks = Integer.MAX_VALUE;

    for (Future future : futures) {
      Integer numLocks = (Integer) future.get();
      totalLocks += numLocks;
      if (minLocks > numLocks) {
        minLocks = numLocks;
      }
      if (maxLocks < numLocks) {
        maxLocks = numLocks;
      }
    }

    vms.forEach(vm -> vm.invoke(() -> DistributedLockService.destroy(serviceName)));

    logger.info("[testFairness] totalLocks=" + totalLocks + " minLocks="
        + minLocks + " maxLocks=" + maxLocks);

    int expectedLocks = (totalLocks / numThreads) + 1;

    int deviation = (int) (expectedLocks * 0.3);
    int lowThreshold = expectedLocks - deviation;
    int highThreshold = expectedLocks + deviation;

    logger.info("[testFairness] deviation=" + deviation + " expectedLocks="
        + expectedLocks + " lowThreshold=" + lowThreshold + " highThreshold=" + highThreshold);

    assertThat(minLocks >= lowThreshold).withFailMessage("minLocks is less than lowThreshold")
        .isTrue();
    assertThat(maxLocks <= highThreshold).withFailMessage("maxLocks is greater than highThreshold")
        .isTrue();
  }

  @Test
  public void testOneGetsAndOthersTimeOut() throws Exception {
    doOneGetsAndOthersTimeOut(1, 1);
    doOneGetsAndOthersTimeOut(4, 3);
  }

  private static InternalDistributedMember getLockGrantor(String serviceName) {
    DLockService service = (DLockService) DistributedLockService.getServiceNamed(serviceName);
    assertThat(service).isNotNull();
    InternalDistributedMember grantor = service.getLockGrantorId().getLockGrantorMember();
    assertThat(grantor).isNotNull();
    System.out.println("In identifyLockGrantor - grantor is " + grantor);
    return grantor;
  }

  private static Boolean isLockGrantor(String serviceName) {
    DLockService service = (DLockService) DistributedLockService.getServiceNamed(serviceName);
    assertThat(service).isNotNull();
    Boolean result = service.isLockGrantor();
    System.out.println("In isLockGrantor: " + result);
    return result;
  }

  protected static void becomeLockGrantor(String serviceName) {
    DLockService service = (DLockService) DistributedLockService.getServiceNamed(serviceName);
    assertThat(service).isNotNull();
    System.out.println("About to call becomeLockGrantor...");
    service.becomeLockGrantor();
  }

  @Test
  public void testGrantorSelection() {
    // TODO change distributedCreateService usage to be concurrent threads

    final String serviceName = "testGrantorSelection_" + getUniqueName();

    final List<VM> vmList = distributedCreateService(4, serviceName, true);


    getLockGrantor(serviceName);

    assertGrantorIsConsistent(serviceName, vmList);
  }

  @Test
  public void testBasicGrantorRecovery() {
    int numVMs = 4;
    final String serviceName = "testBasicGrantorRecovery_" + getUniqueName();
    final List<VM> vmList = distributedCreateService(numVMs, serviceName, false);

    // Apparently it's necessary to query the lock grantor to have a server be nominated grantor.
    vmList.get(0).invoke(() -> getLockGrantor(serviceName));

    VM originalGrantorVM = identifyLockGrantorWithSanityCheck(serviceName, vmList);
    VM lockHolder = null;
    for (VM vm : vmList) {
      if (vm != originalGrantorVM) {
        lockHolder = vm;
        vm.invoke(() -> DLockService.getServiceNamed(serviceName).lock("foo", 1200, -1));
        break;
      }
    }

    final InternalDistributedMember originalGrantorMember =
        originalGrantorVM.invoke(() -> getLockGrantor(serviceName));

    System.out.println("originalGrantorMember = " + originalGrantorMember);
    final InternalDistributedMember originalGrantorSelfReportedMember = originalGrantorVM
        .invoke(() -> InternalDistributedSystem.getAnyInstance().getDistributedMember());
    assertThat(originalGrantorMember).isEqualTo(originalGrantorSelfReportedMember);

    originalGrantorVM.invoke(() -> disconnectFromDS());

    vmList.remove(originalGrantorVM);

    await("vm0 leave has been processed")
        .until(() -> vmList.parallelStream()
            .noneMatch(vm -> vm.invoke(() -> InternalDistributedSystem
                .getAnyInstance()
                .getAllOtherMembers()
                .contains(originalGrantorMember))));

    // It seems that we need to actually use the lock service to have a new grantor be picked.
    lockHolder.invoke(() -> DLockService.getServiceNamed(serviceName).unlock("foo"));

    final VM newGrantorMember = identifyLockGrantorWithSanityCheck(serviceName, vmList);
    assertThat(newGrantorMember).isNotEqualTo(originalGrantorMember);
  }

  /**
   * This is just a better-sounding wrapper for when we want to do only consistency checks.
   */
  private static void assertGrantorIsConsistent(String serviceName, Collection<VM> vmList) {
    identifyLockGrantorWithSanityCheck(serviceName, vmList);
  }

  private static VM identifyLockGrantorWithSanityCheck(String serviceName, Collection<VM> vmList) {
    VM grantorVM = null;

    for (VM vm : vmList) {
      if (vm.invoke(() -> isLockGrantor(serviceName))) {
        grantorVM = vm;
      }
    }

    assertThat(grantorVM).withFailMessage("Some VM should think it's the lock grantor").isNotNull();

    final DistributedMember grantorID =
        grantorVM.invoke(() -> InternalDistributedSystem.getAnyInstance().getDistributedMember());

    for (VM vm : vmList) {
      if (vm != grantorVM) {
        assertThat(vm.invoke(() -> isLockGrantor(serviceName))).isFalse();
      }
      assertThat(vm.invoke(() -> getLockGrantor(serviceName))).isEqualTo(grantorID);
    }

    return grantorVM;
  }

  @Test
  public void testLockFailover() {
    final int originalGrantorVM = 0;
    final int oneVM = 1;
    final int twoVM = 2;
    final String serviceName = "testLockFailover-" + getUniqueName();

    // create lock services...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockFailover] create services");
    }

    VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.getLockGrantor(serviceName));

    Boolean isGrantor = VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
    assertThat(isGrantor)
        .withFailMessage("First member calling getLockGrantor failed to become grantor")
        .isEqualTo(Boolean.TRUE);

    // get locks...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockFailover] get lock");
    }

    Boolean locked = VM.getVM(originalGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + originalGrantorVM));
    assertThat(locked).withFailMessage("Failed to get lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    locked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + twoVM));
    assertThat(locked).withFailMessage("Failed to get lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    locked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + oneVM));
    assertThat(locked).withFailMessage("Failed to get lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    // disconnect originalGrantorVM...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockFailover] disconnect originalGrantorVM");
    }

    VM.getVM(originalGrantorVM).invoke(new SerializableRunnable() {
      public void run() {
        disconnectFromDS();
      }
    });

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    // verify locks by unlocking...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockFailover] release locks");
    }

    Boolean unlocked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + twoVM));
    assertThat(unlocked).withFailMessage("Failed to release lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    unlocked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + oneVM));
    assertThat(unlocked).withFailMessage("Failed to release lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    // switch locks...
    locked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + twoVM));
    assertThat(unlocked).withFailMessage("Failed to release lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    locked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + oneVM));
    assertThat(unlocked).withFailMessage("Failed to release lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    unlocked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + twoVM));
    assertThat(unlocked).withFailMessage("Failed to release lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    unlocked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + oneVM));
    assertThat(unlocked).withFailMessage("Failed to release lock in testLockFailover")
        .isEqualTo(Boolean.TRUE);

    // verify grantor is unique...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockFailover] verify grantor identity");
    }

    InternalDistributedMember oneID = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.getLockGrantor(serviceName));
    InternalDistributedMember twoID = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.getLockGrantor(serviceName));
    assertThat(oneID != null && twoID != null)
        .withFailMessage("Failed to identifyLockGrantor in testLockFailover").isTrue();
    assertThat(twoID).withFailMessage("Failed grantor uniqueness in testLockFailover")
        .isEqualTo(oneID);
  }

  @Test
  public void testLockThenBecomeLockGrantor() {
    final int originalGrantorVM = 0;
    final int becomeGrantorVM = 1;
    final int thirdPartyVM = 2;
    final String serviceName = "testLockThenBecomeLockGrantor-" + getUniqueName();

    // create lock services...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockThenBecomeLockGrantor] create services");
    }

    VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    try {
      Thread.sleep(20);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    VM.getVM(becomeGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(thirdPartyVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.getLockGrantor(serviceName));

    Boolean isGrantor = VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
    assertThat(isGrantor).isEqualTo(Boolean.TRUE)
        .withFailMessage("First member calling getLockGrantor failed to become grantor");

    // control...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockThenBecomeLockGrantor] check control");
    }
    Boolean check = VM.getVM(becomeGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + becomeGrantorVM));
    assertThat(check).isEqualTo(Boolean.FALSE)
        .withFailMessage("Check of control failed... unlock succeeded but nothing locked");

    // get locks...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockThenBecomeLockGrantor] get lock");
    }

    Boolean locked = VM.getVM(originalGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + originalGrantorVM));
    assertThat(locked).isEqualTo(Boolean.TRUE)
        .withFailMessage("Failed to get lock in testLockThenBecomeLockGrantor");

    locked = VM.getVM(thirdPartyVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + thirdPartyVM));
    assertThat(locked).isEqualTo(Boolean.TRUE)
        .withFailMessage("Failed to get lock in testLockThenBecomeLockGrantor");

    locked = VM.getVM(becomeGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + becomeGrantorVM));
    assertThat(locked).isEqualTo(Boolean.TRUE)
        .withFailMessage("Failed to get lock in testLockThenBecomeLockGrantor");

    // become lock grantor...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockThenBecomeLockGrantor] become lock grantor");
    }

    VM.getVM(becomeGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.becomeLockGrantor(serviceName));

    try {
      Thread.sleep(20);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    isGrantor = VM.getVM(becomeGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
    assertThat(isGrantor).isEqualTo(Boolean.TRUE).withFailMessage("Failed to become lock grantor");

    // verify locks by unlocking...
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockThenBecomeLockGrantor] release locks");
    }

    Boolean unlocked = VM.getVM(originalGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + originalGrantorVM));
    assertThat(unlocked).isEqualTo(Boolean.TRUE)
        .withFailMessage("Failed to release lock in testLockThenBecomeLockGrantor");

    unlocked = VM.getVM(thirdPartyVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + thirdPartyVM));
    assertThat(unlocked).isEqualTo(Boolean.TRUE)
        .withFailMessage("Failed to release lock in testLockThenBecomeLockGrantor");

    unlocked = VM.getVM(becomeGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + becomeGrantorVM));
    assertThat(unlocked).isEqualTo(Boolean.TRUE)
        .withFailMessage("Failed to release lock in testLockThenBecomeLockGrantor");

    // test for bug in which transferred token gets re-entered causing lock recursion
    unlocked = VM.getVM(becomeGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + becomeGrantorVM));
    assertThat(unlocked).isEqualTo(Boolean.FALSE)
        .withFailMessage("Transfer of tokens caused lock recursion in held lock");
  }

  @Test
  public void testBecomeLockGrantor() {
    // create lock services...
    int numVMs = 4;
    final String serviceName = "testBecomeLockGrantor-" + getUniqueName();
    distributedCreateService(numVMs, serviceName, true);

    // each one gets a lock...
    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      Boolean locked = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "obj-" + finalvm));
      assertThat(locked).isEqualTo(Boolean.TRUE)
          .withFailMessage("Failed to get lock in testBecomeLockGrantor");
    }

    // find the grantor...
    int originalVM = -1;
    InternalDistributedMember oldGrantor = null;
    for (int vm = 0; vm < numVMs; vm++) {
      Boolean isGrantor = VM.getVM(vm)
          .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
      if (isGrantor) {
        originalVM = vm;
        oldGrantor = VM.getVM(vm)
            .invoke(() -> DistributedLockServiceDUnitTest.getLockGrantor(
                serviceName));
        break;
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("[testBecomeLockGrantor] original grantor is " + oldGrantor);
    }

    // have one call becomeLockGrantor
    for (int vm = 0; vm < numVMs; vm++) {
      if (vm != originalVM) {
        VM.getVM(vm)
            .invoke(() -> DistributedLockServiceDUnitTest.becomeLockGrantor(serviceName));
        Boolean isGrantor = VM.getVM(vm)
            .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
        assertThat(isGrantor).isEqualTo(Boolean.TRUE)
            .withFailMessage("isLockGrantor is false after calling becomeLockGrantor");
        break;
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("[testBecomeLockGrantor] one vm has called becomeLockGrantor...");
    }

    InternalDistributedMember newGrantor = null;
    for (int vm = 0; vm < numVMs; vm++) {
      Boolean isGrantor = VM.getVM(vm)
          .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
      if (isGrantor) {
        newGrantor = VM.getVM(vm)
            .invoke(() -> DistributedLockServiceDUnitTest.getLockGrantor(
                serviceName));
        break;
      }
    }
    assertThat(newGrantor).isNotEqualTo(oldGrantor);
    // verify locks still held by unlocking
    // each one unlocks...
    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      Boolean unlocked = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "obj-" + finalvm));
      assertThat(unlocked).isEqualTo(Boolean.TRUE)
          .withFailMessage("Failed to unlock in testBecomeLockGrantor");
    }

    if (logger.isDebugEnabled()) {
      logger.debug("[testBecomeLockGrantor] finished");
    }

    // verify that pending requests are granted by unlocking them also
  }

  @Test
  public void testOnlyOneVmAcquiresWithTryLock() throws Exception {
    final Long waitMillis = 100L;

    // create lock services...
    if (logger.isDebugEnabled()) {
      logger.debug("[testTryLock] create lock services");
    }
    final String serviceName = "testTryLock-" + getUniqueName();
    final List<VM> vms = distributedCreateService(4, serviceName, false);

    final List<AsyncInvocation<Boolean>> invocations = vms.stream()
        .map(vm -> vm.invokeAsync(
            () -> DistributedLockServiceDUnitTest.tryLock(serviceName, "KEY", waitMillis)))
        .collect(Collectors
            .toList());

    int lockCount = 0;
    for (AsyncInvocation<Boolean> invocation : invocations) {
      if (invocation.get()) {
        lockCount++;
      }
    }

    assertThat(lockCount).isEqualTo(1)
        .withFailMessage("More than one vm acquired the tryLock");

    if (logger.isDebugEnabled()) {
      logger.debug("[testTryLock] unlock tryLock");
    }
    int unlockCount = 0;
    for (VM vm : vms) {
      Boolean unlocked =
          vm.invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY"));
      if (unlocked) {
        unlockCount++;
      }
    }

    assertThat(unlockCount).withFailMessage("More than one vm unlocked the tryLock").isEqualTo(1);
  }

  @Test
  public void testOneGetsThenOtherGets() throws Exception { // (numVMs, numThreadsPerVM)
    doOneGetsThenOtherGets(1, 1);
    // doOneGetsThenOtherGets(2, 2);
    // doOneGetsThenOtherGets(3, 3);
    doOneGetsThenOtherGets(4, 3);
  }

  @Test
  public void testLockDifferentNames() {
    String serviceName = getUniqueName();

    // Same VM
    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    assertThat(service.lock("obj1", -1, -1)).isTrue();
    assertThat(service.lock("obj2", -1, -1)).isTrue();
    service.unlock("obj1");
    service.unlock("obj2");

    // Different VMs
    VM vm = VM.getVM(0);
    vm.invoke(() -> remoteCreateService(serviceName));
    assertThat(service.lock("masterVMobj", -1, -1)).isTrue();

    assertThat(vm.invoke(() -> getLockAndIncrement(serviceName, "otherVMobj", -1, 0))).isTrue();

    service.unlock("masterVMobj");
  }

  @Test
  public void testLocalGetLockAndIncrement() throws Exception {
    String serviceName = getUniqueName();
    remoteCreateService(serviceName);
    DistributedLockService.getServiceNamed(serviceName);
    assertThat(getLockAndIncrement(serviceName, "localVMobj", -1, 0)).isTrue();
  }

  @Test
  public void testRemoteGetLockAndIncrement() {
    String serviceName = getUniqueName();
    VM vm = VM.getVM(0);
    vm.invoke(() -> remoteCreateService(serviceName));
    assertThat(vm.invoke(() -> getLockAndIncrement(serviceName, "remoteVMobj",
        -1, 0))).isTrue();
  }

  @Test
  public void testLockSameNameDifferentService() {
    String serviceName1 = getUniqueName() + "_1";
    String serviceName2 = getUniqueName() + "_2";
    String objName = "obj";

    // Same VM
    remoteCreateService(serviceName1);
    remoteCreateService(serviceName2);
    DistributedLockService service1 = DistributedLockService.getServiceNamed(serviceName1);
    DistributedLockService service2 = DistributedLockService.getServiceNamed(serviceName2);
    assertThat(service1.lock(objName, -1, -1)).isTrue();
    assertThat(service2.lock(objName, -1, -1)).isTrue();
    service1.unlock(objName);
    service2.unlock(objName);

    // Different VMs
    VM vm = VM.getVM(0);
    vm.invoke(() -> remoteCreateService(serviceName1));
    vm.invoke(() -> remoteCreateService(serviceName2));
    assertThat(service1.lock(objName, -1, -1)).isTrue();
    assertThat(vm.invoke(() -> getLockAndIncrement(serviceName2, objName, -1, 0))).isTrue();
    service1.unlock(objName);
  }

  @Test
  public void testLeaseDoesntExpire() {
    String serviceName = getUniqueName();
    final Object objName = 3;

    // Same VM
    remoteCreateService(serviceName);
    final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    // lock objName with a sufficiently long lease
    assertThat(service.lock(objName, -1, 60000)).isTrue();
    // try to lock in another thread, with a timeout shorter than above lease
    final boolean[] resultHolder = new boolean[] {false};
    Thread thread = new Thread(new Runnable() {
      public void run() {
        resultHolder[0] = !service.lock(objName, 1000, -1);
      }
    });
    thread.start();
    ThreadUtils.join(thread, 30 * 1000);
    assertThat(resultHolder[0]).isTrue();
    // the unlock should succeed without throwing LeaseExpiredException
    service.unlock(objName);

    // Different VM
    VM vm = VM.getVM(0);
    vm.invoke(() -> remoteCreateService(serviceName));
    // lock objName in this VM with a sufficiently long lease
    assertThat(service.lock(objName, -1, 60000)).isTrue();
    // try to lock in another VM, with a timeout shorter than above lease
    assertThat(vm.invoke(() -> getLockAndIncrement(serviceName, objName, 1000L, 0L))).isFalse();
    // the unlock should succeed without throwing LeaseExpiredException
    service.unlock(objName);
  }

  @Test
  public void testLockUnlock() {
    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    service.lock(objName, -1, -1);
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    service.unlock(objName);
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();
  }

  @Test
  public void testLockExpireUnlock() throws Exception {
    long leaseMs = 200;
    long waitBeforeLockingMs = 210;

    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    Thread.sleep(waitBeforeLockingMs); // should expire...
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    Assertions.assertThatThrownBy(() -> service.unlock(objName))
        .isInstanceOf(LeaseExpiredException.class);
  }

  @Test
  public void testLockRecursion() {
    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    // initial lock...
    assertThat(service.lock(objName, -1, -1)).isTrue();
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    // recursion +1...
    assertThat(service.lock(objName, -1, -1)).isTrue();

    // recursion -1...
    service.unlock(objName);
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    // and unlock...
    service.unlock(objName);
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();
  }

  @Test
  public void testLockRecursionWithExpiration() throws Exception {
    long leaseMs = 500;
    long waitBeforeLockingMs = 750;

    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    // initial lock...
    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    // recursion +1...
    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    // expire...
    Thread.sleep(waitBeforeLockingMs);
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    // should fail...
    try {
      service.unlock(objName);
      fail("unlock should have thrown LeaseExpiredException");
    } catch (LeaseExpiredException ex) {
    }

    // relock it...
    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    // and unlock to verify no recursion...
    service.unlock(objName);
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue(); // throws failure!!

    // go thru again in different order...
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    // initial lock...
    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    // expire...
    Thread.sleep(waitBeforeLockingMs);
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();

    // relock it...
    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    assertThat(service.isHeldByCurrentThread(objName)).isTrue();

    // and unlock to verify no recursion...
    service.unlock(objName);
    assertThat(!service.isHeldByCurrentThread(objName)).isTrue();
  }

  @Test
  public void testLeaseExpiresBeforeOtherLocks() throws InterruptedException {
    leaseExpiresTest(false);
  }

  @Test
  public void testLeaseExpiresWhileOtherLocks() throws InterruptedException {
    leaseExpiresTest(true);
  }

  private void leaseExpiresTest(boolean tryToLockBeforeExpiration) throws InterruptedException {
    long leaseMs = 100;
    long waitBeforeLockingMs = tryToLockBeforeExpiration ? 50 : 110;

    final String serviceName = getUniqueName();
    final Object objName = 3;

    // Same VM
    remoteCreateService(serviceName);
    final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    // lock objName with a short lease
    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    Thread.sleep(waitBeforeLockingMs);

    if (waitBeforeLockingMs > leaseMs) {
      assertThat(!service.isHeldByCurrentThread(objName)).isTrue();
    }

    // try to lock in another thread - lease should have expired
    final boolean[] resultHolder = new boolean[] {false};
    Thread thread = new Thread(() -> {
      resultHolder[0] = service.lock(objName, -1, -1);
      service.unlock(objName);
      assertThat(!service.isHeldByCurrentThread(objName)).isTrue();
    });
    thread.start();
    ThreadUtils.join(thread, 30 * 1000);
    assertThat(resultHolder[0]).isTrue();

    // this thread's unlock should throw LeaseExpiredException
    Assertions.assertThatThrownBy(() -> {
      service.unlock(objName);
    }).isInstanceOf(LeaseExpiredException.class);

    VM vm = VM.getVM(0);
    vm.invoke(() -> remoteCreateService(serviceName));

    // lock objName in this VM with a short lease
    assertThat(service.lock(objName, -1, leaseMs)).isTrue();
    Thread.sleep(waitBeforeLockingMs);

    if (logger.isDebugEnabled()) {
      logger.debug("[testLeaseExpires] succeed lock in other vm");
    }
    // try to lock in another VM - should succeed
    assertThat(vm.invoke(() -> getLockAndIncrement(serviceName, objName, (long) -1, 0L)))
        .isEqualTo(TRUE);

    if (logger.isDebugEnabled()) {
      logger.debug("[testLeaseExpires] unlock should throw LeaseExpiredException again");
    }
    // this VMs unlock should throw LeaseExpiredException
    Assertions.assertThatThrownBy(() -> {
      service.unlock(objName);
    }).isInstanceOf(LeaseExpiredException.class);
  }

  @Test
  public void testSuspendLockingAfterExpiration() throws Exception {
    final long leaseMillis = 100;
    final long suspendWaitMillis = 10000;

    final String serviceName = getUniqueName();
    final Object key = 3;

    // controller locks key and then expires - controller is grantor

    DistributedLockService dls = DistributedLockService.create(serviceName, getSystem());

    assertThat(dls.lock(key, -1, leaseMillis)).isTrue();

    // wait for expiration
    Thread.sleep(leaseMillis * 2);

    Assertions.assertThatThrownBy(() -> dls.unlock(key)).isInstanceOf(LeaseExpiredException.class);

    // other vm calls suspend

    if (logger.isDebugEnabled()) {
      logger.debug("[leaseExpiresThenSuspendTest] call to suspend locking");
    }
    VM.getVM(0).invoke(() -> {
      final DistributedLockService dlock =
          DistributedLockService.create(serviceName, getSystem());
      dlock.suspendLocking(suspendWaitMillis);
      dlock.resumeLocking();
      assertThat(dlock.lock(key, -1, leaseMillis)).isTrue();
      dlock.unlock(key);
    });
  }

  private volatile boolean started = false;
  private volatile boolean gotLock = false;
  volatile Throwable exception = null;
  private volatile Throwable throwable = null;

  @Test
  public void testLockInterruptiblyIsInterruptible() {
    started = false;
    gotLock = false;
    exception = null;
    throwable = null;

    // Get a lock in first thread
    final String serviceName = getUniqueName();
    final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    service.becomeLockGrantor();
    assertThat(service.lock("obj", 1000, -1)).isTrue();

    // Start second thread that tries to lock in second thread
    Thread thread2 = new Thread(() -> {
      try {
        started = true;
        gotLock = service.lockInterruptibly("obj", -1, -1);
      } catch (InterruptedException ex) {
        exception = ex;
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        throwable = t;
      }
    });
    thread2.start();

    // Interrupt second thread
    while (!started)
      Thread.yield();
    thread2.interrupt();
    ThreadUtils.join(thread2, 20 * 1000);

    // Expect it got InterruptedException and didn't lock the service
    assertThat(gotLock).isFalse();
    if (throwable != null) {
      logger.warn("testLockInterruptiblyIsInterruptible threw unexpected Throwable", throwable);
    }
    assertThat(exception).isNotNull();

    // Unlock "obj" in first thread
    service.unlock("obj");

    // Make sure it didn't get locked by second thread
    logger.info(
        "[testLockInterruptiblyIsInterruptible] try to get lock with timeout should not fail");
    assertThat(service.lock("obj", 5000, -1)).isTrue();
    DistributedLockService.destroy(serviceName);
  }

  private volatile boolean wasFlagSet = false;

  @Test
  public void testLockIsNotInterruptible() throws Exception {
    // Lock entire service in first thread
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockIsNotInterruptible] lock in first thread");
    }
    started = false;
    gotLock = false;
    exception = null;
    wasFlagSet = false;

    final String serviceName = getUniqueName();
    final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    assertThat(service.lock("obj", 1000, -1)).isTrue();

    // Start second thread that tries to lock in second thread
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockIsNotInterruptible] attempt lock in second thread");
    }
    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        try {
          started = true;
          gotLock = service.lock("obj", -1, -1);
          if (logger.isDebugEnabled()) {
            logger.debug("[testLockIsNotInterruptible] thread2 finished lock() - got " + gotLock);
          }
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable ex) {
          logger.warn("[testLockIsNotInterruptible] Caught...", ex);
          exception = ex;
        }
        wasFlagSet = Thread.currentThread().isInterrupted();
      }
    });
    thread2.start();

    // Interrupt second thread
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockIsNotInterruptible] interrupt second thread");
    }
    while (!started)
      Thread.yield();
    Thread.sleep(500);
    thread2.interrupt();
    // Expect it didn't get an exception and didn't lock the service
    Thread.sleep(500);
    assertThat(gotLock).isFalse();
    assertThat(exception).isNull();

    // Unlock entire service in first thread
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockIsNotInterruptible] unlock in first thread");
    }
    service.unlock("obj");
    Thread.sleep(500);

    // Expect that thread2 should now complete execution.
    ThreadUtils.join(thread2, 20 * 1000);

    // Now thread2 should have gotten the lock, not the exception, but the
    // thread's flag should be set
    if (logger.isDebugEnabled()) {
      logger.debug("[testLockIsNotInterruptible] verify second thread got lock");
    }
    assertThat(exception).isNull();
    assertThat(gotLock).isTrue();
    assertThat(wasFlagSet).isTrue();
  }

  /**
   * Test DistributedLockService.acquireExclusiveLocking(), releaseExclusiveLocking()
   */
  @Test
  public void testSuspendLockingBasic() {
    final DistributedLockService service =
        DistributedLockService.create(getUniqueName(), dlstSystem);

    try {
      service.resumeLocking();
      fail("Didn't throw LockNotHeldException");
    } catch (LockNotHeldException ex) {
      // expected
    }

    assertThat(service.suspendLocking(-1)).isTrue();
    service.resumeLocking();

    // It's not reentrant
    assertThat(service.suspendLocking(1000)).isTrue();
    try {
      service.suspendLocking(1);
      fail("didn't get IllegalStateException");
    } catch (IllegalStateException ex) {
      // expected
    }
    service.resumeLocking();

    // Get "false" if another thread is holding it
    Thread thread = new Thread(() -> {
      System.out.println("new thread about to suspendLocking()");
      assertThat(service.suspendLocking(1000)).isTrue();
    });

    thread.start();
    ThreadUtils.join(thread, 30 * 1000);
    System.out.println("main thread about to suspendLocking");
    assertThat(!service.suspendLocking(1000)).isTrue();
  }

  /**
   * Test that exlusive locking prohibits locking activity
   */
  @Test
  public void testSuspendLockingProhibitsLocking() {
    final String name = getUniqueName();
    distributedCreateService(2, name, true);
    DistributedLockService service = DistributedLockService.getServiceNamed(name);

    // Should be able to lock from other VM
    VM vm1 = VM.getVM(1);
    assertThat(vm1.invoke(() -> DistributedLockServiceDUnitTest.tryToLock(name))).isTrue();

    assertThat(service.suspendLocking(1000)).isTrue();

    // vm1 is the grantor... use debugHandleSuspendTimeouts
    vm1.invoke(new SerializableRunnable("setDebugHandleSuspendTimeouts") {
      public void run() {
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(name);
        assertThat(dls.isLockGrantor()).isTrue();
        DLockGrantor grantor = dls.getGrantorWithNoSync();
        grantor.setDebugHandleSuspendTimeouts(5000);
      }
    });

    // Shouldn't be able to lock a name from another VM
    assertThat(!vm1.invoke(() -> DistributedLockServiceDUnitTest.tryToLock(name))).isTrue();

    service.resumeLocking();

    vm1.invoke(new SerializableRunnable("unsetDebugHandleSuspendTimeouts") {
      public void run() {
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(name);
        assertThat(dls.isLockGrantor()).isTrue();
        DLockGrantor grantor = dls.getGrantorWithNoSync();
        grantor.setDebugHandleSuspendTimeouts(0);
      }
    });

    // Should be able to lock again
    assertThat(vm1.invoke(() -> DistributedLockServiceDUnitTest.tryToLock(name))).isTrue();

  }

  /**
   * Test that suspend locking behaves under various usage patterns. This ensures that suspend and
   * regular locks behave as ReadWriteLocks and processing occurs in order.
   */
  @Test
  public void testSuspendLockingBehaves() throws Exception {
    try {
      doTestSuspendLockingBehaves();
    } finally {
      Invoke.invokeInEveryVM(new SerializableRunnable() {
        public void run() {
          try {
            if (suspendClientSuspendLockingBehaves != null) {
              suspendClientSuspendLockingBehaves.stop();
              suspendClientSuspendLockingBehaves = null;
            }
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            logger.error("Error in testSuspendLockingBehaves finally", t);
          }
          try {
            if (lockClientSuspendLockingBehaves != null) {
              lockClientSuspendLockingBehaves.stop();
              lockClientSuspendLockingBehaves = null;
            }
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            logger.error("Error in testSuspendLockingBehaves finally", t);
          }
        }
      });
    }
  }

  private void doTestSuspendLockingBehaves() {
    final String dlsName = getUniqueName();
    final VM vmGrantor = VM.getVM(0);
    final VM vmOne = VM.getVM(1);
    final VM vmTwo = VM.getVM(2);
    final VM vmThree = VM.getVM(3);
    final String key1 = "key1";

    // TODO: make sure suspend thread can get other locks

    // TODO: test local (in grantor) locks and suspends also

    // define some SerializableRunnables
    final SerializableRunnable createDLS = new SerializableRunnable("Create " + dlsName) {
      public void run() {
        DistributedLockService.create(dlsName, getSystem());
        lockClientSuspendLockingBehaves = new BasicLockClient(dlsName, key1);
        suspendClientSuspendLockingBehaves = new BasicLockClient(dlsName, key1);
        assertThat(isLockGrantor(dlsName)).isFalse();
      }
    };
    final SerializableRunnable suspendLocking =
        new SerializableRunnable("Suspend locking " + dlsName) {
          public void run() {
            suspendClientSuspendLockingBehaves.suspend();
          }
        };
    final SerializableRunnable resumeLocking =
        new SerializableRunnable("Resume locking " + dlsName) {
          public void run() {
            suspendClientSuspendLockingBehaves.resume();
          }
        };
    final SerializableRunnable lockKey = new SerializableRunnable("Get lock " + dlsName) {
      public void run() {
        lockClientSuspendLockingBehaves.lock();
      }
    };
    final SerializableRunnable unlockKey = new SerializableRunnable("Unlock " + dlsName) {
      public void run() {
        lockClientSuspendLockingBehaves.unlock();
      }
    };

    // create grantor
    logger.info("[testSuspendLockingBehaves] Create grantor " + dlsName);
    vmGrantor.invoke(new SerializableRunnable("Create grantor " + dlsName) {
      public void run() {
        DistributedLockService.create(dlsName, getSystem());
        DistributedLockService.getServiceNamed(dlsName).lock(key1, -1, -1);
        DistributedLockService.getServiceNamed(dlsName).unlock(key1);
        assertThat(isLockGrantor(dlsName)).isTrue();
      }
    });

    // create dls in other vms
    vmOne.invoke(createDLS);
    vmTwo.invoke(createDLS);
    vmThree.invoke(createDLS);

    // get a lock
    logger.info("[testSuspendLockingBehaves] line up vms for lock");
    vmOne.invoke(lockKey);
    AsyncInvocation vmTwoLocking = vmTwo.invokeAsync(lockKey);
    Wait.pause(2000); // make sure vmTwo is first in line
    AsyncInvocation vmThreeLocking = vmThree.invokeAsync(lockKey);
    Wait.pause(2000);

    // make sure vmTwo and vmThree are still waiting for lock on key1
    Wait.pause(100);
    assertThat(vmTwoLocking.isAlive()).isTrue();
    Wait.pause(100);
    assertThat(vmThreeLocking.isAlive()).isTrue();

    // let vmTwo get key
    logger.info("[testSuspendLockingBehaves] unlock so vmTwo can get key");
    vmOne.invoke(unlockKey);
    ThreadUtils.join(vmTwoLocking, 10 * 1000);

    // start suspending in vmOne and vmTwo
    logger.info("[testSuspendLockingBehaves] start suspending requests");
    AsyncInvocation vmOneSuspending = vmOne.invokeAsync(suspendLocking);
    Wait.pause(2000); // make sure vmOne is first in line
    AsyncInvocation vmTwoSuspending = vmTwo.invokeAsync(suspendLocking);
    Wait.pause(2000);

    // let vmThree finish locking key
    logger.info("[testSuspendLockingBehaves] unlock so vmThree can get key");
    vmTwo.invoke(unlockKey);
    ThreadUtils.join(vmThreeLocking, 10 * 1000);

    // have vmOne get back in line for locking key
    logger.info("[testSuspendLockingBehaves] start another lock request");
    AsyncInvocation vmOneLockingAgain = vmOne.invokeAsync(lockKey);
    Wait.pause(2000);

    // let vmOne suspend locking
    logger.info("[testSuspendLockingBehaves] let vmOne suspend locking");
    Wait.pause(100);
    assertThat(vmOneSuspending.isAlive()).isTrue();
    vmThree.invoke(unlockKey);
    ThreadUtils.join(vmOneSuspending, 10 * 1000);

    // start suspending in vmThree
    logger
        .info("[testSuspendLockingBehaves] line up vmThree for suspending");
    AsyncInvocation vmThreeSuspending = vmThree.invokeAsync(suspendLocking);
    Wait.pause(2000);

    // let vmTwo suspend locking
    logger.info("[testSuspendLockingBehaves] let vmTwo suspend locking");
    Wait.pause(100);
    assertThat(vmTwoSuspending.isAlive()).isTrue();
    vmOne.invoke(resumeLocking);
    ThreadUtils.join(vmTwoSuspending, 10 * 1000);

    // let vmOne get that lock
    logger.info("[testSuspendLockingBehaves] let vmOne get that lock");
    Wait.pause(100);
    assertThat(vmOneLockingAgain.isAlive()).isTrue();
    vmTwo.invoke(resumeLocking);
    ThreadUtils.join(vmOneLockingAgain, 10 * 1000);

    // let vmThree suspend locking
    logger.info("[testSuspendLockingBehaves] let vmThree suspend locking");
    Wait.pause(100);
    assertThat(vmThreeSuspending.isAlive()).isTrue();
    vmOne.invoke(unlockKey);
    ThreadUtils.join(vmThreeSuspending, 10 * 1000);

    // done
    vmThree.invoke(resumeLocking);
  }

  private static BasicLockClient suspendClientSuspendLockingBehaves;
  private static BasicLockClient lockClientSuspendLockingBehaves;

  /**
   * Test that exlusive locking prohibits locking activity
   */
  @Test
  public void testSuspendLockingBlocksUntilNoLocks() throws InterruptedException {

    final String name = getUniqueName();
    distributedCreateService(2, name, true);
    final DistributedLockService service = getServiceNamed(name);

    // Get lock from other VM. Since same thread needs to lock and unlock,
    // invoke asynchronously, get lock, wait to be notified, then unlock.
    VM vm1 = getVM(1);
    vm1.invokeAsync(new SerializableRunnable("Lock & unlock in vm1") {
      public void run() {
        DistributedLockService service2 = getServiceNamed(name);
        assertThat(service2.lock("lock", -1, -1)).isTrue();
        synchronized (monitor) {
          try {
            monitor.wait();
          } catch (InterruptedException ex) {
            out.println("Unexpected InterruptedException");
            fail("interrupted");
          }
        }
        service2.unlock("lock");
      }
    });
    // Let vm1's thread get the lock and go into wait()
    sleep(100);

    Thread thread = new Thread(new Runnable() {
      public void run() {
        setGot(service.suspendLocking(-1));
        setDone(true);
        service.resumeLocking();
      }
    });
    setGot(false);
    setDone(false);
    thread.start();

    // Let thread start, make sure it's blocked in suspendLocking
    sleep(100);
    assertThat(getGot() || getDone())
        .withFailMessage("Before release, got: " + getGot() + ", done: " + getDone()).isFalse();

    vm1.invoke(new SerializableRunnable("notify vm1 to unlock") {
      public void run() {
        synchronized (monitor) {
          monitor.notify();
        }
      }
    });

    // Let thread finish, make sure it successfully suspended and is done
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return getDone();
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    if (!getGot() || !getDone()) {
      dumpAllStacks();
    }
    assertThat(getGot() && getDone())
        .withFailMessage("After release, got: " + getGot() + ", done: " + getDone()).isTrue();

  }

  @Test
  public void testSuspendLockingInterruptiblyIsInterruptible() throws Exception {

    started = false;
    gotLock = false;
    exception = null;

    // Lock entire service in first thread
    final String name = getUniqueName();
    final DistributedLockService service = DistributedLockService.create(name, dlstSystem);
    assertThat(service.suspendLocking(1000)).isTrue();

    // Start second thread that tries to lock in second thread
    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        try {
          started = true;
          gotLock = service.suspendLockingInterruptibly(-1);
        } catch (InterruptedException ex) {
          exception = ex;
        }
      }
    });
    thread2.start();

    // Interrupt second thread
    while (!started)
      Thread.yield();
    thread2.interrupt();
    ThreadUtils.join(thread2, 20 * 1000);

    // Expect it got InterruptedException and didn't lock the service
    Thread.sleep(500);
    assertThat(gotLock).isFalse();
    assertThat(exception).isNotNull();

    // Unlock entire service in first thread
    service.resumeLocking();
    Thread.sleep(500);

    // Make sure it didn't get locked by second thread
    assertThat(service.suspendLocking(1000)).isTrue();
    DistributedLockService.destroy(name);
  }

  @Test
  public void testSuspendLockingIsNotInterruptible() throws Exception {

    started = false;
    gotLock = false;
    exception = null;
    wasFlagSet = false;

    // Lock entire service in first thread
    final String name = getUniqueName();
    final DistributedLockService service = DistributedLockService.create(name, dlstSystem);
    assertThat(service.suspendLocking(1000)).isTrue();

    // Start second thread that tries to lock in second thread
    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        try {
          started = true;
          gotLock = service.suspendLocking(-1);
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable ex) {
          exception = ex;
        }
        wasFlagSet = Thread.currentThread().isInterrupted();
      }
    });
    thread2.start();

    // Interrupt second thread
    while (!started)
      Thread.yield();
    thread2.interrupt();
    // Expect it didn't get an exception and didn't lock the service
    Thread.sleep(500);
    assertThat(gotLock).isFalse();
    assertThat(exception).isNull();

    // Unlock entire service in first thread
    service.resumeLocking();
    ThreadUtils.join(thread2, 20 * 1000);

    // Now thread2 should have gotten the lock, not the exception, but the
    // thread's flag should be set
    logger.info("[testSuspendLockingIsNotInterruptible]" + " gotLock="
        + gotLock + " wasFlagSet=" + wasFlagSet + " exception=" + exception, exception);
    assertThat(gotLock).isTrue();
    assertThat(exception).isNull();
    assertThat(wasFlagSet).isTrue();
  }

  /**
   * Tests what happens when you attempt to lock a name on a lock service that has been destroyed.
   *
   * @author David Whitlock
   */
  @Test
  public void testLockDestroyedService() {
    String serviceName = this.getUniqueName();
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    DistributedLockService.destroy(serviceName);
    try {
      boolean locked = service.lock("TEST", -1, -1);
      fail("Lock of destroyed service returned: " + locked);

    } catch (LockServiceDestroyedException ex) {
      // pass...
    }
  }

  @Test
  public void testDepartedLastOwnerWithLease() {
    final String serviceName = this.getUniqueName();

    // Create service in this VM
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    assertThat(service.lock("key", -1, -1)).isTrue();
    service.unlock("key");

    // Create service in other VM
    VM otherVm = VM.getVM(0);
    otherVm.invoke(new SerializableRunnable() {
      public void run() {
        DistributedLockService service2 = DistributedLockService.create(serviceName, dlstSystem);
        service2.lock("key", -1, 360000);
        service2.unlock("key");
        // Wait for asynchronous messaging to complete
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          fail("interrupted");
        }
        disconnectFromDS();
      }
    });

    // Now lock back in this VM
    assertThat(service.lock("key", -1, -1)).isTrue();

  }

  @Test
  public void testDepartedLastOwnerNoLease() {
    final String serviceName = this.getUniqueName();

    // Create service in this VM
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    assertThat(service.lock("key", -1, -1)).isTrue();
    service.unlock("key");

    // Create service in other VM
    VM otherVm = VM.getVM(0);
    otherVm.invoke(new SerializableRunnable() {
      public void run() {
        DistributedLockService service2 = DistributedLockService.create(serviceName, dlstSystem);
        service2.lock("key", -1, -1);
        service2.unlock("key");
        // Wait for asynchronous messaging to complete
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          fail("interrupted");
        }
        disconnectFromDS();
      }
    });

    // Now lock back in this VM
    assertThat(service.lock("key", -1, -1)).isTrue();

  }

  /**
   * Tests for 32461 R3 StuckLocks can occur on locks with an expiration lease
   * <p>
   * VM-A locks/unlocks "lock", VM-B leases "lock" and disconnects, VM-C attempts to lock "lock" and
   * old dlock throws StuckLockException. VM-C should now succeed in acquiring the lock.
   */
  @Test
  public void testBug32461() {
    if (logger.isDebugEnabled()) {
      logger.debug("[testBug32461] prepping");
    }

    final String serviceName = getUniqueName();
    final Object objName = "32461";
    final int VM_A = 0;
    final int VM_B = 1;
    final int VM_C = 2;

    // VM-A locks/unlocks "lock"...
    if (logger.isDebugEnabled()) {
      logger.debug("[testBug32461] VM-A locks/unlocks '32461'");
    }

    VM.getVM(VM_A).invoke(new SerializableRunnable() {
      public void run() {
        remoteCreateService(serviceName);
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
        assertThat(service.lock(objName, -1, Long.MAX_VALUE)).isTrue();
        service.unlock(objName);
      }
    });

    // VM-B leases "lock" and disconnects,
    if (logger.isDebugEnabled()) {
      logger.debug("[testBug32461] VM_B leases '32461' and disconnects");
    }

    VM.getVM(VM_B).invoke(new SerializableRunnable() {
      public void run() {
        remoteCreateService(serviceName);
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
        assertThat(service.lock(objName, -1, Long.MAX_VALUE)).isTrue();
        DistributedLockService.destroy(serviceName);
        disconnectFromDS();
      }
    });

    if (logger.isDebugEnabled()) {
      logger.debug("[testBug32461] VM_C attempts to lock '32461'");
    }

    VM.getVM(VM_C).invoke(new SerializableRunnable() {
      public void run() {
        remoteCreateService(serviceName);
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
        assertThat(service.lock(objName, -1, -1)).isTrue();
        service.unlock(objName);
      }
    });
  }

  @Test
  public void testNoStuckLock() {
    final String serviceName = this.getUniqueName();
    final Object keyWithLease = "key-with-lease";
    final Object keyNoLease = "key-no-lease";

    // Create service in this VM
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);

    assertThat(service.lock(keyWithLease, -1, -1)).isTrue();
    service.unlock(keyWithLease);

    assertThat(service.lock(keyNoLease, -1, -1)).isTrue();
    service.unlock(keyNoLease);

    // Create service in other VM
    VM otherVm = VM.getVM(0);
    otherVm.invoke(new SerializableRunnable() {
      public void run() {
        DistributedLockService service2 = DistributedLockService.create(serviceName, dlstSystem);
        service2.lock(keyWithLease, -1, 360000);
        service2.lock(keyNoLease, -1, -1);
        disconnectFromDS();
      }
    });

    // Now lock back in this VM... no stuck locks anymore
    assertThat(service.lock(keyWithLease, -1, -1)).isTrue();
    service.unlock(keyWithLease);
    assertThat(service.lock(keyNoLease, -1, -1)).isTrue();
    service.unlock(keyNoLease);
  }

  volatile boolean startedThread1_testReleaseOrphanedGrant;
  volatile boolean releaseThread1_testReleaseOrphanedGrant;
  volatile boolean startedThread2_testReleaseOrphanedGrant;
  volatile boolean gotLockThread2_testReleaseOrphanedGrant;

  /**
   * Client requests lock and then interrupts lock request before processing the grant reply. This
   * causes the Client to send a release msg to the grantor.
   */
  @Test
  public void testReleaseOrphanedGrant_Local() throws Exception {
    DLockRequestProcessor.setDebugReleaseOrphanedGrant(true);
    DLockRequestProcessor.setWaitToProcessDLockResponse(false);
    try {
      startedThread2_testReleaseOrphanedGrant = false;
      gotLockThread2_testReleaseOrphanedGrant = false;
      releaseThread1_testReleaseOrphanedGrant = false;

      logger.info("[testReleaseOrphanedGrant_Local] create lock service");
      final String serviceName = getUniqueName();
      final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);

      // thread to get lock and wait and then unlock
      final Thread thread1 = new Thread(new Runnable() {
        public void run() {
          logger.info("[testReleaseOrphanedGrant_Local] get the lock");
          assertThat(service.lock("obj", -1, -1)).isTrue();
          DLockRequestProcessor.setWaitToProcessDLockResponse(true);
          startedThread1_testReleaseOrphanedGrant = true;
          synchronized (Thread.currentThread()) {
            while (!releaseThread1_testReleaseOrphanedGrant) {
              try {
                Thread.currentThread().wait();
              } catch (InterruptedException ignore) {
                fail("interrupted");
              }
            }
          }
          logger.info("[testReleaseOrphanedGrant_Local] unlock the lock");
          service.unlock("obj");
        }
      });
      thread1.start();
      while (!startedThread1_testReleaseOrphanedGrant) {
        Thread.yield();
      }

      // thread to interrupt lockInterruptibly call to cause zombie grant
      final Thread thread2 = new Thread(new Runnable() {
        public void run() {
          try {
            logger
                .info("[testReleaseOrphanedGrant_Local] call lockInterruptibly");
            startedThread2_testReleaseOrphanedGrant = true;
            assertThat(service.lockInterruptibly("obj", -1, -1)).isFalse();
          } catch (InterruptedException expected) {
            Thread.currentThread().interrupt();
          }
        }
      });
      thread2.start();
      while (!startedThread2_testReleaseOrphanedGrant) {
        Thread.yield();
      }

      // release first thread to unlock
      logger.info("[testReleaseOrphanedGrant_Local] release 1st thread");
      Thread.sleep(500);
      synchronized (thread1) {
        releaseThread1_testReleaseOrphanedGrant = true;
        thread1.notifyAll();
      }
      Thread.sleep(500);

      // while first thread is stuck on waitToProcessDLockResponse,
      // interrupt 2nd thread
      logger.info("[testReleaseOrphanedGrant_Local] interrupt 2nd thread");
      thread2.interrupt();
      ThreadUtils.join(thread2, 20 * 1000);

      // release waitToProcessDLockResponse
      logger.info("[testReleaseOrphanedGrant_Local] process lock response");
      Thread.sleep(500);
      DLockRequestProcessor.setWaitToProcessDLockResponse(false);

      // relock obj to make sure zombie release worked
      logger.info("[testReleaseOrphanedGrant_Local] verify lock not held");
      assertThat(service.lock("obj", 1000, -1)).isTrue();
    } finally {
      DLockRequestProcessor.setDebugReleaseOrphanedGrant(false);
      DLockRequestProcessor.setWaitToProcessDLockResponse(false);
    }
  }

  private static volatile Thread threadVM1_testReleaseOrphanedGrant_Remote;
  private static volatile Thread threadVM2_testReleaseOrphanedGrant_Remote;
  private static volatile boolean startedThreadVM1_testReleaseOrphanedGrant_Remote;
  private static volatile boolean releaseThreadVM1_testReleaseOrphanedGrant_Remote;
  private static volatile boolean unlockedThreadVM1_testReleaseOrphanedGrant_Remote;
  private static volatile boolean startedThreadVM2_testReleaseOrphanedGrant_Remote;

  @Test
  public void testReleaseOrphanedGrant_Remote() throws Exception {
    doTestReleaseOrphanedGrant_Remote(false);
  }

  @Test
  public void testReleaseOrphanedGrant_RemoteWithDestroy() throws Exception {
    doTestReleaseOrphanedGrant_Remote(true);
  }

  private void doTestReleaseOrphanedGrant_Remote(final boolean destroyLockService)
      throws InterruptedException {
    final VM vm1 = VM.getVM(0);
    final VM vm2 = VM.getVM(1);

    try {
      logger.info("[testReleaseOrphanedGrant_Remote] create lock service");
      final String serviceName = getUniqueName();
      final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);

      // lock and unlock to make sure this vm is grantor
      assertThat(service.lock("obj", -1, -1)).isTrue();
      service.unlock("obj");

      // thread to get lock and wait and then unlock
      vm1.invokeAsync(new SerializableRunnable() {
        public void run() {
          logger.info("[testReleaseOrphanedGrant_Remote] get the lock");
          threadVM1_testReleaseOrphanedGrant_Remote = Thread.currentThread();
          connectDistributedSystem();
          DistributedLockService service_vm1 =
              DistributedLockService.create(serviceName, getSystem());
          assertThat(service_vm1.lock("obj", -1, -1)).isTrue();
          synchronized (threadVM1_testReleaseOrphanedGrant_Remote) {
            while (!releaseThreadVM1_testReleaseOrphanedGrant_Remote) {
              try {
                startedThreadVM1_testReleaseOrphanedGrant_Remote = true;
                Thread.currentThread().wait();
              } catch (InterruptedException ignore) {
                fail("interrupted");
              }
            }
          }
          logger.info("[testReleaseOrphanedGrant_Remote] unlock the lock");
          service_vm1.unlock("obj");
          unlockedThreadVM1_testReleaseOrphanedGrant_Remote = true;
        }
      });
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          while (!startedThreadVM1_testReleaseOrphanedGrant_Remote) {
            Thread.yield();
          }
        }
      });
      Thread.sleep(500);

      // thread to interrupt lockInterruptibly call to cause zombie grant
      vm2.invokeAsync(new SerializableRunnable() {
        public void run() {
          logger
              .info("[testReleaseOrphanedGrant_Remote] call lockInterruptibly");
          threadVM2_testReleaseOrphanedGrant_Remote = Thread.currentThread();
          DistributedLockService service_vm2 =
              DistributedLockService.create(serviceName, getSystem());
          startedThreadVM2_testReleaseOrphanedGrant_Remote = true;
          try {
            DLockRequestProcessor.setDebugReleaseOrphanedGrant(true);
            DLockRequestProcessor.setWaitToProcessDLockResponse(true);
            assertThat(service_vm2.lockInterruptibly("obj", -1, -1)).isFalse();
          } catch (InterruptedException expected) {
            Thread.currentThread().interrupt();
          }
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          while (!startedThreadVM2_testReleaseOrphanedGrant_Remote) {
            Thread.yield();
          }
        }
      });
      Thread.sleep(500);

      // release first thread to unlock
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          logger
              .info("[testReleaseOrphanedGrant_Remote] release 1st thread");
          synchronized (threadVM1_testReleaseOrphanedGrant_Remote) {
            releaseThreadVM1_testReleaseOrphanedGrant_Remote = true;
            threadVM1_testReleaseOrphanedGrant_Remote.notifyAll();
          }
        }
      });
      Thread.sleep(500); // lock is being released, grantor will grant lock to vm2

      // while first thread is stuck on waitToProcessDLockResponse,
      // interrupt 2nd thread
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          logger
              .info("[testReleaseOrphanedGrant_Remote] interrupt 2nd thread");
          threadVM2_testReleaseOrphanedGrant_Remote.interrupt();
          ThreadUtils.join(threadVM2_testReleaseOrphanedGrant_Remote, 5 * 60 * 1000);
          if (destroyLockService) {
            logger
                .info("[testReleaseOrphanedGrant_Remote] destroy lock service");
            DistributedLockService.destroy(serviceName);
            assertThat(DistributedLockService.getServiceNamed(serviceName)).isNull();
          }
        }
      });
      Thread.sleep(500); // grant is blocked while reply processor is being destroyed

      // release waitToProcessDLockResponse
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          logger
              .info("[testReleaseOrphanedGrant_Remote] process lock response");
          DLockRequestProcessor.setWaitToProcessDLockResponse(false);
        }
      });
      Thread.sleep(500); // process grant and send zombie release to grantor

      // relock obj to make sure zombie release worked
      logger.info("[testReleaseOrphanedGrant_Remote] verify lock not held");
      assertThat(service.lock("obj", 1000, -1)).isTrue();
    } finally {
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          logger
              .info("[testReleaseOrphanedGrant_Remote] clean up DebugReleaseOrphanedGrant");
          DLockRequestProcessor.setDebugReleaseOrphanedGrant(false);
          DLockRequestProcessor.setWaitToProcessDLockResponse(false);
        }
      });
    }
  }

  @Test
  public void testDestroyLockServiceAfterGrantResponse() {
    VM vm0 = VM.getVM(0);

    final String serviceName = getUniqueName();

    vm0.invoke(() -> createLockGrantor(serviceName));

    DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

      @Override
      public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof DLockResponseMessage) {
          DistributedLockService.destroy(serviceName);
        }
      }
    });

    connectDistributedSystem();
    final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    try {
      service.lock("obj", -1, -1);
      fail("The lock service should have been destroyed");
    } catch (LockServiceDestroyedException expected) {
      // Do nothing
    }

    vm0.invoke(new SerializableRunnable("check to make sure the lock is not orphaned") {

      public void run() {
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

        // lock and unlock to make sure this vm is grantor
        assertThat(service.lock("obj", -1, -1)).isTrue();
        service.unlock("obj");
      }
    });
  }

  @Test
  public void testDestroyLockServiceBeforeGrantRequest() {
    VM vm0 = VM.getVM(0);

    final String serviceName = getUniqueName();

    vm0.invoke(() -> createLockGrantor(serviceName));

    DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

      @Override
      public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof DLockRequestMessage) {
          DistributedLockService.destroy(serviceName);
        }
      }
    });

    connectDistributedSystem();
    final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    try {
      service.lock("obj", -1, -1);
      fail("The lock service should have been destroyed");
    } catch (LockServiceDestroyedException expected) {
      // Do nothing
    }

    vm0.invoke(new SerializableRunnable("check to make sure the lock is not orphaned") {

      public void run() {
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

        // lock and unlock to make sure this vm is grantor
        assertThat(service.lock("obj", -1, -1)).isTrue();
        service.unlock("obj");
      }
    });
  }

  private static void createLockGrantor(String serviceName) {
    connectDistributedSystem();
    final DistributedLockService service =
        DistributedLockService.create(serviceName, dlstSystem);

    // lock and unlock to make sure this vm is grantor
    // implementation detail: as long as this VM gets the first lock, it will be the grantor
    assertThat(service.lock("obj", -1, -1)).isTrue();
    service.unlock("obj");
  }

  ////////// Private test methods

  protected synchronized boolean getDone() {
    return done;
  }

  protected synchronized void setDone(boolean done) {
    this.done = done;
  }

  private synchronized boolean getGot() {
    return got;
  }

  protected synchronized void setGot(boolean got) {
    this.got = got;
  }

  protected static Boolean lock(String serviceName, Object name) {
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    boolean locked = service.lock(name, 1000, -1);
    return locked;
  }

  protected static Boolean tryLock(String serviceName, Object name, Long wait) {
    DLockService service = DLockService.getInternalServiceNamed(serviceName);
    boolean locked = service.lock(name, wait, -1, true);
    return locked;
  }

  protected static Boolean unlock(String serviceName, Object name) {
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    try {
      service.unlock(name);
      return Boolean.TRUE;
    } catch (LockNotHeldException e) {
      return Boolean.FALSE;
    } catch (Exception e) {
      e.printStackTrace();
      return Boolean.FALSE;
    }
  }

  private static Boolean tryToLock(String serviceName) {
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    boolean locked = service.lock("obj", 1000, -1);
    if (locked) {
      service.unlock("obj");
    }
    return locked;
  }

  private void doOneGetsAndOthersTimeOut(int numVMs, int numThreadsPerVM) throws Exception {

    final String serviceName = getUniqueName() + "-" + numVMs + "-" + numThreadsPerVM;
    final String objectName = "obj";

    System.out.println("Starting testtt " + serviceName);

    distributedCreateService(numVMs, serviceName, true);
    hits = 0;
    completes = 0;
    blackboard.initCount();
    blackboard.setIsLocked(false);

    // tell them all to request a lock and increment
    long timeout = 1000;
    long holdTime = (timeout * 5);
    final Host host = Host.getHost(0);
    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      for (int thread = 0; thread < numThreadsPerVM; thread++) {
        final int finalthread = thread;
        (new Thread(() -> {
          Boolean result = VM.getVM(finalvm)
              .invoke(() -> DistributedLockServiceDUnitTest.getLockAndIncrement(
                  serviceName, objectName, timeout, holdTime));
          if (result) {
            incHits();
          }
          incCompletes();
        }, "doOneGetsAndOthersTimeOut-" + thread)).start();
      }
    }

    // wait for timeout
    // wait for completion or timeout
    long start = System.currentTimeMillis();
    while ((completes < numVMs * numThreadsPerVM)
        && (System.currentTimeMillis() - start < holdTime * 10)) {
      Thread.sleep(200);
    }

    // assert that only one got ownership
    if (hits != 1) {
      ThreadUtils.dumpAllStacks();
    }
    assertThat(hits).isEqualTo(1)
        .withFailMessage("number of VMs that got ownership is wrong");

    // assert that all the others timed out
    assertThat(completes).isEqualTo(numVMs * numThreadsPerVM)
        .withFailMessage("number of threads that completed is wrong");

    // Check final value of entry
    long count = blackboard.getCount();
    assertThat(count).isEqualTo(1).withFailMessage("Final entry value wrong");

    System.out.println("Done testtt " + serviceName);
  }

  // 2, 2... expect 4, but get 3
  private void doOneGetsThenOtherGets(int numVMs, int numThreadsPerVM) throws Exception {

    final String serviceName = getUniqueName() + "-" + numVMs + "-" + numThreadsPerVM;
    final String objectName = "obj";

    System.out.println("Starting testtt " + serviceName);

    distributedCreateService(numVMs, serviceName, true);
    hits = 0;
    completes = 0;
    blackboard.initCount();
    blackboard.setIsLocked(false);

    // tell all VMs to lock, long timeout, short hold time
    // each one gets lock, increments and releases
    final long timeout = Math.max(1000 * numVMs * numThreadsPerVM, 10 * 1000); // at least 10
                                                                               // seconds

    final Host host = Host.getHost(0);
    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      for (int thread = 0; thread < numThreadsPerVM; thread++) {
        final int finalthread = thread;
        (new Thread(() -> {

          System.out.println("VM " + finalvm + ", thread " + finalthread + " in " + serviceName
              + " about to invoke");

          Boolean result = null;
          try {
            result = VM.getVM(finalvm)
                .invoke(() -> DistributedLockServiceDUnitTest.getLockAndIncrement(
                    serviceName, objectName, timeout, 0L));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          System.out.println("VM " + finalvm + ", thread " + finalthread + " in " + serviceName
              + " got result " + result);
          if (result) {
            incHits();
          }
          incCompletes();
        })).start();
      }
    }

    // wait for completion or timeout
    long start = System.currentTimeMillis();
    while (completes < numVMs * numThreadsPerVM) {
      if (!(System.currentTimeMillis() - start < timeout * 2)) {
        System.out.println("Test serviceName timed out");
        break;
      }
      Thread.sleep(200);
    }

    // assert that all completed
    assertThat(completes).isEqualTo(numVMs * numThreadsPerVM)
        .withFailMessage("number of threads that completed is wrong");
    // -------------------------------------------------------

    // assert that all were able to lock
    if (hits != numVMs * numThreadsPerVM) {
      ThreadUtils.dumpAllStacks();
    }
    assertThat(hits).isEqualTo(numVMs * numThreadsPerVM)
        .withFailMessage("number of VMs that got ownership is wrong");

    // Check final value of entry
    long count = blackboard.getCount();
    assertThat(count).isEqualTo(numVMs * numThreadsPerVM)
        .withFailMessage("Blackboard.getCount() wrong");

    System.out.println("Done testtt " + serviceName);

  }

  @Test
  public void testTokenCleanup() {
    final String dlsName = getUniqueName();

    final VM vmGrantor = VM.getVM(0);
    final VM vm1 = VM.getVM(1);
    // final VM vm2 =VM.getVM(2);

    final String key1 = "key1";

    // vmGrantor creates grantor
    vmGrantor.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vmGrantor creates grantor");
        connectDistributedSystem();
        DLockService dls = (DLockService) DistributedLockService.create(dlsName, getSystem());

        assertThat(dls.lock(key1, -1, -1)).isTrue();
        assertThat(dls.isLockGrantor()).isTrue();
        assertThat(dls.getToken(key1)).isNotNull();

        dls.unlock(key1);
        assertThat(dls.getToken(key1)).isNotNull();

        // token should be removed when freeResources is called
        dls.freeResources(key1);
        // assertThat(dls.getToken(key1)).isNull();

        DLockToken token = dls.getToken(key1);
        assertThat(token).withFailMessage("Failed with bug 38180: " + token).isNull();

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertThat(tokens.size()).isEqualTo(0)
            .withFailMessage("Failed with bug 38180: tokens=" + tokens);
      }
    });

    // vm1 locks and frees key1
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vm1 locks key1");
        connectDistributedSystem();
        DLockService dls = (DLockService) DistributedLockService.create(dlsName, getSystem());

        assertThat(dls.lock(key1, -1, -1)).isTrue();
        assertThat(dls.isLockGrantor()).isFalse();
        assertThat(dls.getToken(key1)).isNotNull();

        dls.unlock(key1);
        assertThat(dls.getToken(key1)).isNotNull();

        dls.freeResources(key1);
        // assertThat(dls.getToken(key1)).isNull();

        DLockToken token = dls.getToken(key1);
        assertThat(token).withFailMessage("Failed with bug 38180: " + token).isNull();

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertThat(tokens.size()).isEqualTo(0)
            .withFailMessage("Failed with bug 38180: tokens=" + tokens);
      }
    });

    // vm1 tests recursion
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vm1 tests recursion");
        connectDistributedSystem();
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

        assertThat(dls.lock(key1, -1, -1)).isTrue(); // 1
        assertThat(dls.getToken(key1).getUsageCount()).isEqualTo(1);
        assertThat(dls.lock(key1, -1, -1)).isTrue(); // 2
        assertThat(dls.getToken(key1).getUsageCount()).isEqualTo(2);
        assertThat(dls.lock(key1, -1, -1)).isTrue(); // 3
        assertThat(dls.getToken(key1).getUsageCount()).isEqualTo(3);

        DLockToken token0 = dls.getToken(key1);
        assertThat(token0).isNotNull();
        Collection tokens = dls.getTokens();
        assertThat(tokens.contains(token0)).isTrue();
        assertThat(tokens.size()).isEqualTo(1);

        dls.unlock(key1); // 1
        assertThat(dls.getToken(key1).getUsageCount()).isEqualTo(2);
        dls.freeResources(key1);

        DLockToken token1 = dls.getToken(key1);
        assertThat(token1).isNotNull();
        assertThat(token1).isEqualTo(token0);
        tokens = dls.getTokens();
        assertThat(tokens.contains(token1)).isTrue();
        assertThat(tokens.size()).isEqualTo(1);

        dls.unlock(key1); // 2
        assertThat(dls.getToken(key1).getUsageCount()).isEqualTo(1);
        dls.freeResources(key1);
        assertThat(dls.getToken(key1)).isNotNull();

        DLockToken token2 = dls.getToken(key1);
        assertThat(token2).isNotNull();
        assertThat(token2).isEqualTo(token0);
        tokens = dls.getTokens();
        assertThat(tokens.contains(token2)).isTrue();
        assertThat(tokens.size()).isEqualTo(1);

        dls.unlock(key1); // 3
        assertThat(dls.getToken(key1).getUsageCount()).isEqualTo(0);
        dls.freeResources(key1);

        DLockToken token3 = dls.getToken(key1);
        assertThat(token3).withFailMessage("Failed with bug 38180: " + token3).isNull();

        // make sure there are NO tokens at all
        tokens = dls.getTokens();
        assertThat(tokens.size()).isEqualTo(0)
            .withFailMessage("Failed with bug 38180: tokens=" + tokens);
      }
    });
  }

  @Test
  public void testGrantTokenCleanup() {
    final String dlsName = getUniqueName();

    final VM vmGrantor = VM.getVM(0);
    final VM vm1 = VM.getVM(1);

    final String key1 = "key1";

    // vmGrantor creates grantor
    vmGrantor.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testGrantTokenCleanup] vmGrantor creates grantor");
        connectDistributedSystem();
        DistributedLockService dls = DLockService.create(dlsName, getSystem(), true, true, true);
        assertThat(dls.lock(key1, -1, -1)).isTrue();
        assertThat(dls.isLockGrantor()).isTrue();
        DLockGrantor grantor = ((DLockService) dls).getGrantor();
        assertThat(grantor).isNotNull();
        DLockGrantor.DLockGrantToken grantToken = grantor.getGrantToken(key1);
        assertThat(grantToken).isNotNull();
        logger.info("[testGrantTokenCleanup] vmGrantor unlocks key1");
        dls.unlock(key1);
        assertThat(grantor.getGrantToken(key1)).isNull();
      }
    });

    // vm1 locks and frees key1
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vm1 locks key1");
        connectDistributedSystem();
        DLockService dls =
            (DLockService) DLockService.create(dlsName, getSystem(), true, true, false);
        assertThat(dls.lock(key1, -1, -1)).isTrue();

        logger.info("[testTokenCleanup] vm1 frees key1");
        dls.unlock(key1);

        // Without automateFreeResources, token for key1 still exists until freeResources is called
        assertThat(dls.getToken(key1)).isNotNull();
        dls.freeResources(key1);

        // make sure token for key1 is gone
        DLockToken token = dls.getToken(key1);
        assertThat(token).withFailMessage("token should have been cleaned up").isNull();

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertThat(tokens.size()).isEqualTo(0)
            .withFailMessage("There should be no tokens");
      }
    });

    // vmGrantor frees key1
    vmGrantor.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vmGrantor frees key1");
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

        // Because automateFreeResources is true, DLockToken and DLockGrantToken should have been
        // removed when vm1 unlocked key1

        // make sure token for key1 is gone
        DLockToken token = dls.getToken(key1);
        assertThat(token).withFailMessage("token should have been cleaned up").isNull();

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertThat(tokens.size()).isEqualTo(0)
            .withFailMessage("There should be no tokens");

        // make sure there are NO grant tokens at all
        DLockGrantor grantor = dls.getGrantor();
        Collection grantTokens = grantor.getGrantTokens();
        assertThat(grantTokens.size()).isEqualTo(0)
            .withFailMessage("There should be no tokens");
      }
    });
  }

  private static final AtomicBoolean testLockQuery_whileVM1Locks = new AtomicBoolean();

  @Test
  public void testLockQuery() {
    final String dlsName = getUniqueName();

    final VM vmGrantor = VM.getVM(0);
    final VM vm1 = VM.getVM(1);
    final VM vm2 = VM.getVM(2);

    final String key1 = "key1";

    // vmGrantor creates grantor
    vmGrantor.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testLockQuery] vmGrantor creates grantor");
        connectDistributedSystem();
        DLockService dls = (DLockService) DistributedLockService.create(dlsName, getSystem());

        assertThat(dls.lock(key1, -1, -1)).isTrue();
        assertThat(dls.isLockGrantor()).isTrue();
        dls.unlock(key1);
        dls.freeResources(key1);
      }
    });

    AsyncInvocation whileVM1Locks = null;
    try {
      // vm1 locks key1
      whileVM1Locks = vm1.invokeAsync(new SerializableRunnable() {
        public void run() {
          logger.info("[testLockQuery] vm1 locks key1");
          connectDistributedSystem();
          DLockService dls = (DLockService) DistributedLockService.create(dlsName, getSystem());

          assertThat(dls.lock(key1, -1, -1)).isTrue();
          assertThat(dls.isLockGrantor()).isFalse();

          try {
            synchronized (testLockQuery_whileVM1Locks) {
              testLockQuery_whileVM1Locks.set(true);
              testLockQuery_whileVM1Locks.notifyAll();
              long maxWait = 10000;
              StopWatch timer = new StopWatch(true);
              while (testLockQuery_whileVM1Locks.get()) { // while true
                long timeLeft = maxWait - timer.elapsedTimeMillis();
                if (timeLeft > 0) {
                  testLockQuery_whileVM1Locks.wait(timeLeft);
                } else {
                  fail("Test attempted to wait too long");
                }
              }
            }
          } catch (InterruptedException e) {
            org.apache.geode.test.dunit.Assert.fail(e.getMessage(), e);
          }

          logger.info("[testLockQuery] vm1 unlocks key1");
          dls.unlock(key1);
          dls.freeResources(key1);
        }
      });

      // wait for vm1 to set testLockQuery_whileVM1Locks
      // get DistributedMember for vm1
      final DistributedMember vm1Member = vm1.invoke(() -> {
        logger.info("[testLockQuery] vm1 waits for locking thread");
        synchronized (testLockQuery_whileVM1Locks) {
          long maxWait = 10000;
          StopWatch timer = new StopWatch(true);
          while (!testLockQuery_whileVM1Locks.get()) { // while false
            long timeLeft = maxWait - timer.elapsedTimeMillis();
            if (timeLeft > 0) {
              testLockQuery_whileVM1Locks.wait(timeLeft);
            } else {
              fail("Test attempted to wait too long");
            }
          }
        }
        return getSystem().getDistributedMember();
      });
      assertThat(vm1Member).isNotNull();

      // vmGrantor tests positive local dlock query
      vmGrantor.invoke(new SerializableRunnable() {
        public void run() {
          logger.info("[testLockQuery] vmGrantor tests local query");
          DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

          DLockRemoteToken result = dls.queryLock(key1);
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(key1);
          assertThat(result.getLeaseId() != -1).isTrue();
          assertThat(result.getLeaseExpireTime()).isEqualTo(MAX_VALUE);
          RemoteThread lesseeThread = result.getLesseeThread();
          assertThat(lesseeThread).isNotNull();
          assertThat(lesseeThread.getDistributedMember()).isEqualTo(vm1Member);
          assertThat(result.getLessee()).isEqualTo(vm1Member);
          // nothing to test for on threadId unless we serialize info from vm1
        }
      });

      // vm2 tests positive remote dlock query
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          logger.info("[testLockQuery] vm2 tests remote query");
          connectDistributedSystem();
          DLockService dls = (DLockService) DistributedLockService.create(dlsName, getSystem());

          DLockRemoteToken result = dls.queryLock(key1);
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(key1);
          assertThat(result.getLeaseId() != -1).isTrue();
          assertThat(result.getLeaseExpireTime()).isEqualTo(MAX_VALUE);
          RemoteThread lesseeThread = result.getLesseeThread();
          assertThat(lesseeThread).isNotNull();
          assertThat(lesseeThread.getDistributedMember()).isEqualTo(vm1Member);
          assertThat(result.getLessee()).isEqualTo(vm1Member);
          // nothing to test for on threadId unless we serialize info from vm1
        }
      });

    } finally { // guarantee that testLockQuery_whileVM1Locks is notfied!
      // vm1 sets and notifies testLockQuery_whileVM1Locks to release lock
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          logger.info("[testLockQuery] vm1 notifies/releases key1");
          synchronized (testLockQuery_whileVM1Locks) {
            testLockQuery_whileVM1Locks.set(false);
            testLockQuery_whileVM1Locks.notifyAll();
          }
        }
      });

      ThreadUtils.join(whileVM1Locks, 10 * 1000);
      if (whileVM1Locks.exceptionOccurred()) {
        org.apache.geode.test.dunit.Assert.fail("Test failed", whileVM1Locks.getException());
      }
    }

    // vmGrantor tests negative local dlock query
    vmGrantor.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testLockQuery] vmGrantor tests negative query");
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

        DLockRemoteToken result = dls.queryLock(key1);
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo(key1);
        assertThat(result.getLeaseId()).isEqualTo(-1);
        assertThat(result.getLeaseExpireTime()).isEqualTo(0);
        assertThat(result.getLesseeThread()).isNull();
        assertThat(result.getLessee()).isNull();
      }
    });

    // vm2 tests negative remote dlock query
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testLockQuery] vm2 tests negative query");
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

        DLockRemoteToken result = dls.queryLock(key1);
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo(key1);
        assertThat(result.getLeaseId()).isEqualTo(-1);
        assertThat(result.getLeaseExpireTime()).isEqualTo(0);
        assertThat(result.getLesseeThread()).isNull();
        assertThat(result.getLessee()).isNull();
      }
    });

  }

  ////////// Support methods

  private List<VM> distributedCreateService(int numVMs, String serviceName, boolean useLocalVM) {
    // create an entry - use scope DIST_ACK, not GLOBAL, since we're testing
    // that explicit use of the ownership api provides the synchronization

    final List<VM> vms = new ArrayList<>();

    if (useLocalVM) {
      vms.add(VM.getVM(VM.getCurrentVMNum()));
      remoteCreateService(serviceName);
    }

    vms.addAll(forNumVMsInvoke(numVMs, () -> remoteCreateService(serviceName)));
    // remoteCreateService(serviceName);

    return vms;


  }

  /**
   * Creates a new DistributedLockService in a remote VM.
   *
   * @param name The name of the newly-created DistributedLockService. It is recommended that the
   *        name of the Region be the {@link #getUniqueName()} of the test, or at least derive from
   *        it.
   */
  protected static void remoteCreateService(String name) {
    DistributedLockService newService = DistributedLockService.create(name, dlstSystem);
    System.out.println("Created " + newService);
  }

  private static Boolean getLockAndIncrement(String serviceName, Object objectName, long timeout,
      long holdTime) throws Exception {
    System.out.println("[getLockAndIncrement] In getLockAndIncrement");
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    boolean got = service.lock(objectName, timeout, -1);
    System.out.println("[getLockAndIncrement] In getLockAndIncrement - got is " + got);
    if (got) {
      // Make sure we don't think anyone else is holding the lock
      if (blackboard.getIsLocked()) {
        String msg = "obtained lock on " + serviceName + "/" + objectName
            + " but blackboard was locked, grantor=" + ((DLockService) service).getLockGrantorId()
            + ", isGrantor=" + service.isLockGrantor();
        System.out.println("[getLockAndIncrement] In getLockAndIncrement: " + msg);
        fail(msg);
      }
      blackboard.setIsLocked(true);
      long count = blackboard.getCount();
      System.out
          .println("[getLockAndIncrement] In getLockAndIncrement - count is " + count + " for "
              + serviceName + "/" + objectName);
      Thread.sleep(holdTime);
      blackboard.incCount();
      blackboard.setIsLocked(false);
      System.out
          .println("[getLockAndIncrement] In getLockAndIncrement: " + "cleared blackboard lock for "
              + serviceName + "/" + objectName);
      service.unlock(objectName);
    }
    System.out.println("[getLockAndIncrement] Returning from getLockAndIncrement");
    return got;
  }

  private synchronized void incHits() {
    hits = hits + 1;
  }

  private synchronized void incCompletes() {
    completes = completes + 1;
  }

  /**
   * Assumes there is only one host, and invokes the given method in the first numVMs VMs that host
   * knows about.
   */
  private List<VM> forNumVMsInvoke(int numVMs, final SerializableRunnableIF runnable) {
    List<VM> vms = new ArrayList<>();
    for (int i = 0; i < numVMs; i++) {
      VM thisVm = VM.getVM(i);
      thisVm.invoke(runnable);
      vms.add(thisVm);
    }

    return vms;
  }

  public static class BasicLockClient implements Runnable {
    private static Logger logger = LogService.getLogger();
    private static final Integer LOCK = 1;
    private static final Integer UNLOCK = 2;
    private static final Integer SUSPEND = 3;
    private static final Integer RESUME = 4;
    private static final Integer STOP = 5;
    private final Object sync = new Object();
    private final Thread thread;
    private final String dlsName;
    private final String key;
    // ordered queue of requests
    private final LinkedList requests = new LinkedList();
    // map of requests to operations
    private final Map operationsMap = new HashMap();
    // map of requests to throwables
    private final Map throwables = new HashMap();
    private final Set completedRequests = new HashSet();
    private int latestRequest = 0;
    private boolean stayinAlive = true;

    public BasicLockClient(String dlsName, String key) {
      this.dlsName = dlsName;
      this.key = key;
      this.thread = new Thread(this);
      this.thread.start();
    }

    public void run() {
      logger.info("BasicLockClient running");
      while (this.stayinAlive) {
        synchronized (this.sync) {
          if (this.requests.size() > 0) {
            Integer requestId = (Integer) this.requests.removeFirst();
            Integer operationId = (Integer) this.operationsMap.get(requestId);
            try {
              switch (operationId) {
                case 1:
                  logger.info("BasicLockClient lock");
                  assertThat(DistributedLockService.getServiceNamed(dlsName).lock(key, -1, -1))
                      .isTrue();
                  break;
                case 2:
                  logger.info("BasicLockClient unlock");
                  DistributedLockService.getServiceNamed(dlsName).unlock(key);
                  break;
                case 3:
                  logger.info("BasicLockClient suspendLocking");
                  assertThat(DistributedLockService.getServiceNamed(dlsName).suspendLocking(-1))
                      .isTrue();
                  break;
                case 4:
                  logger.info("BasicLockClient resumeLocking");
                  DistributedLockService.getServiceNamed(dlsName).resumeLocking();
                  break;
                case 5:
                  logger.info("BasicLockClient stopping");
                  this.stayinAlive = false;
                  break;
              } // switch
            } // try
            catch (VirtualMachineError e) {
              SystemFailure.initiateFailure(e);
              throw e;
            } catch (Throwable t) {
              this.throwables.put(requestId, t);
            } finally {
              this.completedRequests.add(requestId);
              this.sync.notify();
            }
          }
          try {
            this.sync.wait();
          } catch (InterruptedException e) {
            logger.info("BasicLockClient interrupted");
            this.stayinAlive = false;
          }
        } // sync
      } // while
    }

    public void lock() throws Error {
      doLock(LOCK);
    }

    public void unlock() throws Error {
      doLock(UNLOCK);
    }

    public void suspend() throws Error {
      doLock(SUSPEND);
    }

    public void resume() throws Error {
      doLock(RESUME);
    }

    public void stop() throws Error {
      doLock(STOP);
    }

    private void doLock(Integer lock) {
      try {
        synchronized (this.sync) {
          this.latestRequest++;
          Integer requestId = this.latestRequest;
          this.operationsMap.put(requestId, lock);
          this.requests.add(requestId);
          this.sync.notify();
          long maxWait = System.currentTimeMillis() + 2000;
          while (!this.completedRequests.contains(requestId)) {
            long waitMillis = maxWait - System.currentTimeMillis();
            assertThat(waitMillis > 0).isTrue();
            this.sync.wait(waitMillis);
          }
          Throwable t = (Throwable) this.throwables.get(requestId);
          if (t != null) {
            throw new Error(t);
          }
        }
      } catch (Exception ex) {
        throw new Error(ex);
      }
    }
  }
}
