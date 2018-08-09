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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
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
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.RMIException;
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
  public final void preTearDown() throws Exception {
    Invoke.invokeInEveryVM(() -> destroyAllDLockServices());
    this.lockGrantor = null;
  }

  @Override
  public void postTearDown() throws Exception {
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

  @Test
  public void testBasic() {
    String serviceName = getUniqueName();
    String objectName = "object";

    // Create service
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);

    // Not locked initially
    assertFalse(service.isHeldByCurrentThread(objectName));

    // Get lock
    assertTrue(service.lock(objectName, 3000, -1));
    assertTrue(service.isHeldByCurrentThread(objectName));
    assertTrue(service.lock(objectName, 3000, -1));
    assertTrue(service.isHeldByCurrentThread(objectName));

    // Release lock
    service.unlock(objectName);
    assertTrue(service.isHeldByCurrentThread(objectName));
    service.unlock(objectName);
    assertFalse(service.isHeldByCurrentThread(objectName));

    // Destroy service
    DistributedLockService.destroy(serviceName);
  }

  @Test
  public void testCreateDestroy() throws Exception {
    final String serviceName = getUniqueName();
    final String abc = "abc";

    // create and destroy dls
    assertNull(DistributedLockService.getServiceNamed(serviceName));
    DistributedLockService service = DistributedLockService.create(serviceName, getSystem());
    assertSame(service, DistributedLockService.getServiceNamed(serviceName));
    DistributedLockService.destroy(serviceName);

    final DistributedLockService svcForLambda = service;

    assertThatThrownBy(() -> svcForLambda.lock(abc, -1, -1))
        .isInstanceOf(LockServiceDestroyedException.class);

    // assert that destroyed dls is no longer available
    assertNull(DistributedLockService.getServiceNamed(serviceName));

    // recreate the dls
    service = DistributedLockService.create(serviceName, getSystem());
    assertTrue(!((DLockService) service).isDestroyed());
    ((DLockService) service).checkDestroyed();

    // get the same dls from another thread and hold a lock
    Thread thread = new Thread(new Runnable() {
      public void run() {
        DistributedLockService dls = DistributedLockService.getServiceNamed(serviceName);
        assertTrue(!((DLockService) dls).isDestroyed());
        ((DLockService) dls).checkDestroyed();
        dls.lock(abc, -1, -1); // get lock on abc and hold it
      }
    });
    thread.start();
    ThreadUtils.join(thread, 30 * 1000);

    // start a new thread to wait for lock on abc
    AsyncInvocation remoteWaitingThread =
        VM.getVM(0).invokeAsync(new SerializableRunnable() {
          public void run() {
            DistributedLockService dls = DistributedLockService.create(serviceName, getSystem());
            try {
              dls.lock(abc, -1, -1); // waiting to get lock abc
              fail("remoteWaitingThread got lock after dls destroyed");
            } catch (LockServiceDestroyedException expected) {
              return;
            }
            fail("remoteWaitingThread lock failed to throw LockServiceDestroyedException");
          }
        });

    // loop will handle race condition with 1 sec sleep and retry
    int retry = 10;
    for (int i = 0; i < retry; i++) {
      try {
        // destroy DLS and free up remoteWaitingThread
        VM.getVM(0).invoke(new SerializableRunnable() {
          public void run() {
            DistributedLockService.destroy(serviceName);
          }
        });
      } catch (RMIException e) {
        // race condition: remoteWaitingThread probably hasn't created DLS yet
        if (i < retry && e.getCause() instanceof IllegalArgumentException) {
          Thread.sleep(1000);
          continue;
        } else {
          throw e;
        }
      }
      break; // completed so break out of loop
    }

    DistributedLockService.destroy(serviceName);

    // make sure remoteWaitingThread stopped waiting and threw LockServiceDestroyedException
    ThreadUtils.join(remoteWaitingThread, 10 * 1000);
    if (remoteWaitingThread.exceptionOccurred()) {
      Throwable e = remoteWaitingThread.getException();
      org.apache.geode.test.dunit.Assert.fail(e.getMessage(), e);
    }

    // make sure LockServiceDestroyedException is thrown
    try {
      service.lock(abc, -1, -1);
      fail("didn't get LockServiceDestroyedException");
    } catch (LockServiceDestroyedException ex) {
    }

    // make sure getServiceNamed returns null
    service = DistributedLockService.getServiceNamed(serviceName);
    assertNull("" + service, service);
  }

  private static DistributedLockService dls_testFairness;
  private static int count_testFairness[] = new int[16];
  private static volatile boolean stop_testFairness;
  private static volatile boolean[] done_testFairness = new boolean[16];
  static {
    Arrays.fill(done_testFairness, true);
  }

  @Test
  public void testFairness() throws Exception {
    final String serviceName = "testFairness_" + getUniqueName();
    final Object lock = "lock";

    // get the lock and hold it until all threads are ready to go
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    assertTrue(service.lock(lock, -1, -1));

    final int[] vmThreads = new int[] {1, 4, 8, 16};
    forNumVMsInvoke(vmThreads.length, () -> remoteCreateService(serviceName));
    Thread.sleep(100);

    // line up threads for the fairness race...
    for (int i = 0; i < vmThreads.length; i++) {
      final int vm = i;
      logger
          .info("[testFairness] lining up " + vmThreads[vm] + " threads in vm " + vm);

      for (int j = 0; j < vmThreads[vm]; j++) {
        final int thread = j;
        /*
         * getLogWriter().info("[testFairness] setting up thread " + thread + " in vm " + vm);
         */

        VM.getVM(vm).invokeAsync(new SerializableRunnable() {
          public void run() {
            // lock, inc count, and unlock until stop_testFairness is set true
            try {
              done_testFairness[thread] = false;
              dls_testFairness = DistributedLockService.getServiceNamed(serviceName);
              while (!stop_testFairness) {
                assertTrue(dls_testFairness.lock(lock, -1, -1));
                count_testFairness[thread]++;
                dls_testFairness.unlock(lock);
              }
              done_testFairness[thread] = true;
            } catch (VirtualMachineError e) {
              SystemFailure.initiateFailure(e);
              throw e;
            } catch (Throwable t) {
              logger.warn(t);
              fail(t.getMessage());
            }
          }
        });
      }
    }
    Thread.sleep(500); // 500 ms

    // start the race!
    service.unlock(lock);
    Thread.sleep(1000 * 5); // 5 seconds
    assertTrue(service.lock(lock, -1, -1));

    // stop the race...
    for (int i = 0; i < vmThreads.length; i++) {
      final int vm = i;
      VM.getVM(vm).invoke(new SerializableRunnable() {
        public void run() {
          stop_testFairness = true;
        }
      });
    }
    service.unlock(lock);
    for (int i = 0; i < vmThreads.length; i++) {
      final int vm = i;
      VM.getVM(vm).invoke(new SerializableRunnable() {
        public void run() {
          try {
            boolean testIsDone = false;
            while (!stop_testFairness || !testIsDone) {
              testIsDone = true;
              for (int i2 = 0; i2 < done_testFairness.length; i2++) {
                if (!done_testFairness[i2])
                  testIsDone = false;
              }
            }
            DistributedLockService.destroy(serviceName);
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            fail(t.getMessage());
          }
        }
      });
    }

    // calc total locks granted...
    int totalLocks = 0;
    int minLocks = Integer.MAX_VALUE;
    int maxLocks = 0;

    // add up total locks across all vms and threads...
    int numThreads = 0;
    for (int i = 0; i < vmThreads.length; i++) {
      final int vm = i;
      for (int j = 0; j < vmThreads[vm]; j++) {
        final int thread = j;
        Integer count = VM.getVM(vm).invoke(
            () -> DistributedLockServiceDUnitTest.get_count_testFairness(thread));
        int numLocks = count;
        if (numLocks < minLocks)
          minLocks = numLocks;
        if (numLocks > maxLocks)
          maxLocks = numLocks;
        totalLocks = totalLocks + numLocks;
        numThreads++;
      }
    }

    logger.info("[testFairness] totalLocks=" + totalLocks + " minLocks="
        + minLocks + " maxLocks=" + maxLocks);

    int expectedLocks = (totalLocks / numThreads) + 1;

    int deviation = (int) (expectedLocks * 0.3);
    int lowThreshold = expectedLocks - deviation;
    int highThreshold = expectedLocks + deviation;

    logger.info("[testFairness] deviation=" + deviation + " expectedLocks="
        + expectedLocks + " lowThreshold=" + lowThreshold + " highThreshold=" + highThreshold);

    assertTrue("minLocks is less than lowThreshold", minLocks >= lowThreshold);
    assertTrue("maxLocks is greater than highThreshold", maxLocks <= highThreshold);
  }

  private static Integer get_count_testFairness(Integer i) {
    return count_testFairness[i];
  }

  @Test
  public void testOneGetsAndOthersTimeOut() throws Exception {
    doOneGetsAndOthersTimeOut(1, 1);
    // doOneGetsAndOthersTimeOut(2, 2);
    // doOneGetsAndOthersTimeOut(3, 2);
    doOneGetsAndOthersTimeOut(4, 3);
  }

  private InternalDistributedMember lockGrantor;

  private synchronized void assertGrantorIsConsistent(InternalDistributedMember id) {
    if (this.lockGrantor == null) {
      this.lockGrantor = id;
    } else {
      assertEquals("assertGrantorIsConsistent failed", lockGrantor, id);
    }
  }

  private static InternalDistributedMember identifyLockGrantor(String serviceName) {
    DLockService service = (DLockService) DistributedLockService.getServiceNamed(serviceName);
    assertNotNull(service);
    InternalDistributedMember grantor = service.getLockGrantorId().getLockGrantorMember();
    assertNotNull(grantor);
    logInfo("In identifyLockGrantor - grantor is " + grantor);
    return grantor;
  }

  private static Boolean isLockGrantor(String serviceName) {
    DLockService service = (DLockService) DistributedLockService.getServiceNamed(serviceName);
    assertNotNull(service);
    Boolean result = service.isLockGrantor();
    logInfo("In isLockGrantor: " + result);
    return result;
  }

  protected static void becomeLockGrantor(String serviceName) {
    DLockService service = (DLockService) DistributedLockService.getServiceNamed(serviceName);
    assertNotNull(service);
    logInfo("About to call becomeLockGrantor...");
    service.becomeLockGrantor();
  }

  @Test
  public void testGrantorSelection() {
    // TODO change distributedCreateService usage to be concurrent threads

    // bring up 4 members and make sure all identify one as grantor
    int numVMs = 4;
    final String serviceName = "testGrantorSelection_" + getUniqueName();
    distributedCreateService(numVMs, serviceName);

    for (int vm = 0; vm < numVMs; vm++) {
      logInfo("VM " + vm + " in " + serviceName + " about to invoke");
      InternalDistributedMember id = VM.getVM(vm)
          .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(
              serviceName));
      logInfo("VM " + vm + " in " + serviceName + " got " + id);
      assertGrantorIsConsistent(id);
    }
  }

  @Test
  public void testBasicGrantorRecovery() {
    // 1) start up 4 VM members...
    int numVMs = 4;
    final String serviceName = "testBasicGrantorRecovery_" + getUniqueName();
    distributedCreateService(numVMs, serviceName);
    try {
      Thread.sleep(100);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    int originalGrantor = 3;
    VM.getVM(originalGrantor)
        .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(serviceName));

    int originalVM = -1;
    InternalDistributedMember oldGrantor = null;

    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      Boolean isGrantor = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(
              serviceName));
      if (isGrantor) {
        originalVM = vm;
        oldGrantor = VM.getVM(finalvm)
            .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(
                serviceName));
        break;
      }
    }

    assertTrue(originalVM == originalGrantor);

    VM.getVM(originalVM).invoke(new SerializableRunnable() {
      public void run() {
        disconnectFromDS();
      }
    });

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    // 3) verify that another member recovers for grantor
    int attempts = 3;
    for (int attempt = 0; attempt < attempts; attempt++) {
      try {
        for (int vm = 0; vm < numVMs; vm++) {
          if (vm == originalVM)
            continue; // skip because he's disconnected
          final int finalvm = vm;
          logInfo("[testBasicGrantorRecovery] VM " + finalvm + " in " + serviceName
              + " about to invoke");
          InternalDistributedMember id = VM.getVM(finalvm)
              .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(serviceName));
          logInfo("[testBasicGrantorRecovery] VM " + finalvm + " in " + serviceName + " got " + id);
          assertGrantorIsConsistent(id);
          logInfo(
              "[testBasicGrantorRecovery] new grantor " + id + " is not old grantor " + oldGrantor);
          assertNotEquals("New grantor must not equal the old grantor", id, oldGrantor);
        } // loop thru vms
        logInfo("[testBasicGrantorRecovery] succeeded attempt " + attempt);
        break; // success
      } catch (AssertionError e) {
        logInfo("[testBasicGrantorRecovery] failed attempt " + attempt);
        if (attempt == attempts - 1)
          throw e;
      }
    } // loop thru attempts
  }

  @Test
  public void testLockFailover() {
    final int originalGrantorVM = 0;
    final int oneVM = 1;
    final int twoVM = 2;
    final String serviceName = "testLockFailover-" + getUniqueName();

    // create lock services...
    logger.debug("[testLockFailover] create services");

    VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.remoteCreateService(serviceName));

    VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(serviceName));

    Boolean isGrantor = VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
    assertEquals("First member calling getLockGrantor failed to become grantor", Boolean.TRUE,
        isGrantor);

    // get locks...
    logger.debug("[testLockFailover] get lock");

    Boolean locked = VM.getVM(originalGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + originalGrantorVM));
    assertEquals("Failed to get lock in testLockFailover", Boolean.TRUE, locked);

    locked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + twoVM));
    assertEquals("Failed to get lock in testLockFailover", Boolean.TRUE, locked);

    locked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + oneVM));
    assertEquals("Failed to get lock in testLockFailover", Boolean.TRUE, locked);

    // disconnect originalGrantorVM...
    logger.debug("[testLockFailover] disconnect originalGrantorVM");

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
    logger.debug("[testLockFailover] release locks");

    Boolean unlocked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + twoVM));
    assertEquals("Failed to release lock in testLockFailover", Boolean.TRUE, unlocked);

    unlocked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + oneVM));
    assertEquals("Failed to release lock in testLockFailover", Boolean.TRUE, unlocked);

    // switch locks...
    locked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + twoVM));
    assertEquals("Failed to get lock in testLockFailover", Boolean.TRUE, locked);

    locked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + oneVM));
    assertEquals("Failed to get lock in testLockFailover", Boolean.TRUE, locked);

    unlocked = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + twoVM));
    assertEquals("Failed to release lock in testLockFailover", Boolean.TRUE, unlocked);

    unlocked = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + oneVM));
    assertEquals("Failed to release lock in testLockFailover", Boolean.TRUE, unlocked);

    // verify grantor is unique...
    logger.debug("[testLockFailover] verify grantor identity");

    InternalDistributedMember oneID = VM.getVM(oneVM)
        .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(serviceName));
    InternalDistributedMember twoID = VM.getVM(twoVM)
        .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(serviceName));
    assertTrue("Failed to identifyLockGrantor in testLockFailover", oneID != null && twoID != null);
    assertEquals("Failed grantor uniqueness in testLockFailover", oneID, twoID);
  }

  @Test
  public void testLockThenBecomeLockGrantor() {
    final int originalGrantorVM = 0;
    final int becomeGrantorVM = 1;
    final int thirdPartyVM = 2;
    final String serviceName = "testLockThenBecomeLockGrantor-" + getUniqueName();

    // create lock services...
    logger.debug("[testLockThenBecomeLockGrantor] create services");

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
        .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(serviceName));

    Boolean isGrantor = VM.getVM(originalGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
    assertEquals("First member calling getLockGrantor failed to become grantor", Boolean.TRUE,
        isGrantor);

    // control...
    logger.debug("[testLockThenBecomeLockGrantor] check control");
    Boolean check = VM.getVM(becomeGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + becomeGrantorVM));
    assertEquals("Check of control failed... unlock succeeded but nothing locked", Boolean.FALSE,
        check);

    // get locks...
    logger.debug("[testLockThenBecomeLockGrantor] get lock");

    Boolean locked = VM.getVM(originalGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + originalGrantorVM));
    assertEquals("Failed to get lock in testLockThenBecomeLockGrantor", Boolean.TRUE, locked);

    locked = VM.getVM(thirdPartyVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + thirdPartyVM));
    assertEquals("Failed to get lock in testLockThenBecomeLockGrantor", Boolean.TRUE, locked);

    locked = VM.getVM(becomeGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "KEY-" + becomeGrantorVM));
    assertEquals("Failed to get lock in testLockThenBecomeLockGrantor", Boolean.TRUE, locked);

    // become lock grantor...
    logger.debug("[testLockThenBecomeLockGrantor] become lock grantor");

    VM.getVM(becomeGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.becomeLockGrantor(serviceName));

    try {
      Thread.sleep(20);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    isGrantor = VM.getVM(becomeGrantorVM)
        .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
    assertEquals("Failed to become lock grantor", Boolean.TRUE, isGrantor);

    // verify locks by unlocking...
    logger.debug("[testLockThenBecomeLockGrantor] release locks");

    Boolean unlocked = VM.getVM(originalGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + originalGrantorVM));
    assertEquals("Failed to release lock in testLockThenBecomeLockGrantor", Boolean.TRUE, unlocked);

    unlocked = VM.getVM(thirdPartyVM)
        .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + thirdPartyVM));
    assertEquals("Failed to release lock in testLockThenBecomeLockGrantor", Boolean.TRUE, unlocked);

    unlocked = VM.getVM(becomeGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + becomeGrantorVM));
    assertEquals("Failed to release lock in testLockThenBecomeLockGrantor", Boolean.TRUE, unlocked);

    // test for bug in which transferred token gets re-entered causing lock recursion
    unlocked = VM.getVM(becomeGrantorVM).invoke(
        () -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY-" + becomeGrantorVM));
    assertEquals("Transfer of tokens caused lock recursion in held lock", Boolean.FALSE, unlocked);
  }

  @Test
  public void testBecomeLockGrantor() {
    // create lock services...
    int numVMs = 4;
    final String serviceName = "testBecomeLockGrantor-" + getUniqueName();
    distributedCreateService(numVMs, serviceName);

    // each one gets a lock...
    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      Boolean locked = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.lock(serviceName, "obj-" + finalvm));
      assertEquals("Failed to get lock in testBecomeLockGrantor", Boolean.TRUE, locked);
    }

    // find the grantor...
    int originalVM = -1;
    InternalDistributedMember oldGrantor = null;
    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      Boolean isGrantor = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
      if (isGrantor) {
        originalVM = vm;
        oldGrantor = VM.getVM(finalvm)
            .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(
                serviceName));
        break;
      }
    }

    logger.debug("[testBecomeLockGrantor] original grantor is " + oldGrantor);

    // have one call becomeLockGrantor
    for (int vm = 0; vm < numVMs; vm++) {
      if (vm != originalVM) {
        final int finalvm = vm;
        VM.getVM(finalvm)
            .invoke(() -> DistributedLockServiceDUnitTest.becomeLockGrantor(serviceName));
        Boolean isGrantor = VM.getVM(finalvm)
            .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
        assertEquals("isLockGrantor is false after calling becomeLockGrantor", Boolean.TRUE,
            isGrantor);
        break;
      }
    }

    logger.debug("[testBecomeLockGrantor] one vm has called becomeLockGrantor...");

    InternalDistributedMember newGrantor = null;
    for (int vm = 0; vm < numVMs; vm++) {
      Boolean isGrantor = VM.getVM(vm)
          .invoke(() -> DistributedLockServiceDUnitTest.isLockGrantor(serviceName));
      if (isGrantor) {
        newGrantor = VM.getVM(vm)
            .invoke(() -> DistributedLockServiceDUnitTest.identifyLockGrantor(
                serviceName));
        break;
      }
    }
    logger.debug("[testBecomeLockGrantor] new Grantor is " + newGrantor);
    assertEquals(false, newGrantor.equals(oldGrantor));

    // verify locks still held by unlocking
    // each one unlocks...
    for (int vm = 0; vm < numVMs; vm++) {
      final int finalvm = vm;
      Boolean unlocked = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "obj-" + finalvm));
      assertEquals("Failed to unlock in testBecomeLockGrantor", Boolean.TRUE, unlocked);
    }

    logger.debug("[testBecomeLockGrantor] finished");

    // verify that pending requests are granted by unlocking them also
  }

  @Test
  public void testTryLock() {
    final Long waitMillis = 100L;

    // create lock services...
    logger.debug("[testTryLock] create lock services");
    final String serviceName = "testTryLock-" + getUniqueName();
    distributedCreateService(4, serviceName);

    // all 4 vms scramble to get tryLock but only one should succeed...
    logger.debug("[testTryLock] attempt to get tryLock");
    int lockCount = 0;
    for (int vm = 0; vm < 4; vm++) {
      final int finalvm = vm;
      Boolean locked = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.tryLock(serviceName, "KEY", waitMillis));
      if (locked)
        lockCount++;
    }

    assertEquals("More than one vm acquired the tryLock", 1, lockCount);

    logger.debug("[testTryLock] unlock tryLock");
    int unlockCount = 0;
    for (int vm = 0; vm < 4; vm++) {
      final int finalvm = vm;
      Boolean unlocked = VM.getVM(finalvm)
          .invoke(() -> DistributedLockServiceDUnitTest.unlock(serviceName, "KEY"));
      if (unlocked)
        unlockCount++;
    }

    assertEquals("More than one vm unlocked the tryLock", 1, unlockCount);
  }

  @Test
  public void testOneGetsThenOtherGets() throws Exception { // (numVMs, numThreadsPerVM)
    doOneGetsThenOtherGets(1, 1);
    // doOneGetsThenOtherGets(2, 2);
    // doOneGetsThenOtherGets(3, 3);
    doOneGetsThenOtherGets(4, 3);
  }

  @Test
  public void testLockDifferentNames() throws Exception {
    String serviceName = getUniqueName();

    // Same VM
    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    assertTrue(service.lock("obj1", -1, -1));
    assertTrue(service.lock("obj2", -1, -1));
    service.unlock("obj1");
    service.unlock("obj2");

    // Different VMs
    VM vm = VM.getVM(0);
    vm.invoke(() -> this.remoteCreateService(serviceName));
    assertTrue(service.lock("masterVMobj", -1, -1));

    assertEquals(Boolean.TRUE, vm.invoke(() -> this.getLockAndIncrement(serviceName, "otherVMobj",
        -1, 0)));

    service.unlock("masterVMobj");
  }

  @Test
  public void testLocalGetLockAndIncrement() throws Exception {
    String serviceName = getUniqueName();
    remoteCreateService(serviceName);
    DistributedLockService.getServiceNamed(serviceName);
    assertEquals(Boolean.TRUE, getLockAndIncrement(serviceName, "localVMobj", -1, 0));
  }

  @Test
  public void testRemoteGetLockAndIncrement() {
    String serviceName = getUniqueName();
    VM vm = VM.getVM(0);
    vm.invoke(() -> this.remoteCreateService(serviceName));
    assertEquals(Boolean.TRUE, vm.invoke(() -> this.getLockAndIncrement(serviceName, "remoteVMobj",
        -1, 0)));
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
    assertTrue(service1.lock(objName, -1, -1));
    assertTrue(service2.lock(objName, -1, -1));
    service1.unlock(objName);
    service2.unlock(objName);

    // Different VMs
    VM vm = VM.getVM(0);
    vm.invoke(() -> this.remoteCreateService(serviceName1));
    vm.invoke(() -> this.remoteCreateService(serviceName2));
    assertTrue(service1.lock(objName, -1, -1));
    assertEquals(Boolean.TRUE, vm.invoke(
        () -> this.getLockAndIncrement(serviceName2, objName, -1, 0)));
    service1.unlock(objName);
  }

  @Test
  public void testLeaseDoesntExpire() throws InterruptedException {
    String serviceName = getUniqueName();
    final Object objName = 3;

    // Same VM
    remoteCreateService(serviceName);
    final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    // lock objName with a sufficiently long lease
    assertTrue(service.lock(objName, -1, 60000));
    // try to lock in another thread, with a timeout shorter than above lease
    final boolean[] resultHolder = new boolean[] {false};
    Thread thread = new Thread(new Runnable() {
      public void run() {
        resultHolder[0] = !service.lock(objName, 1000, -1);
      }
    });
    thread.start();
    ThreadUtils.join(thread, 30 * 1000);
    assertTrue(resultHolder[0]);
    // the unlock should succeed without throwing LeaseExpiredException
    service.unlock(objName);

    // Different VM
    VM vm = VM.getVM(0);
    vm.invoke(() -> this.remoteCreateService(serviceName));
    // lock objName in this VM with a sufficiently long lease
    assertTrue(service.lock(objName, -1, 60000));
    // try to lock in another VM, with a timeout shorter than above lease
    assertEquals(Boolean.FALSE, vm
        .invoke(() -> this.getLockAndIncrement(serviceName, objName, 1000L, 0L)));
    // the unlock should succeed without throwing LeaseExpiredException
    service.unlock(objName);
  }

  @Test
  public void testLockUnlock() {
    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertTrue(!service.isHeldByCurrentThread(objName));

    service.lock(objName, -1, -1);
    assertTrue(service.isHeldByCurrentThread(objName));

    service.unlock(objName);
    assertTrue(!service.isHeldByCurrentThread(objName));
  }

  @Test
  public void testLockExpireUnlock() throws Exception {
    long leaseMs = 200;
    long waitBeforeLockingMs = 210;

    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertTrue(!service.isHeldByCurrentThread(objName));

    assertTrue(service.lock(objName, -1, leaseMs));
    assertTrue(service.isHeldByCurrentThread(objName));

    Thread.sleep(waitBeforeLockingMs); // should expire...
    assertTrue(!service.isHeldByCurrentThread(objName));

    try {
      service.unlock(objName);
      fail("unlock should have thrown LeaseExpiredException");
    } catch (LeaseExpiredException ex) {
    }
  }

  @Test
  public void testLockRecursion() {
    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertTrue(!service.isHeldByCurrentThread(objName));

    // initial lock...
    assertTrue(service.lock(objName, -1, -1));
    assertTrue(service.isHeldByCurrentThread(objName));

    // recursion +1...
    assertTrue(service.lock(objName, -1, -1));

    // recursion -1...
    service.unlock(objName);
    assertTrue(service.isHeldByCurrentThread(objName));

    // and unlock...
    service.unlock(objName);
    assertTrue(!service.isHeldByCurrentThread(objName));
  }

  @Test
  public void testLockRecursionWithExpiration() throws Exception {
    long leaseMs = 500;
    long waitBeforeLockingMs = 750;

    String serviceName = getUniqueName();
    Object objName = 42;

    remoteCreateService(serviceName);
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    assertTrue(!service.isHeldByCurrentThread(objName));

    // initial lock...
    assertTrue(service.lock(objName, -1, leaseMs));
    assertTrue(service.isHeldByCurrentThread(objName));

    // recursion +1...
    assertTrue(service.lock(objName, -1, leaseMs));
    assertTrue(service.isHeldByCurrentThread(objName));

    // expire...
    Thread.sleep(waitBeforeLockingMs);
    assertTrue(!service.isHeldByCurrentThread(objName));

    // should fail...
    try {
      service.unlock(objName);
      fail("unlock should have thrown LeaseExpiredException");
    } catch (LeaseExpiredException ex) {
    }

    // relock it...
    assertTrue(service.lock(objName, -1, leaseMs));
    assertTrue(service.isHeldByCurrentThread(objName));

    // and unlock to verify no recursion...
    service.unlock(objName);
    assertTrue(!service.isHeldByCurrentThread(objName)); // throws failure!!

    // go thru again in different order...
    assertTrue(!service.isHeldByCurrentThread(objName));

    // initial lock...
    assertTrue(service.lock(objName, -1, leaseMs));
    assertTrue(service.isHeldByCurrentThread(objName));

    // expire...
    Thread.sleep(waitBeforeLockingMs);
    assertTrue(!service.isHeldByCurrentThread(objName));

    // relock it...
    assertTrue(service.lock(objName, -1, leaseMs));
    assertTrue(service.isHeldByCurrentThread(objName));

    // and unlock to verify no recursion...
    service.unlock(objName);
    assertTrue(!service.isHeldByCurrentThread(objName));
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
    logger.debug("[testLeaseExpires] prepping");
    long leaseMs = 100;
    long waitBeforeLockingMs = tryToLockBeforeExpiration ? 50 : 110;

    final String serviceName = getUniqueName();
    final Object objName = 3;

    // Same VM
    remoteCreateService(serviceName);
    final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);

    logger.debug("[testLeaseExpires] acquire first lock");
    // lock objName with a short lease
    assertTrue(service.lock(objName, -1, leaseMs));
    Thread.sleep(waitBeforeLockingMs);

    if (waitBeforeLockingMs > leaseMs) {
      assertTrue(!service.isHeldByCurrentThread(objName));
    }

    logger.debug("[testLeaseExpires] acquire lock that expired");
    // try to lock in another thread - lease should have expired
    final boolean[] resultHolder = new boolean[] {false};
    Thread thread = new Thread(new Runnable() {
      public void run() {
        resultHolder[0] = service.lock(objName, -1, -1);
        service.unlock(objName);
        assertTrue(!service.isHeldByCurrentThread(objName));
      }
    });
    thread.start();
    ThreadUtils.join(thread, 30 * 1000);
    assertTrue(resultHolder[0]);

    logger.debug("[testLeaseExpires] unlock should throw LeaseExpiredException");
    // this thread's unlock should throw LeaseExpiredException
    try {
      service.unlock(objName);
      fail("unlock should have thrown LeaseExpiredException");
    } catch (LeaseExpiredException ex) {
    }

    logger.debug("[testLeaseExpires] create service in other vm");
    // Different VM
    VM vm = VM.getVM(0);
    vm.invoke(() -> this.remoteCreateService(serviceName));

    logger.debug("[testLeaseExpires] acquire lock again and expire");
    // lock objName in this VM with a short lease
    assertTrue(service.lock(objName, -1, leaseMs));
    Thread.sleep(waitBeforeLockingMs);

    logger.debug("[testLeaseExpires] succeed lock in other vm");
    // try to lock in another VM - should succeed
    assertEquals(Boolean.TRUE,
        vm.invoke(() -> this.getLockAndIncrement(serviceName, objName, (long) -1, 0L)));

    logger.debug("[testLeaseExpires] unlock should throw LeaseExpiredException again");
    // this VMs unlock should throw LeaseExpiredException
    try {
      service.unlock(objName);
      fail("unlock should have thrown LeaseExpiredException");
    } catch (LeaseExpiredException ex) {
    }
  }

  @Test
  public void testSuspendLockingAfterExpiration() throws Exception {
    logger.debug("[leaseExpiresThenSuspendTest]");

    final long leaseMillis = 100;
    final long suspendWaitMillis = 10000;

    final String serviceName = getUniqueName();
    final Object key = 3;

    // controller locks key and then expires - controller is grantor

    DistributedLockService dls = DistributedLockService.create(serviceName, getSystem());

    assertTrue(dls.lock(key, -1, leaseMillis));

    // wait for expiration
    Thread.sleep(leaseMillis * 2);

    logger.debug("[leaseExpiresThenSuspendTest] unlock should throw LeaseExpiredException");
    // this thread's unlock should throw LeaseExpiredException
    try {
      dls.unlock(key);
      fail("unlock should have thrown LeaseExpiredException");
    } catch (LeaseExpiredException ex) {
    }

    // other vm calls suspend

    logger.debug("[leaseExpiresThenSuspendTest] call to suspend locking");
    VM.getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        final DistributedLockService dlock =
            DistributedLockService.create(serviceName, getSystem());
        dlock.suspendLocking(suspendWaitMillis);
        dlock.resumeLocking();
        assertTrue(dlock.lock(key, -1, leaseMillis));
        dlock.unlock(key);
      }
    });
  }

  volatile boolean started = false;
  volatile boolean gotLock = false;
  volatile Throwable exception = null;
  volatile Throwable throwable = null;

  @Test
  public void testLockInterruptiblyIsInterruptible() {
    started = false;
    gotLock = false;
    exception = null;
    throwable = null;

    // Lock entire service in first thread
    logger
        .info("[testLockInterruptiblyIsInterruptible] get and hold the lock");
    final String serviceName = getUniqueName();
    final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    service.becomeLockGrantor();
    assertTrue(service.lock("obj", 1000, -1));

    // Start second thread that tries to lock in second thread
    logger
        .info("[testLockInterruptiblyIsInterruptible] call lockInterruptibly");
    Thread thread2 = new Thread(new Runnable() {
      public void run() {
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
      }
    });
    thread2.start();

    // Interrupt second thread
    logger
        .info("[testLockInterruptiblyIsInterruptible] interrupt calling thread");
    while (!started)
      Thread.yield();
    thread2.interrupt();
    ThreadUtils.join(thread2, 20 * 1000);

    // Expect it got InterruptedException and didn't lock the service
    logger
        .info("[testLockInterruptiblyIsInterruptible] verify failed to get lock");
    assertFalse(gotLock);
    if (throwable != null) {
      logger.warn("testLockInterruptiblyIsInterruptible threw unexpected Throwable", throwable);
    }
    assertNotNull(exception);

    // Unlock "obj" in first thread
    logger.info("[testLockInterruptiblyIsInterruptible] unlock the lock");
    service.unlock("obj");

    // Make sure it didn't get locked by second thread
    logger.info(
        "[testLockInterruptiblyIsInterruptible] try to get lock with timeout should not fail");
    assertTrue(service.lock("obj", 5000, -1));
    DistributedLockService.destroy(serviceName);
  }

  volatile boolean wasFlagSet = false;

  @Test
  public void testLockIsNotInterruptible() throws Exception {
    // Lock entire service in first thread
    logger.debug("[testLockIsNotInterruptible] lock in first thread");
    started = false;
    gotLock = false;
    exception = null;
    wasFlagSet = false;

    final String serviceName = getUniqueName();
    final DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    assertTrue(service.lock("obj", 1000, -1));

    // Start second thread that tries to lock in second thread
    logger.debug("[testLockIsNotInterruptible] attempt lock in second thread");
    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        try {
          started = true;
          gotLock = service.lock("obj", -1, -1);
          logger.debug("[testLockIsNotInterruptible] thread2 finished lock() - got " + gotLock);
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
    logger.debug("[testLockIsNotInterruptible] interrupt second thread");
    while (!started)
      Thread.yield();
    Thread.sleep(500);
    thread2.interrupt();
    // Expect it didn't get an exception and didn't lock the service
    Thread.sleep(500);
    assertFalse(gotLock);
    assertNull(exception);

    // Unlock entire service in first thread
    logger.debug("[testLockIsNotInterruptible] unlock in first thread");
    service.unlock("obj");
    Thread.sleep(500);

    // Expect that thread2 should now complete execution.
    ThreadUtils.join(thread2, 20 * 1000);

    // Now thread2 should have gotten the lock, not the exception, but the
    // thread's flag should be set
    logger.debug("[testLockIsNotInterruptible] verify second thread got lock");
    assertNull(exception);
    assertTrue(gotLock);
    assertTrue(wasFlagSet);
  }

  /**
   * Test DistributedLockService.acquireExclusiveLocking(), releaseExclusiveLocking()
   */
  @Test
  public void testSuspendLockingBasic() throws Exception {
    final DistributedLockService service =
        DistributedLockService.create(getUniqueName(), dlstSystem);

    try {
      service.resumeLocking();
      fail("Didn't throw LockNotHeldException");
    } catch (LockNotHeldException ex) {
      // expected
    }

    assertTrue(service.suspendLocking(-1));
    service.resumeLocking();

    // It's not reentrant
    assertTrue(service.suspendLocking(1000));
    try {
      service.suspendLocking(1);
      fail("didn't get IllegalStateException");
    } catch (IllegalStateException ex) {
      // expected
    }
    service.resumeLocking();

    // Get "false" if another thread is holding it
    Thread thread = new Thread(() -> {
      logInfo("new thread about to suspendLocking()");
      assertTrue(service.suspendLocking(1000));
    });

    thread.start();
    ThreadUtils.join(thread, 30 * 1000);
    logInfo("main thread about to suspendLocking");
    assertTrue(!service.suspendLocking(1000));
  }

  /**
   * Test that exlusive locking prohibits locking activity
   */
  @Test
  public void testSuspendLockingProhibitsLocking() {
    final String name = getUniqueName();
    distributedCreateService(2, name);
    DistributedLockService service = DistributedLockService.getServiceNamed(name);

    // Should be able to lock from other VM
    VM vm1 = VM.getVM(1);
    assertTrue(vm1.invoke(() -> DistributedLockServiceDUnitTest.tryToLock(name)));

    assertTrue(service.suspendLocking(1000));

    // vm1 is the grantor... use debugHandleSuspendTimeouts
    vm1.invoke(new SerializableRunnable("setDebugHandleSuspendTimeouts") {
      public void run() {
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(name);
        assertTrue(dls.isLockGrantor());
        DLockGrantor grantor = dls.getGrantorWithNoSync();
        grantor.setDebugHandleSuspendTimeouts(5000);
      }
    });

    // Shouldn't be able to lock a name from another VM
    assertTrue(!vm1.invoke(() -> DistributedLockServiceDUnitTest.tryToLock(name)));

    service.resumeLocking();

    vm1.invoke(new SerializableRunnable("unsetDebugHandleSuspendTimeouts") {
      public void run() {
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(name);
        assertTrue(dls.isLockGrantor());
        DLockGrantor grantor = dls.getGrantorWithNoSync();
        grantor.setDebugHandleSuspendTimeouts(0);
      }
    });

    // Should be able to lock again
    assertTrue(vm1.invoke(() -> DistributedLockServiceDUnitTest.tryToLock(name)));

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

  private void doTestSuspendLockingBehaves() throws Exception {
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
        assertFalse(isLockGrantor(dlsName));
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
        assertTrue(isLockGrantor(dlsName));
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
    assertTrue(vmTwoLocking.isAlive());
    Wait.pause(100);
    assertTrue(vmThreeLocking.isAlive());

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
    assertTrue(vmOneSuspending.isAlive());
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
    assertTrue(vmTwoSuspending.isAlive());
    vmOne.invoke(resumeLocking);
    ThreadUtils.join(vmTwoSuspending, 10 * 1000);

    // let vmOne get that lock
    logger.info("[testSuspendLockingBehaves] let vmOne get that lock");
    Wait.pause(100);
    assertTrue(vmOneLockingAgain.isAlive());
    vmTwo.invoke(resumeLocking);
    ThreadUtils.join(vmOneLockingAgain, 10 * 1000);

    // let vmThree suspend locking
    logger.info("[testSuspendLockingBehaves] let vmThree suspend locking");
    Wait.pause(100);
    assertTrue(vmThreeSuspending.isAlive());
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
    distributedCreateService(2, name);
    final DistributedLockService service = DistributedLockService.getServiceNamed(name);

    // Get lock from other VM. Since same thread needs to lock and unlock,
    // invoke asynchronously, get lock, wait to be notified, then unlock.
    VM vm1 = VM.getVM(1);
    vm1.invokeAsync(new SerializableRunnable("Lock & unlock in vm1") {
      public void run() {
        DistributedLockService service2 = DistributedLockService.getServiceNamed(name);
        assertTrue(service2.lock("lock", -1, -1));
        synchronized (monitor) {
          try {
            monitor.wait();
          } catch (InterruptedException ex) {
            System.out.println("Unexpected InterruptedException");
            fail("interrupted");
          }
        }
        service2.unlock("lock");
      }
    });
    // Let vm1's thread get the lock and go into wait()
    Thread.sleep(100);

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
    Thread.sleep(100);
    assertFalse("Before release, got: " + getGot() + ", done: " + getDone(), getGot() || getDone());

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
    Wait.waitForCriterion(ev, 30 * 1000, 200, true);
    if (!getGot() || !getDone()) {
      ThreadUtils.dumpAllStacks();
    }
    assertTrue("After release, got: " + getGot() + ", done: " + getDone(), getGot() && getDone());

  }

  @Test
  public void testSuspendLockingInterruptiblyIsInterruptible() throws Exception {

    started = false;
    gotLock = false;
    exception = null;

    // Lock entire service in first thread
    final String name = getUniqueName();
    final DistributedLockService service = DistributedLockService.create(name, dlstSystem);
    assertTrue(service.suspendLocking(1000));

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
    assertFalse(gotLock);
    assertNotNull(exception);

    // Unlock entire service in first thread
    service.resumeLocking();
    Thread.sleep(500);

    // Make sure it didn't get locked by second thread
    assertTrue(service.suspendLocking(1000));
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
    assertTrue(service.suspendLocking(1000));

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
    assertFalse(gotLock);
    assertNull(exception);

    // Unlock entire service in first thread
    service.resumeLocking();
    ThreadUtils.join(thread2, 20 * 1000);

    // Now thread2 should have gotten the lock, not the exception, but the
    // thread's flag should be set
    logger.info("[testSuspendLockingIsNotInterruptible]" + " gotLock="
        + gotLock + " wasFlagSet=" + wasFlagSet + " exception=" + exception, exception);
    assertTrue(gotLock);
    assertNull(exception);
    assertTrue(wasFlagSet);
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
    assertTrue(service.lock("key", -1, -1));
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
    assertTrue(service.lock("key", -1, -1));

  }

  @Test
  public void testDepartedLastOwnerNoLease() {
    final String serviceName = this.getUniqueName();

    // Create service in this VM
    DistributedLockService service = DistributedLockService.create(serviceName, dlstSystem);
    assertTrue(service.lock("key", -1, -1));
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
    assertTrue(service.lock("key", -1, -1));

  }

  /**
   * Tests for 32461 R3 StuckLocks can occur on locks with an expiration lease
   * <p>
   * VM-A locks/unlocks "lock", VM-B leases "lock" and disconnects, VM-C attempts to lock "lock" and
   * old dlock throws StuckLockException. VM-C should now succeed in acquiring the lock.
   */
  @Test
  public void testBug32461() throws Exception {
    logger.debug("[testBug32461] prepping");

    final String serviceName = getUniqueName();
    final Object objName = "32461";
    final int VM_A = 0;
    final int VM_B = 1;
    final int VM_C = 2;

    // VM-A locks/unlocks "lock"...
    logger.debug("[testBug32461] VM-A locks/unlocks '32461'");

    VM.getVM(VM_A).invoke(new SerializableRunnable() {
      public void run() {
        remoteCreateService(serviceName);
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
        assertTrue(service.lock(objName, -1, Long.MAX_VALUE));
        service.unlock(objName);
      }
    });

    // VM-B leases "lock" and disconnects,
    logger.debug("[testBug32461] VM_B leases '32461' and disconnects");

    VM.getVM(VM_B).invoke(new SerializableRunnable() {
      public void run() {
        remoteCreateService(serviceName);
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
        assertTrue(service.lock(objName, -1, Long.MAX_VALUE));
        DistributedLockService.destroy(serviceName);
        disconnectFromDS();
      }
    });

    logger.debug("[testBug32461] VM_C attempts to lock '32461'");

    VM.getVM(VM_C).invoke(new SerializableRunnable() {
      public void run() {
        remoteCreateService(serviceName);
        final DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
        assertTrue(service.lock(objName, -1, -1));
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

    assertTrue(service.lock(keyWithLease, -1, -1));
    service.unlock(keyWithLease);

    assertTrue(service.lock(keyNoLease, -1, -1));
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
    assertTrue(service.lock(keyWithLease, -1, -1));
    service.unlock(keyWithLease);
    assertTrue(service.lock(keyNoLease, -1, -1));
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
          assertTrue(service.lock("obj", -1, -1));
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
            assertFalse(service.lockInterruptibly("obj", -1, -1));
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
      assertTrue(service.lock("obj", 1000, -1));
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
      assertTrue(service.lock("obj", -1, -1));
      service.unlock("obj");

      // thread to get lock and wait and then unlock
      vm1.invokeAsync(new SerializableRunnable() {
        public void run() {
          logger.info("[testReleaseOrphanedGrant_Remote] get the lock");
          threadVM1_testReleaseOrphanedGrant_Remote = Thread.currentThread();
          connectDistributedSystem();
          DistributedLockService service_vm1 =
              DistributedLockService.create(serviceName, getSystem());
          assertTrue(service_vm1.lock("obj", -1, -1));
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
            assertFalse(service_vm2.lockInterruptibly("obj", -1, -1));
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
            assertNull(DistributedLockService.getServiceNamed(serviceName));
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
      assertTrue(service.lock("obj", 1000, -1));
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
  public void testDestroyLockServiceAfterGrantResponse() throws Throwable {
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
        assertTrue(service.lock("obj", -1, -1));
        service.unlock("obj");
      }
    });
  }

  @Test
  public void testDestroyLockServiceBeforeGrantRequest() throws Throwable {
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
        assertTrue(service.lock("obj", -1, -1));
        service.unlock("obj");
      }
    });
  }

  private static void createLockGrantor(String serviceName) {
    connectDistributedSystem();
    final DistributedLockService service =
        DistributedLockService.create(serviceName, dlstSystem);

    // lock and unlock to make sure this vm is grantor
    // FIXME GALEN that statement is incorrect
    assertTrue(service.lock("obj", -1, -1));
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

  /**
   * Accessed via reflection. DO NOt REMOVE
   */
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

    logInfo("Starting testtt " + serviceName);

    distributedCreateService(numVMs, serviceName);
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
          logInfo("VM " + finalvm + ", thread " + finalthread + " in " + serviceName
              + " about to invoke");
          Boolean result = VM.getVM(finalvm)
              .invoke(() -> DistributedLockServiceDUnitTest.getLockAndIncrement(
                  serviceName, objectName, timeout, holdTime));
          logInfo("VM " + finalvm + ", thread " + finalthread + " in " + serviceName + " got "
              + result);
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
    assertEquals("number of VMs that got ownership is wrong", 1, hits);

    // assert that all the others timed out
    assertEquals("number of threads that completed is wrong", numVMs * numThreadsPerVM, completes);

    // Check final value of entry
    long count = blackboard.getCount();
    assertEquals("Final entry value wrong", 1, count);

    logInfo("Done testtt " + serviceName);
  }

  // 2, 2... expect 4, but get 3
  private void doOneGetsThenOtherGets(int numVMs, int numThreadsPerVM) throws Exception {

    final String serviceName = getUniqueName() + "-" + numVMs + "-" + numThreadsPerVM;
    final String objectName = "obj";

    logInfo("Starting testtt " + serviceName);

    distributedCreateService(numVMs, serviceName);
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

          logInfo("VM " + finalvm + ", thread " + finalthread + " in " + serviceName
              + " about to invoke");

          Boolean result = null;
          try {
            result = VM.getVM(finalvm)
                .invoke(() -> DistributedLockServiceDUnitTest.getLockAndIncrement(
                    serviceName, objectName, timeout, 0L));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          logInfo("VM " + finalvm + ", thread " + finalthread + " in " + serviceName
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
        logInfo("Test serviceName timed out");
        break;
      }
      Thread.sleep(200);
    }

    // assert that all completed
    assertEquals("number of threads that completed is wrong", numVMs * numThreadsPerVM, completes);
    // -------------------------------------------------------

    // assert that all were able to lock
    if (hits != numVMs * numThreadsPerVM) {
      ThreadUtils.dumpAllStacks();
    }
    assertEquals("number of VMs that got ownership is wrong", numVMs * numThreadsPerVM, hits);

    // Check final value of entry
    long count = blackboard.getCount();
    assertEquals("Blackboard.getCount() wrong", numVMs * numThreadsPerVM, count);

    logInfo("Done testtt " + serviceName);

  }

  @Test
  public void testTokenCleanup() throws Exception {
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

        assertTrue(dls.lock(key1, -1, -1));
        assertTrue(dls.isLockGrantor());
        assertNotNull(dls.getToken(key1));

        dls.unlock(key1);
        assertNotNull(dls.getToken(key1));

        // token should be removed when freeResources is called
        dls.freeResources(key1);
        // assertNull(dls.getToken(key1));

        DLockToken token = dls.getToken(key1);
        assertNull("Failed with bug 38180: " + token, token);

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertEquals("Failed with bug 38180: tokens=" + tokens, 0, tokens.size());
      }
    });

    // vm1 locks and frees key1
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vm1 locks key1");
        connectDistributedSystem();
        DLockService dls = (DLockService) DistributedLockService.create(dlsName, getSystem());

        assertTrue(dls.lock(key1, -1, -1));
        assertFalse(dls.isLockGrantor());
        assertNotNull(dls.getToken(key1));

        dls.unlock(key1);
        assertNotNull(dls.getToken(key1));

        dls.freeResources(key1);
        // assertNull(dls.getToken(key1));

        DLockToken token = dls.getToken(key1);
        assertNull("Failed with bug 38180: " + token, token);

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertEquals("Failed with bug 38180: tokens=" + tokens, 0, tokens.size());
      }
    });

    // vm1 tests recursion
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vm1 tests recursion");
        connectDistributedSystem();
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

        assertTrue(dls.lock(key1, -1, -1)); // 1
        assertEquals(1, dls.getToken(key1).getUsageCount());
        assertTrue(dls.lock(key1, -1, -1)); // 2
        assertEquals(2, dls.getToken(key1).getUsageCount());
        assertTrue(dls.lock(key1, -1, -1)); // 3
        assertEquals(3, dls.getToken(key1).getUsageCount());

        DLockToken token0 = dls.getToken(key1);
        assertNotNull(token0);
        Collection tokens = dls.getTokens();
        assertTrue(tokens.contains(token0));
        assertEquals(1, tokens.size());

        dls.unlock(key1); // 1
        assertEquals(2, dls.getToken(key1).getUsageCount());
        dls.freeResources(key1);

        DLockToken token1 = dls.getToken(key1);
        assertNotNull(token1);
        assertEquals(token0, token1);
        tokens = dls.getTokens();
        assertTrue(tokens.contains(token1));
        assertEquals(1, tokens.size());

        dls.unlock(key1); // 2
        assertEquals(1, dls.getToken(key1).getUsageCount());
        dls.freeResources(key1);
        assertNotNull(dls.getToken(key1));

        DLockToken token2 = dls.getToken(key1);
        assertNotNull(token2);
        assertEquals(token0, token2);
        tokens = dls.getTokens();
        assertTrue(tokens.contains(token2));
        assertEquals(1, tokens.size());

        dls.unlock(key1); // 3
        assertEquals(0, dls.getToken(key1).getUsageCount());
        dls.freeResources(key1);

        DLockToken token3 = dls.getToken(key1);
        assertNull("Failed with bug 38180: " + token3, token3);

        // make sure there are NO tokens at all
        tokens = dls.getTokens();
        assertEquals("Failed with bug 38180: tokens=" + tokens, 0, tokens.size());
      }
    });
  }

  @Test
  public void testGrantTokenCleanup() throws Exception {
    final String dlsName = getUniqueName();

    final VM vmGrantor = VM.getVM(0);
    final VM vm1 = VM.getVM(1);

    final String key1 = "key1";

    // vmGrantor creates grantor
    vmGrantor.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testGrantTokenCleanup] vmGrantor creates grantor");
        connectDistributedSystem();
        DistributedLockService dls = DistributedLockService.create(dlsName, getSystem());
        assertTrue(dls.lock(key1, -1, -1));
        assertTrue(dls.isLockGrantor());
        DLockGrantor grantor = ((DLockService) dls).getGrantor();
        assertNotNull(grantor);
        DLockGrantor.DLockGrantToken grantToken = grantor.getGrantToken(key1);
        assertNotNull(grantToken);
        logger.info("[testGrantTokenCleanup] vmGrantor unlocks key1");
        dls.unlock(key1);
        assertNull(grantor.getGrantToken(key1));
      }
    });

    if (true)
      return; // TODO: remove early-out and complete this test

    // vm1 locks and frees key1
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vm1 locks key1");
        connectDistributedSystem();
        DLockService dls = (DLockService) DistributedLockService.create(dlsName, getSystem());
        assertTrue(dls.lock(key1, -1, -1));

        logger.info("[testTokenCleanup] vm1 frees key1");
        dls.unlock(key1);

        // token for key1 still exists until freeResources is called
        assertNotNull(dls.getToken(key1));
        dls.freeResources(key1);

        // make sure token for key1 is gone
        DLockToken token = dls.getToken(key1);
        assertNull("Failed with bug 38180: " + token, token);

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertEquals("Failed with bug 38180: tokens=" + tokens, 0, tokens.size());
      }
    });

    // vmGrantor frees key1
    vmGrantor.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testTokenCleanup] vmGrantor frees key1");
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

        // NOTE: DLockToken and DLockGrantToken should have been removed when
        // vm1 unlocked key1

        if (true)
          return; // TODO: remove this when 38180/38179 are fixed

        // check for bug 38180...

        // make sure token for key1 is gone
        DLockToken token = dls.getToken(key1);
        assertNull("Failed with bug 38180: " + token, token);

        // make sure there are NO tokens at all
        Collection tokens = dls.getTokens();
        assertEquals("Failed with bug 38180: tokens=" + tokens, 0, tokens.size());

        // check for bug 38179...

        // make sure there are NO grant tokens at all
        DLockGrantor grantor = dls.getGrantor();
        Collection grantTokens = grantor.getGrantTokens();
        assertEquals("Failed with bug 38179: grantTokens=" + grantTokens, 0, grantTokens.size());

        // dls.freeResources(key1);
        // TODO: assert that DLockGrantToken for key1 is gone
        assertNull(dls.getToken(key1));
      }
    });
  }

  private static final AtomicBoolean testLockQuery_whileVM1Locks = new AtomicBoolean();

  @Test
  public void testLockQuery() throws Exception {
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

        assertTrue(dls.lock(key1, -1, -1));
        assertTrue(dls.isLockGrantor());
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

          assertTrue(dls.lock(key1, -1, -1));
          assertFalse(dls.isLockGrantor());

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
      assertNotNull(vm1Member);

      // vmGrantor tests positive local dlock query
      vmGrantor.invoke(new SerializableRunnable() {
        public void run() {
          logger.info("[testLockQuery] vmGrantor tests local query");
          DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

          DLockRemoteToken result = dls.queryLock(key1);
          assertNotNull(result);
          assertEquals(key1, result.getName());
          assertTrue(result.getLeaseId() != -1);
          assertEquals(Long.MAX_VALUE, result.getLeaseExpireTime());
          RemoteThread lesseeThread = result.getLesseeThread();
          assertNotNull(lesseeThread);
          assertEquals(vm1Member, lesseeThread.getDistributedMember());
          assertEquals(vm1Member, result.getLessee());
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
          assertNotNull(result);
          assertEquals(key1, result.getName());
          assertTrue(result.getLeaseId() != -1);
          assertEquals(Long.MAX_VALUE, result.getLeaseExpireTime());
          RemoteThread lesseeThread = result.getLesseeThread();
          assertNotNull(lesseeThread);
          assertEquals(vm1Member, lesseeThread.getDistributedMember());
          assertEquals(vm1Member, result.getLessee());
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
        assertNotNull(result);
        assertEquals(key1, result.getName());
        assertEquals(-1, result.getLeaseId());
        assertEquals(0, result.getLeaseExpireTime());
        assertNull(result.getLesseeThread());
        assertNull(result.getLessee());
      }
    });

    // vm2 tests negative remote dlock query
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        logger.info("[testLockQuery] vm2 tests negative query");
        DLockService dls = (DLockService) DistributedLockService.getServiceNamed(dlsName);

        DLockRemoteToken result = dls.queryLock(key1);
        assertNotNull(result);
        assertEquals(key1, result.getName());
        assertEquals(-1, result.getLeaseId());
        assertEquals(0, result.getLeaseExpireTime());
        assertNull(result.getLesseeThread());
        assertNull(result.getLessee());
      }
    });

  }

  ////////// Support methods

  private void distributedCreateService(int numVMs, String serviceName) {
    // create an entry - use scope DIST_ACK, not GLOBAL, since we're testing
    // that explicit use of the ownership api provides the synchronization
    forNumVMsInvoke(numVMs, () -> remoteCreateService(serviceName));

    remoteCreateService(serviceName);
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
    logInfo("Created " + newService);
  }

  private static Boolean getLockAndIncrement(String serviceName, Object objectName, long timeout,
      long holdTime) throws Exception {
    logInfo("[getLockAndIncrement] In getLockAndIncrement");
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    boolean got = service.lock(objectName, timeout, -1);
    logInfo("[getLockAndIncrement] In getLockAndIncrement - got is " + got);
    if (got) {
      // Make sure we don't think anyone else is holding the lock
      if (blackboard.getIsLocked()) {
        String msg = "obtained lock on " + serviceName + "/" + objectName
            + " but blackboard was locked, grantor=" + ((DLockService) service).getLockGrantorId()
            + ", isGrantor=" + service.isLockGrantor();
        logInfo("[getLockAndIncrement] In getLockAndIncrement: " + msg);
        fail(msg);
      }
      blackboard.setIsLocked(true);
      long count = blackboard.getCount();
      logInfo("[getLockAndIncrement] In getLockAndIncrement - count is " + count + " for "
          + serviceName + "/" + objectName);
      Thread.sleep(holdTime);
      blackboard.incCount();
      blackboard.setIsLocked(false);
      logInfo("[getLockAndIncrement] In getLockAndIncrement: " + "cleared blackboard lock for "
          + serviceName + "/" + objectName);
      service.unlock(objectName);
    }
    logInfo("[getLockAndIncrement] Returning from getLockAndIncrement");
    return got;
  }

  private synchronized void incHits() {
    hits = hits + 1;
  }

  private synchronized void incCompletes() {
    completes = completes + 1;
  }

  protected static void logInfo(String msg) {
    dlstSystem.getLogWriter().fine(msg);
  }

  /**
   * Assumes there is only one host, and invokes the given method in the first numVMs VMs that host
   * knows about.
   */
  private void forNumVMsInvoke(int numVMs, final SerializableRunnableIF runnable) {
    Host host = Host.getHost(0);
    for (int i = 0; i < numVMs; i++) {
      VM.getVM(i).invoke(runnable);
    }
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
                  assertTrue(DistributedLockService.getServiceNamed(dlsName).lock(key, -1, -1));
                  break;
                case 2:
                  logger.info("BasicLockClient unlock");
                  DistributedLockService.getServiceNamed(dlsName).unlock(key);
                  break;
                case 3:
                  logger.info("BasicLockClient suspendLocking");
                  assertTrue(DistributedLockService.getServiceNamed(dlsName).suspendLocking(-1));
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
            assertTrue(waitMillis > 0);
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
