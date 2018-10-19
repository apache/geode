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
package org.apache.geode.internal.cache.locks;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.locks.DLockRecoverGrantorProcessor;
import org.apache.geode.distributed.internal.locks.DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXRegionLockRequestImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DLockTest;

/**
 * This class tests distributed ownership via the DistributedLockService api.
 */
@Category({DLockTest.class})
public class TXLockServiceDUnitTest extends JUnit4DistributedTestCase {

  private static InternalDistributedSystem system;

  protected static boolean testTXRecoverGrantor_replyCode_PASS = false;
  protected static boolean testTXRecoverGrantor_heldLocks_PASS = false;

  private InternalDistributedMember lockGrantor;

  public TXLockServiceDUnitTest() {
    super();
  }

  // -------------------------------------------------------------------------
  // Lifecycle methods
  // -------------------------------------------------------------------------

  /**
   * Returns a previously created (or new, if this is the first time this method is called in this
   * VM) distributed system which is somewhat configurable via hydra test parameters.
   */
  @Override
  public final void postSetUp() throws Exception {
    Invoke.invokeInEveryVM("connectDistributedSystem", () -> connectDistributedSystem());
    connectDistributedSystem();
  }


  @Override
  public final void preTearDown() throws Exception {
    // invokeInEveryVM(TXLockServiceDUnitTest.class,
    // "remoteDumpAllDLockServices");

    Invoke.invokeInEveryVM(TXLockServiceDUnitTest.class, "destroyServices");

    destroyServices();

    // // Disconnects the DistributedSystem in every VM - since
    // // each test randomly chooses whether shared memory is used
    // disconnectAllFromDS();

    this.lockGrantor = null;

    testTXRecoverGrantor_replyCode_PASS = false;
    testTXRecoverGrantor_heldLocks_PASS = false;
  }

  // -------------------------------------------------------------------------
  // Test methods
  // -------------------------------------------------------------------------

  @Test
  public void testGetAndDestroy() {
    forEachVMInvoke("checkGetAndDestroy", new Object[] {});
    /*
     * invokeInEveryVM(TXLockServiceDUnitTest.class, "destroyServices");
     * forEachVMInvoke("checkGetAndDestroy", new Object[] {});
     */
  }

  @Test
  public void testGetAndDestroyAgain() {
    testGetAndDestroy();
  }

  @Test
  public void testTXRecoverGrantorMessageProcessor() throws Exception {
    TXLockService.createDTLS(system);
    checkDLockRecoverGrantorMessageProcessor();

    /*
     * call TXRecoverGrantorMessageProcessor.process directly to make sure that correct behavior
     * occurs
     */

    // get txLock and hold it
    final Set participants = Collections.EMPTY_SET;
    final List regionLockReqs = new ArrayList();
    regionLockReqs.add(new TXRegionLockRequestImpl("/testTXRecoverGrantorMessageProcessor",
        new HashSet(Arrays.asList(new String[] {"KEY-1", "KEY-2", "KEY-3", "KEY-4"}))));
    TXLockService dtls = TXLockService.getDTLS();
    TXLockId txLockId = dtls.txLock(regionLockReqs, participants);

    // async call TXRecoverGrantorMessageProcessor.process
    final DLockService dlock = ((TXLockServiceImpl) dtls).getInternalDistributedLockService();
    final TestDLockRecoverGrantorProcessor testProc =
        new TestDLockRecoverGrantorProcessor(dlock.getDistributionManager(), Collections.EMPTY_SET);
    assertEquals("No valid processorId", true, testProc.getProcessorId() > -1);

    final DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage msg =
        new DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage();
    msg.setServiceName(dlock.getName());
    msg.setProcessorId(testProc.getProcessorId());
    msg.setSender(dlock.getDistributionManager().getId());

    Thread thread = new Thread(() -> {
      TXRecoverGrantorMessageProcessor proc =
          (TXRecoverGrantorMessageProcessor) dlock.getDLockRecoverGrantorMessageProcessor();
      proc.processDLockRecoverGrantorMessage(dlock.getDistributionManager(), msg);
    });
    thread.setName("TXLockServiceDUnitTest thread");
    thread.setDaemon(true);
    thread.start();

    await("waiting for recovery message to block").until(() -> {
      return ((TXLockServiceImpl) dtls).isRecovering();
    });

    dtls.release(txLockId);

    // check results to verify no locks were provided in the reply
    await("waiting for thread to exit").until(() -> {
      return !thread.isAlive();
    });

    assertFalse(((TXLockServiceImpl) dtls).isRecovering());

    assertEquals("testTXRecoverGrantor_replyCode_PASS is false", true,
        testTXRecoverGrantor_replyCode_PASS);
    assertEquals("testTXRecoverGrantor_heldLocks_PASS is false", true,
        testTXRecoverGrantor_heldLocks_PASS);
  }


  @Test
  public void testTXGrantorMigration() throws Exception {
    // first make sure some other VM is the grantor
    Host.getHost(0).getVM(0).invoke("become lock grantor", () -> {
      TXLockService.createDTLS(system);
      TXLockService vm0dtls = TXLockService.getDTLS();
      DLockService vm0dlock = ((TXLockServiceImpl) vm0dtls).getInternalDistributedLockService();
      vm0dlock.becomeLockGrantor();
    });

    TXLockService.createDTLS(system);
    checkDLockRecoverGrantorMessageProcessor();

    /*
     * call TXRecoverGrantorMessageProcessor.process directly to make sure that correct behavior
     * occurs
     */

    // get txLock and hold it
    final List regionLockReqs = new ArrayList();
    regionLockReqs.add(new TXRegionLockRequestImpl("/testTXRecoverGrantorMessageProcessor2",
        new HashSet(Arrays.asList(new String[] {"KEY-1", "KEY-2", "KEY-3", "KEY-4"}))));
    TXLockService dtls = TXLockService.getDTLS();
    TXLockId txLockId = dtls.txLock(regionLockReqs, Collections.EMPTY_SET);

    final DLockService dlock = ((TXLockServiceImpl) dtls).getInternalDistributedLockService();

    // GEODE-2024: now cause grantor migration while holding the recoveryReadLock.
    // It will lock up in TXRecoverGrantorMessageProcessor until the recoveryReadLock
    // is released. Demonstrate that dtls.release() does not block forever and releases the
    // recoveryReadLock
    // allowing grantor migration to finish

    // create an observer that will block recovery messages from being processed
    MessageObserver observer = new MessageObserver();
    DistributionMessageObserver.setInstance(observer);

    try {
      System.out.println("starting thread to take over being lock grantor from vm0");

      // become the grantor - this will block waiting for a reply to the message blocked by the
      // observer
      Thread thread = new Thread(() -> {
        dlock.becomeLockGrantor();
      });
      thread.setName("TXLockServiceDUnitTest thread2");
      thread.setDaemon(true);
      thread.start();

      await("waiting for recovery to begin").until(() -> {
        return observer.isPreventingProcessing();
      });


      // spawn a thread that will unblock message processing
      // so that TXLockServiceImpl's "recovering" variable will be set
      System.out.println("starting a thread to unblock recovery in 5 seconds");
      Thread unblockThread = new Thread(() -> {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          throw new RuntimeException("sleep interrupted");
        }
        System.out.println("releasing block of recovery message processing");
        observer.releasePreventionOfProcessing();
      });
      unblockThread.setName("TXLockServiceDUnitTest unblockThread");
      unblockThread.setDaemon(true);
      unblockThread.start();

      // release txLock - this will block until unblockThread tells the observer
      // that it can process its message. Then it should release the recovery read-lock
      // allowing the grantor to finish recovery
      System.out.println("releasing transaction locks, which should block for a bit");
      dtls.release(txLockId);

      await("waiting for recovery to finish").until(() -> {
        return !((TXLockServiceImpl) dtls).isRecovering();
      });
    } finally {
      observer.releasePreventionOfProcessing();
      DistributionMessageObserver.setInstance(null);
    }
  }

  static class MessageObserver extends DistributionMessageObserver {
    final boolean[] preventingMessageProcessing = new boolean[] {false};
    final boolean[] preventMessageProcessing = new boolean[] {true};


    public boolean isPreventingProcessing() {
      synchronized (preventingMessageProcessing) {
        return preventingMessageProcessing[0];
      }
    }

    public void releasePreventionOfProcessing() {
      synchronized (preventMessageProcessing) {
        preventMessageProcessing[0] = false;
      }
    }

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof DLockRecoverGrantorMessage) {
        synchronized (preventingMessageProcessing) {
          preventingMessageProcessing[0] = true;
        }
        synchronized (preventMessageProcessing) {
          while (preventMessageProcessing[0]) {
            try {
              preventMessageProcessing.wait(50);
            } catch (InterruptedException e) {
              throw new RuntimeException("sleep interrupted");
            }
          }
        }
      }
    }

  }


  protected static volatile TXLockId testTXLock_TXLockId;

  @Test
  public void testTXLock() {
    LogWriterUtils.getLogWriter().info("[testTXLock]");
    final int grantorVM = 0;
    final int clientA = 1;
    final int clientB = 2;
    final Set participants = Collections.EMPTY_SET;
    final List regionLockReqs = new ArrayList();
    regionLockReqs.add(new TXRegionLockRequestImpl("/testTXLock1",
        new HashSet(Arrays.asList(new String[] {"KEY-1", "KEY-2", "KEY-3", "KEY-4"}))));
    regionLockReqs.add(new TXRegionLockRequestImpl("/testTXLock2",
        new HashSet(Arrays.asList(new String[] {"KEY-A", "KEY-B", "KEY-C", "KEY-D"}))));

    // create grantor
    LogWriterUtils.getLogWriter().info("[testTXLock] create grantor");

    Host.getHost(0).getVM(grantorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS(system);
      }
    });
    sleep(20);

    // create client and request txLock
    LogWriterUtils.getLogWriter().info("[testTXLock] create clientA and request txLock");

    Host.getHost(0).getVM(clientA).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS(system);
      }
    });

    Host.getHost(0).getVM(clientA)
        .invoke(new SerializableRunnable("[testTXLock] create clientA and request txLock") {
          public void run() {
            TXLockService dtls = TXLockService.getDTLS();
            testTXLock_TXLockId = dtls.txLock(regionLockReqs, participants);
            assertNotNull("testTXLock_TXLockId is null", testTXLock_TXLockId);
          }
        });

    // create nuther client and request overlapping txLock... verify fails
    LogWriterUtils.getLogWriter().info("[testTXLock] create clientB and fail txLock");

    Host.getHost(0).getVM(clientB).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS(system);
      }
    });

    Host.getHost(0).getVM(clientB).invoke(new SerializableRunnable() {
      public void run() {
        try {
          TXLockService dtls = TXLockService.getDTLS();
          dtls.txLock(regionLockReqs, participants);
          fail("expected CommitConflictException");
        } catch (CommitConflictException expected) {
        }
      }
    });

    /*
     * try { Host.getHost(0).getVM(clientB).invoke(() -> TXLockServiceDUnitTest.txLock_DTLS(
     * regionLockReqs, participants )); fail("expected CommitConflictException"); } catch
     * (RMIException expected) { assertTrue(expected.getCause() instanceof CommitConflictException);
     * }
     */

    // release txLock
    LogWriterUtils.getLogWriter().info("[testTXLock] clientA releases txLock");

    Host.getHost(0).getVM(clientA)
        .invoke(new SerializableRunnable("[testTXLock] clientA releases txLock") {
          public void run() {
            TXLockService dtls = TXLockService.getDTLS();
            dtls.release(testTXLock_TXLockId);
          }
        });
    sleep(20);

    // try nuther client again and verify success
    LogWriterUtils.getLogWriter().info("[testTXLock] clientB requests txLock");

    Host.getHost(0).getVM(clientB)
        .invoke(new SerializableRunnable("[testTXLock] clientB requests txLock") {
          public void run() {
            TXLockService dtls = TXLockService.getDTLS();
            testTXLock_TXLockId = dtls.txLock(regionLockReqs, participants);
            assertNotNull("testTXLock_TXLockId is null", testTXLock_TXLockId);
          }
        });

    // release txLock
    LogWriterUtils.getLogWriter().info("[testTXLock] clientB releases txLock");

    Host.getHost(0).getVM(clientB)
        .invoke(new SerializableRunnable("[testTXLock] clientB releases txLock") {
          public void run() {
            TXLockService dtls = TXLockService.getDTLS();
            dtls.release(testTXLock_TXLockId);
          }
        });
  }

  protected static volatile TXLockId testTXOriginatorRecoveryProcessor_TXLockId;

  @Test
  public void testTXOriginatorRecoveryProcessor() {
    LogWriterUtils.getLogWriter().info("[testTXOriginatorRecoveryProcessor]");
    final int originatorVM = 0;
    final int grantorVM = 1;
    final int particpantA = 2;
    final int particpantB = 3;

    final List regionLockReqs = new ArrayList();
    regionLockReqs.add(new TXRegionLockRequestImpl("/testTXOriginatorRecoveryProcessor",
        new HashSet(Arrays.asList(new String[] {"KEY-1", "KEY-2", "KEY-3", "KEY-4"}))));

    // build participants set...
    InternalDistributedMember dmId = null;
    final Set participants = new HashSet();
    for (int i = 1; i <= particpantB; i++) {
      final int finalvm = i;
      dmId = (InternalDistributedMember) Host.getHost(0).getVM(finalvm)
          .invoke(() -> TXLockServiceDUnitTest.fetchDistributionManagerId());
      assertEquals("dmId should not be null for vm " + finalvm, false, dmId == null);
      participants.add(dmId);
    }

    // create grantor
    LogWriterUtils.getLogWriter()
        .info("[testTXOriginatorRecoveryProcessor] grantorVM becomes grantor");

    Host.getHost(0).getVM(grantorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS(system);
      }
    });

    Host.getHost(0).getVM(grantorVM)
        .invoke(() -> TXLockServiceDUnitTest.identifyLockGrantor_DTLS());

    Boolean isGrantor = (Boolean) Host.getHost(0).getVM(grantorVM)
        .invoke(() -> TXLockServiceDUnitTest.isLockGrantor_DTLS());
    assertEquals("isLockGrantor should not be false for DTLS", Boolean.TRUE, isGrantor);

    // have a originatorVM get a txLock with three participants including grantor
    LogWriterUtils.getLogWriter()
        .info("[testTXOriginatorRecoveryProcessor] originatorVM requests txLock");

    Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS(system);
      }
    });
    Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable(
        "[testTXOriginatorRecoveryProcessor] originatorVM requests txLock") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        testTXOriginatorRecoveryProcessor_TXLockId = dtls.txLock(regionLockReqs, participants);
        assertNotNull("testTXOriginatorRecoveryProcessor_TXLockId is null",
            testTXOriginatorRecoveryProcessor_TXLockId);
      }
    });

    // create dtls in each participant
    Host.getHost(0).getVM(particpantA).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS(system);
      }
    });
    Host.getHost(0).getVM(particpantB).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS(system);
      }
    });

    // disconnect originatorVM without releasing txLock
    /*
     * doesn't currently trigger the DLockLessorDepatureHandler... TODO
     * Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() { public void run() {
     * TXLockService.destroyServices(); } });
     */

    /*
     * Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() { public void run() {
     * InternalDistributedSystem sys = (InternalDistributedSystem)
     * InternalDistributedSystem.getAnyInstance(); if (sys != null) { sys.disconnect(); } } });
     */

    Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.destroyServices();
      }
    });
    Host.getHost(0).getVM(originatorVM).invoke(() -> disconnectFromDS());

    // grantor sends TXOriginatorRecoveryMessage...
    // TODO: verify processing of message? and have test sleep until finished
    sleep(200);

    // verify txLock is released...
    Host.getHost(0).getVM(particpantA).invoke(
        new SerializableRunnable("[testTXOriginatorRecoveryProcessor] verify txLock is released") {
          public void run() {
            TXLockService dtls = TXLockService.getDTLS();
            testTXOriginatorRecoveryProcessor_TXLockId = dtls.txLock(regionLockReqs, participants);
            assertNotNull("testTXOriginatorRecoveryProcessor_TXLockId is null",
                testTXOriginatorRecoveryProcessor_TXLockId);
          }
        });

    Host.getHost(0).getVM(particpantA).invoke(new SerializableRunnable(
        "[testTXOriginatorRecoveryProcessor] particpantA releases txLock") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        dtls.release(testTXOriginatorRecoveryProcessor_TXLockId);
      }
    });
  }

  @Test
  public void testDTLSIsDistributed() {
    LogWriterUtils.getLogWriter().info("[testDTLSIsDistributed]");

    // have all vms lock and hold the same LTLS lock simultaneously
    final Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    for (int vm = 0; vm < vmCount; vm++) {
      final int finalvm = vm;
      LogWriterUtils.getLogWriter().info("[testDTLSIsDistributed] testing vm " + finalvm);

      Host.getHost(0).getVM(finalvm).invoke(new SerializableRunnable() {
        public void run() {
          TXLockService.createDTLS(system);
        }
      });

      // assert that isDistributed returns false
      Boolean isDistributed =
          (Boolean) host.getVM(finalvm).invoke(() -> TXLockServiceDUnitTest.isDistributed_DTLS());
      assertEquals("isDistributed should be true for DTLS", Boolean.TRUE, isDistributed);
      LogWriterUtils.getLogWriter().info("[testDTLSIsDistributed] isDistributed=" + isDistributed);

      // lock a key...
      Boolean gotLock =
          (Boolean) host.getVM(finalvm).invoke(() -> TXLockServiceDUnitTest.lock_DTLS("KEY"));
      assertEquals("gotLock is false after calling lock_DTLS", Boolean.TRUE, gotLock);
      LogWriterUtils.getLogWriter().info("[testDTLSIsDistributed] gotLock=" + gotLock);

      // unlock it...
      Boolean unlock =
          (Boolean) host.getVM(finalvm).invoke(() -> TXLockServiceDUnitTest.unlock_DTLS("KEY"));
      assertEquals("unlock is false after calling unlock_DTLS", Boolean.TRUE, unlock);
      LogWriterUtils.getLogWriter().info("[testDTLSIsDistributed] unlock=" + unlock);
    }
  }

  // -------------------------------------------------------------------------
  // Static support methods
  // -------------------------------------------------------------------------

  public static void remoteDumpAllDLockServices() {
    DLockService.dumpAllServices();
  }

  /**
   * Creates a new DistributedLockService in a remote VM.
   *
   * @param name The name of the newly-created DistributedLockService. It is recommended that the
   *        name of the Region be the {@link #getUniqueName()} of the test, or at least derive from
   *        it.
   */
  protected static void remoteCreateService(String name) {
    DistributedLockService newService = DistributedLockService.create(name, system);
    logInfo("Created " + newService);
  }

  private static void logInfo(String msg) {
    system.getLogWriter().info(msg);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
  }

  /**
   * Connects a DistributedSystem, saves it in static variable "system"
   */
  private static void connectDistributedSystem() {
    system = (new TXLockServiceDUnitTest()).getSystem();
  }

  private static InternalDistributedMember identifyLockGrantor(String serviceName) {
    DLockService service = (DLockService) DistributedLockService.getServiceNamed(serviceName);
    assertNotNull(service);

    InternalDistributedMember grantor = service.getLockGrantorId().getLockGrantorMember();
    assertNotNull(grantor);
    logInfo("In identifyLockGrantor - grantor is " + grantor);
    return grantor;
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  protected static InternalDistributedMember identifyLockGrantor_DTLS() {
    TXLockService dtls = TXLockService.getDTLS();
    String serviceName = ((TXLockServiceImpl) dtls).getInternalDistributedLockService().getName();
    return identifyLockGrantor(serviceName);
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  protected static Boolean lock_DTLS(Object key) {
    TXLockService dtls = TXLockService.getDTLS();
    boolean gotLock =
        ((TXLockServiceImpl) dtls).getInternalDistributedLockService().lock(key, -1, -1);
    logInfo("lock_LTLS gotLock (hopefully true): " + gotLock);
    return Boolean.valueOf(gotLock);
  }

  /**
   * Accessed via reflection. DO NOT REMOVE.
   */
  protected static Boolean isLockGrantor_DTLS() {
    TXLockService dtls = TXLockService.getDTLS();

    if (true) {
      DLockService service = DLockService.getInternalServiceNamed(
          ((TXLockServiceImpl) dtls).getInternalDistributedLockService().getName());
      assertNotNull(service);

      assertEquals("DTLS and DLock should both report same isLockGrantor result", true,
          dtls.isLockGrantor() == service.isLockGrantor());
    }

    Boolean result = Boolean.valueOf(dtls.isLockGrantor());
    logInfo("isLockGrantor_DTLS: " + result);
    return result;
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  protected static Boolean isDistributed_DTLS() {
    TXLockService dtls = TXLockService.getDTLS();
    boolean isDistributed =
        ((TXLockServiceImpl) dtls).getInternalDistributedLockService().isDistributed();

    DLockService svc = ((TXLockServiceImpl) dtls).getInternalDistributedLockService();
    assertNotNull(svc);
    assertEquals("DTLS InternalDistributedLockService should not be destroyed", false,
        svc.isDestroyed());

    // sleep(50);

    if (true) {
      DLockService service = DLockService.getInternalServiceNamed(svc.getName());
      assertNotNull(service);

      assertEquals("DTLS and DLock should both report same isDistributed result", true,
          isDistributed == service.isDistributed());
    }

    Boolean result = Boolean.valueOf(isDistributed);
    logInfo("isDistributed_DTLS (hopefully true): " + result);
    return result;
  }

  // private static void becomeLockGrantor(String serviceName) {
  // InternalDistributedLockService service = (InternalDistributedLockService)
  // DistributedLockService.getServiceNamed(serviceName);
  // assertNotNull(service);
  // logInfo("About to call becomeLockGrantor...");
  // service.becomeLockGrantor();
  // }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  protected static void checkGetAndDestroy() {
    assertNull(TXLockService.getDTLS());

    TXLockService dtls = TXLockService.createDTLS(system);
    assertNotNull(dtls);
    assertEquals(true, dtls == TXLockService.getDTLS());
    assertEquals(false, dtls.isDestroyed());

    TXLockService.destroyServices();
    assertEquals(true, dtls.isDestroyed());
    assertNull(TXLockService.getDTLS());

    dtls = TXLockService.createDTLS(system);
    assertNotNull(dtls);
    assertEquals(true, dtls == TXLockService.getDTLS());
    assertEquals(false, dtls.isDestroyed());
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  protected static Boolean unlock_DTLS(Object key) {
    TXLockService dtls = TXLockService.getDTLS();
    try {
      ((TXLockServiceImpl) dtls).getInternalDistributedLockService().unlock(key);
      return Boolean.TRUE;
    } catch (Exception e) {
      return Boolean.FALSE;
    }
  }


  /**
   * Accessed via reflection. DO NOT REMOVE.
   */
  protected static InternalDistributedMember fetchDistributionManagerId() {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null) {
      return sys.getDistributionManager().getId();
    } else {
      return null;
    }
  }

  private static void destroyServices() {
    TXLockService.destroyServices();
  }

  // -------------------------------------------------------------------------
  // Non-static support methods
  // -------------------------------------------------------------------------

  /**
   * Assumes there is only one host, and invokes the given method in the first numVMs VMs that host
   * knows about.
   */
  public void forNumVMsInvoke(int numVMs, String methodName, Object[] args) {
    Host host = Host.getHost(0);
    for (int i = 0; i < numVMs; i++) {
      logInfo("Invoking " + methodName + "on VM#" + i);
      host.getVM(i).invoke(this.getClass(), methodName, args);
    }
  }

  public void forEachVMInvoke(String methodName, Object[] args) {
    Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    for (int i = 0; i < vmCount; i++) {
      LogWriterUtils.getLogWriter().info("Invoking " + methodName + "on VM#" + i);
      host.getVM(i).invoke(this.getClass(), methodName, args);
    }
  }

  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    return props;
  }

  // private synchronized void assertGrantorIsConsistent(Serializable id) {
  // if (this.lockGrantor == null) {
  // this.lockGrantor = id;
  // } else {
  // assertIndexDetailsEquals("assertGrantorIsConsistent failed", lockGrantor, id);
  // }
  // }

  // private void distributedCreateService(int numVMs, String serviceName) {
  // forEachVMInvoke(
  // "remoteCreateService",
  // new Object[] { serviceName });
  //
  // remoteCreateService(serviceName);
  // }

  private void checkDLockRecoverGrantorMessageProcessor() {
    /*
     * simple test to make sure getDLockRecoverGrantorMessageProcessor returns instance of
     * TXRecoverGrantorMessageProcessor
     */
    DLockService dlock = null;

    TXLockServiceImpl dtls = (TXLockServiceImpl) TXLockService.getDTLS();
    assertNotNull("DTLS should not be null", dtls);
    dlock = dtls.getInternalDistributedLockService();
    assertEquals("DTLS should use TXRecoverGrantorMessageProcessor", true,
        dlock.getDLockRecoverGrantorMessageProcessor() instanceof TXRecoverGrantorMessageProcessor);
  }

  // -------------------------------------------------------------------------
  // Inner class
  // -------------------------------------------------------------------------

  private static class TestDLockRecoverGrantorProcessor extends ReplyProcessor21 {
    public TestDLockRecoverGrantorProcessor(DistributionManager dm, Set members) {
      super(dm.getSystem(), members);
    }

    protected boolean allowReplyFromSender() {
      return true;
    }

    public void process(DistributionMessage msg) {
      DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage reply =
          (DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage) msg;
      testTXRecoverGrantor_replyCode_PASS =
          (reply.getReplyCode() == DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage.OK);
      testTXRecoverGrantor_heldLocks_PASS = (reply.getHeldLocks().length == 0);
    }
  }

}
