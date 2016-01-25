/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.locks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.locks.DLockRecoverGrantorProcessor;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.TXRegionLockRequestImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * This class tests distributed ownership via the DistributedLockService api.
 */
public class TXLockServiceDUnitTest extends DistributedTestCase {
  
  private static DistributedSystem system;
  
  protected static boolean testTXRecoverGrantor_replyCode_PASS = false;
  protected static boolean testTXRecoverGrantor_heldLocks_PASS = false;
  
  private InternalDistributedMember lockGrantor;

  public TXLockServiceDUnitTest(String name) {
    super(name);
  }
  
  // -------------------------------------------------------------------------
  //   Lifecycle methods
  // -------------------------------------------------------------------------
 
  /**
   * Returns a previously created (or new, if this is the first
   * time this method is called in this VM) distributed system
   * which is somewhat configurable via hydra test parameters.
   */
  public void setUp() throws Exception {
    super.setUp();
    
    // Create a DistributedSystem in every VM
    connectDistributedSystem();

    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        //host.getVM(v).invoke(TXLockServiceDUnitTest.class, "dumpStack");
        host.getVM(v).invoke(
          TXLockServiceDUnitTest.class, "connectDistributedSystem", null);
      }
    }
  }
  
  public static void dumpStack() {
    com.gemstone.gemfire.internal.OSProcess.printStacks(0);
  }

  public void tearDown2() throws Exception {
//    invokeInEveryVM(TXLockServiceDUnitTest.class,
//                    "remoteDumpAllDLockServices");
                    
    invokeInEveryVM(TXLockServiceDUnitTest.class,
                    "destroyServices"); 
    
    destroyServices();
    
//     // Disconnects the DistributedSystem in every VM - since
//     // each test randomly chooses whether shared memory is used
//     disconnectAllFromDS();
    
    this.lockGrantor = null;

    testTXRecoverGrantor_replyCode_PASS = false;
    testTXRecoverGrantor_heldLocks_PASS = false;
  
    // Disconnects from GemFire if using shared memory
    super.tearDown2();
  }
  
  // -------------------------------------------------------------------------
  //   Test methods
  // -------------------------------------------------------------------------
  
  public void testGetAndDestroy() {
    forEachVMInvoke("checkGetAndDestroy", new Object[] {});
    /*invokeInEveryVM(TXLockServiceDUnitTest.class,
                    "destroyServices"); 
    forEachVMInvoke("checkGetAndDestroy", new Object[] {});*/
  }
  
  public void _ttestGetAndDestroyAgain() {
    testGetAndDestroy();
  }
  
  public void disable_testTXRecoverGrantorMessageProcessor() throws Exception {
    getLogWriter().info("[testTXOriginatorRecoveryProcessor]");
    TXLockService.createDTLS();
    checkDLockRecoverGrantorMessageProcessor();
    
    /* call TXRecoverGrantorMessageProcessor.process directly to make sure that
       correct behavior occurs
     */
     
    // get txLock and hold it
    final Set participants = Collections.EMPTY_SET;
    final List regionLockReqs = new ArrayList();
    regionLockReqs.add(new TXRegionLockRequestImpl(
        "/testTXRecoverGrantorMessageProcessor",
        new HashSet(Arrays.asList(
            new String[] { "KEY-1", "KEY-2", "KEY-3", "KEY-4" }))
        ));
    TXLockService dtls = TXLockService.getDTLS();
    TXLockId txLockId = dtls.txLock(regionLockReqs, participants);
    
    // async call TXRecoverGrantorMessageProcessor.process
    final DLockService dlock = 
        ((TXLockServiceImpl)dtls).getInternalDistributedLockService();
    final TestDLockRecoverGrantorProcessor testProc = 
        new TestDLockRecoverGrantorProcessor(
            dlock.getDistributionManager(), Collections.EMPTY_SET);
    assertEquals("No valid processorId", true, testProc.getProcessorId() > -1);
    
    final DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage msg = 
        new DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage();
    msg.setServiceName(dlock.getName());
    msg.setProcessorId(testProc.getProcessorId());
    msg.setSender(dlock.getDistributionManager().getId());
    
    Thread thread = new Thread(new Runnable() {
      public void run() {
        TXRecoverGrantorMessageProcessor proc = (TXRecoverGrantorMessageProcessor) 
            dlock.getDLockRecoverGrantorMessageProcessor();
        proc.processDLockRecoverGrantorMessage(
            dlock.getDistributionManager(), msg);
      }
    });
    thread.start();

    // pause to allow thread to be blocked before we release the lock
    sleep(999);
    
    // release txLock
    dtls.release(txLockId);
    
    // check results to verify no locks were provided in reply
    DistributedTestCase.join(thread, 30 * 1000, getLogWriter());
    assertEquals("testTXRecoverGrantor_replyCode_PASS is false", true, 
        testTXRecoverGrantor_replyCode_PASS);
    assertEquals("testTXRecoverGrantor_heldLocks_PASS is false", true, 
        testTXRecoverGrantor_heldLocks_PASS);
  }
  
  protected static volatile TXLockId testTXLock_TXLockId;
  public void testTXLock() {
    getLogWriter().info("[testTXLock]");
    final int grantorVM = 0;
    final int clientA = 1;
    final int clientB = 2;
    final Set participants = Collections.EMPTY_SET;
    final List regionLockReqs = new ArrayList();
    regionLockReqs.add(new TXRegionLockRequestImpl(
        "/testTXLock1",
        new HashSet(Arrays.asList(
            new String[] { "KEY-1", "KEY-2", "KEY-3", "KEY-4" }))
        ));
    regionLockReqs.add(new TXRegionLockRequestImpl(
        "/testTXLock2",
        new HashSet(Arrays.asList(
            new String[] { "KEY-A", "KEY-B", "KEY-C", "KEY-D" }))
        ));
    
    // create grantor
    getLogWriter().info("[testTXLock] create grantor");
    
    Host.getHost(0).getVM(grantorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS();
      }
    });
    sleep(20);
    
    // create client and request txLock
    getLogWriter().info("[testTXLock] create clientA and request txLock");
    
    Host.getHost(0).getVM(clientA).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS();
      }
    });
    
    Host.getHost(0).getVM(clientA).invoke(new SerializableRunnable(
        "[testTXLock] create clientA and request txLock") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        testTXLock_TXLockId = dtls.txLock(regionLockReqs, participants);
        assertNotNull("testTXLock_TXLockId is null", testTXLock_TXLockId);
      }
    });
    
    // create nuther client and request overlapping txLock... verify fails
    getLogWriter().info("[testTXLock] create clientB and fail txLock");
    
    Host.getHost(0).getVM(clientB).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS();
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
    
    /* try {
      Host.getHost(0).getVM(clientB).invoke(
        TXLockServiceDUnitTest.class, "txLock_DTLS", 
        new Object[] { regionLockReqs, participants });
      fail("expected CommitConflictException");
    } catch (RMIException expected) {
      assertTrue(expected.getCause() instanceof CommitConflictException);
    }
    */
    
    // release txLock
    getLogWriter().info("[testTXLock] clientA releases txLock");
    
    Host.getHost(0).getVM(clientA).invoke(
        new SerializableRunnable("[testTXLock] clientA releases txLock") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        dtls.release(testTXLock_TXLockId);
      }
    });
    sleep(20);
    
    // try nuther client again and verify success
    getLogWriter().info("[testTXLock] clientB requests txLock");
    
    Host.getHost(0).getVM(clientB).invoke(
        new SerializableRunnable("[testTXLock] clientB requests txLock") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        testTXLock_TXLockId = dtls.txLock(regionLockReqs, participants);
        assertNotNull("testTXLock_TXLockId is null", testTXLock_TXLockId);
      }
    });

    // release txLock
    getLogWriter().info("[testTXLock] clientB releases txLock");
    
    Host.getHost(0).getVM(clientB).invoke(
        new SerializableRunnable("[testTXLock] clientB releases txLock") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        dtls.release(testTXLock_TXLockId);
      }
    });
  }
  
  protected static volatile TXLockId testTXOriginatorRecoveryProcessor_TXLockId;
  public void testTXOriginatorRecoveryProcessor() {
    getLogWriter().info("[testTXOriginatorRecoveryProcessor]");
    final int originatorVM = 0;
    final int grantorVM = 1;
    final int particpantA = 2;
    final int particpantB = 3;
    
    final List regionLockReqs = new ArrayList();
    regionLockReqs.add(new TXRegionLockRequestImpl(
        "/testTXOriginatorRecoveryProcessor",
        new HashSet(Arrays.asList(
            new String[] { "KEY-1", "KEY-2", "KEY-3", "KEY-4" }))
        ));
        
    // build participants set...
    InternalDistributedMember dmId = null;
    final Set participants = new HashSet();
    for (int i = 1; i <= particpantB; i++) {
      final int finalvm = i;
      dmId = (InternalDistributedMember)Host.getHost(0).getVM(finalvm).invoke(
          TXLockServiceDUnitTest.class, "fetchDistributionManagerId", new Object[] {});
      assertEquals("dmId should not be null for vm " + finalvm, 
                   false, dmId == null);
      participants.add(dmId);
    }
    
    // create grantor
    getLogWriter().info("[testTXOriginatorRecoveryProcessor] grantorVM becomes grantor");
    
    Host.getHost(0).getVM(grantorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS();
      }
    });
    
    Host.getHost(0).getVM(grantorVM).invoke(
        TXLockServiceDUnitTest.class, "identifyLockGrantor_DTLS", new Object[] {});
        
    Boolean isGrantor = (Boolean)Host.getHost(0).getVM(grantorVM).invoke(
        TXLockServiceDUnitTest.class, "isLockGrantor_DTLS", new Object[] {});
    assertEquals("isLockGrantor should not be false for DTLS", 
                 Boolean.TRUE, isGrantor);
    
    // have a originatorVM get a txLock with three participants including grantor
    getLogWriter().info("[testTXOriginatorRecoveryProcessor] originatorVM requests txLock");
    
    Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS();
      }
    });
    Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable(
        "[testTXOriginatorRecoveryProcessor] originatorVM requests txLock") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        testTXOriginatorRecoveryProcessor_TXLockId = 
            dtls.txLock(regionLockReqs, participants);
        assertNotNull("testTXOriginatorRecoveryProcessor_TXLockId is null", 
                      testTXOriginatorRecoveryProcessor_TXLockId);
      }
    });
    
    // create dtls in each participant
    Host.getHost(0).getVM(particpantA).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS();
      }
    });
    Host.getHost(0).getVM(particpantB).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.createDTLS();
      }
    });
        
    // disconnect originatorVM without releasing txLock
    /* doesn't currently trigger the DLockLessorDepatureHandler... TODO
    Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.destroyServices();
      }
    });*/
    
    /*Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() {
      public void run() {
        InternalDistributedSystem sys = (InternalDistributedSystem) 
            InternalDistributedSystem.getAnyInstance();
        if (sys != null) {
          sys.disconnect();
        }
      }
    });*/
    
    Host.getHost(0).getVM(originatorVM).invoke(new SerializableRunnable() {
      public void run() {
        TXLockService.destroyServices();
      }
    });
    Host.getHost(0).getVM(originatorVM).invoke(
        DistributedTestCase.class, "disconnectFromDS", 
        new Object[] {});
    
    
    // grantor sends TXOriginatorRecoveryMessage...
    // TODO: verify processing of message? and have test sleep until finished
    sleep(200);
    
    // verify txLock is released...
    Host.getHost(0).getVM(particpantA).invoke(new SerializableRunnable(
        "[testTXOriginatorRecoveryProcessor] verify txLock is released") {
      public void run() {
        TXLockService dtls = TXLockService.getDTLS();
        testTXOriginatorRecoveryProcessor_TXLockId = 
            dtls.txLock(regionLockReqs, participants);
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
  
  public void testDTLSIsDistributed() {
    getLogWriter().info("[testDTLSIsDistributed]");
    
    // have all vms lock and hold the same LTLS lock simultaneously
    final Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    for (int vm = 0; vm < vmCount; vm++) {
      final int finalvm = vm;
      getLogWriter().info("[testDTLSIsDistributed] testing vm " + finalvm);
    
      Host.getHost(0).getVM(finalvm).invoke(new SerializableRunnable() {
        public void run() {
          TXLockService.createDTLS();
        }
      });
          
      // assert that isDistributed returns false
      Boolean isDistributed = (Boolean)host.getVM(finalvm).invoke(
          TXLockServiceDUnitTest.class, "isDistributed_DTLS", new Object[] {});
      assertEquals("isDistributed should be true for DTLS", 
                   Boolean.TRUE, isDistributed);
      getLogWriter().info("[testDTLSIsDistributed] isDistributed=" + isDistributed);
                   
      // lock a key...                
      Boolean gotLock = (Boolean)host.getVM(finalvm).invoke(
          TXLockServiceDUnitTest.class, "lock_DTLS", new Object[] {"KEY"});
      assertEquals("gotLock is false after calling lock_DTLS", 
                   Boolean.TRUE, gotLock);
      getLogWriter().info("[testDTLSIsDistributed] gotLock=" + gotLock);
      
      // unlock it...                
      Boolean unlock = (Boolean)host.getVM(finalvm).invoke(
          TXLockServiceDUnitTest.class, "unlock_DTLS", new Object[] {"KEY"});
      assertEquals("unlock is false after calling unlock_DTLS", 
                   Boolean.TRUE, unlock);
      getLogWriter().info("[testDTLSIsDistributed] unlock=" + unlock);
    }
  }
  
  // -------------------------------------------------------------------------
  //   Static support methods
  // -------------------------------------------------------------------------
  
  public static void remoteDumpAllDLockServices() {
    DLockService.dumpAllServices();
  }

  /**
   * Creates a new DistributedLockService in a remote VM.
   *
   * @param name
   *        The name of the newly-created DistributedLockService.  It is
   *        recommended that the name of the Region be the {@link
   *        #getUniqueName()} of the test, or at least derive from it.
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
    system = (new TXLockServiceDUnitTest("dummy")).getSystem();
  }
  
  private static InternalDistributedMember identifyLockGrantor(String serviceName) {
    DLockService service = (DLockService)
        DistributedLockService.getServiceNamed(serviceName);
    assertNotNull(service);
    
    InternalDistributedMember grantor = service.getLockGrantorId().getLockGrantorMember();
    assertNotNull(grantor);
    logInfo("In identifyLockGrantor - grantor is " + grantor);
    return grantor;
  }
  
  /**
   * Accessed via reflection.  DO NOT REMOVE
   */
  protected static InternalDistributedMember identifyLockGrantor_DTLS() {
    TXLockService dtls = TXLockService.getDTLS();
    String serviceName = ((TXLockServiceImpl)dtls).
        getInternalDistributedLockService().getName();
    return identifyLockGrantor(serviceName);
  }
  
  /**
   * Accessed via reflection.  DO NOT REMOVE
   */
  protected static Boolean lock_DTLS(Object key) {
    TXLockService dtls = TXLockService.getDTLS();
    boolean gotLock = ((TXLockServiceImpl)dtls).
        getInternalDistributedLockService().lock(key, -1, -1);
    logInfo("lock_LTLS gotLock (hopefully true): " + gotLock);
    return Boolean.valueOf(gotLock);
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE.
   */
  protected static Boolean isLockGrantor_DTLS() {
    TXLockService dtls = TXLockService.getDTLS();
    
    if (true) {
      DLockService service =
        DLockService.getInternalServiceNamed(((TXLockServiceImpl)dtls).
          getInternalDistributedLockService().getName());
    assertNotNull(service);
    
    assertEquals("DTLS and DLock should both report same isLockGrantor result", 
                 true, 
                 dtls.isLockGrantor() == service.isLockGrantor());
    }
    
    Boolean result = Boolean.valueOf(dtls.isLockGrantor());
    logInfo("isLockGrantor_DTLS: " + result);
    return result;
  }
  
  /**
   * Accessed via reflection.  DO NOT REMOVE
   */
  protected static Boolean isDistributed_DTLS() {
    TXLockService dtls = TXLockService.getDTLS();
    boolean isDistributed = ((TXLockServiceImpl)dtls).
        getInternalDistributedLockService().isDistributed();

    DLockService svc =
      ((TXLockServiceImpl)dtls).getInternalDistributedLockService();
    assertNotNull(svc);
    assertEquals("DTLS InternalDistributedLockService should not be destroyed", 
                 false, svc.isDestroyed());
        
    //sleep(50);
        
    if (true) {
      DLockService service =
        DLockService.getInternalServiceNamed(svc.getName());
    assertNotNull(service);
    
    assertEquals("DTLS and DLock should both report same isDistributed result", 
                 true, isDistributed == service.isDistributed());
    }
    
    Boolean result = Boolean.valueOf(isDistributed);
    logInfo("isDistributed_DTLS (hopefully true): " + result);
    return result;
  }
  
//  private static void becomeLockGrantor(String serviceName) {
//    InternalDistributedLockService service = (InternalDistributedLockService)
//        DistributedLockService.getServiceNamed(serviceName);
//    assertNotNull(service);
//    logInfo("About to call becomeLockGrantor...");
//    service.becomeLockGrantor();
//  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   */
  protected static void checkGetAndDestroy() {
    assertNull(TXLockService.getDTLS());
    
    TXLockService dtls = TXLockService.createDTLS();
    assertNotNull(dtls);
    assertEquals(true, dtls == TXLockService.getDTLS());
    assertEquals(false, dtls.isDestroyed());
    
    TXLockService.destroyServices();
    assertEquals(true, dtls.isDestroyed());
    assertNull(TXLockService.getDTLS());
    
    dtls = TXLockService.createDTLS();
    assertNotNull(dtls);
    assertEquals(true, dtls == TXLockService.getDTLS());
    assertEquals(false, dtls.isDestroyed());
  }
  
  /**
   * Accessed via reflection.  DO NOT REMOVE
   * @param key
   * @return
   */
  protected static Boolean unlock_DTLS(Object key) {
    TXLockService dtls = TXLockService.getDTLS();
    try {
      ((TXLockServiceImpl)dtls).getInternalDistributedLockService().unlock(key);
      return Boolean.TRUE;
    }
    catch (Exception e) {
      return Boolean.FALSE;
    }
  }
 
  
  /**
   * Accessed via reflection.  DO NOT REMOVE.
   */
  protected static InternalDistributedMember fetchDistributionManagerId() {
    InternalDistributedSystem sys = 
        InternalDistributedSystem.getAnyInstance();
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
  //   Non-static support methods
  // -------------------------------------------------------------------------
  
  /**
   * Assumes there is only one host, and invokes the given method in
   * the first numVMs VMs that host knows about.
   */
  public void forNumVMsInvoke(int numVMs, String methodName, Object[] args) {
    Host host = Host.getHost(0);
    for (int i=0; i<numVMs; i++) {
      logInfo("Invoking " + methodName + "on VM#" + i);
      host.getVM(i).invoke(this.getClass(), methodName, args);
    }
  }
  
  public void forEachVMInvoke(String methodName, Object[] args) {
    Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    for (int i=0; i<vmCount; i++) {
      getLogWriter().info("Invoking " + methodName + "on VM#" + i);
      host.getVM(i).invoke(this.getClass(), methodName, args);
    }
  }
  
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty("log-level", getDUnitLogLevel());
    return props;
  }

//  private synchronized void assertGrantorIsConsistent(Serializable id) {
//    if (this.lockGrantor == null) {
//      this.lockGrantor = id;
//    } else {
//      assertEquals("assertGrantorIsConsistent failed", lockGrantor, id);
//    }
//  }
  
//  private void distributedCreateService(int numVMs, String serviceName) {
//    forEachVMInvoke(
//      "remoteCreateService", 
//      new Object[] { serviceName });
//    
//    remoteCreateService(serviceName);
//  }
  
  private void checkDLockRecoverGrantorMessageProcessor() {
    /* simple test to make sure getDLockRecoverGrantorMessageProcessor returns
       instance of TXRecoverGrantorMessageProcessor
     */
    DLockService dlock = null;
    
    TXLockServiceImpl dtls = (TXLockServiceImpl) TXLockService.getDTLS();
    assertNotNull("DTLS should not be null", dtls);
    dlock = dtls.getInternalDistributedLockService();
    assertEquals("DTLS should use TXRecoverGrantorMessageProcessor", true, 
        dlock.getDLockRecoverGrantorMessageProcessor() instanceof 
        TXRecoverGrantorMessageProcessor);
  }

  // -------------------------------------------------------------------------
  //   Inner class
  // -------------------------------------------------------------------------
  
  private static class TestDLockRecoverGrantorProcessor 
  extends ReplyProcessor21 {
    public TestDLockRecoverGrantorProcessor(DM dm, Set members) {
        super(dm.getSystem(), members);
    }
    protected boolean allowReplyFromSender() {
      return true;
    }
    public void process(DistributionMessage msg) {
      DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage reply = 
          (DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage) msg;
      testTXRecoverGrantor_replyCode_PASS = 
          (reply.getReplyCode() == 
          DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage.OK);
      testTXRecoverGrantor_heldLocks_PASS =
          (reply.getHeldLocks().length == 0); 
    }
  }
  
}

