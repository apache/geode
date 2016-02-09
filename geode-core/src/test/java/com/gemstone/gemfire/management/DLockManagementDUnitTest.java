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
package com.gemstone.gemfire.management;

import java.util.Map;
import java.util.Set;

import javax.management.ObjectName;

import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

public class DLockManagementDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;


  private static final String LOCK_SERVICE_NAME = "testLockService";
  
  // This must be bigger than the dunit ack-wait-threshold for the revoke
  // tests. The command line is setting the ack-wait-threshold to be
  // 60 seconds.
  private static final int MAX_WAIT = 70 * 1000;

 
  public DLockManagementDUnitTest(String name) {
    super(name);

  }

  public void setUp() throws Exception {
    super.setUp();

  }

  /**
   * Distributed Lock Service test
   * 
   * @throws Exception
   */

  public void testDLockMBean() throws Throwable {
    
    initManagement(false);
    
    VM[] managedNodes = new VM[getManagedNodeList()
        .size()];
    VM managingNode = getManagingNode();
    
    getManagedNodeList().toArray(managedNodes);

    createGrantorLockService(managedNodes[0]);

    createLockService(managedNodes[1]);

    createLockService(managedNodes[2]);
    
    for (VM vm : getManagedNodeList()) {
      verifyLockData(vm);
    }
    verifyLockDataRemote(managingNode);

    for (VM vm : getManagedNodeList()) {
      closeLockService(vm);
    }
  }
  
  /**
   * Distributed Lock Service test
   * 
   * @throws Exception
   */

  public void testDLockAggregate() throws Throwable {
    initManagement(false);
    VM[] managedNodes = new VM[getManagedNodeList()
        .size()];
    VM managingNode = getManagingNode();
    
    getManagedNodeList().toArray(managedNodes);

    createGrantorLockService(managedNodes[0]);

    createLockService(managedNodes[1]);

    createLockService(managedNodes[2]);
    
    checkAggregate(managingNode, 3);
    DistributedMember member = getMember(managedNodes[2]);
    checkNavigation(managingNode, member);
    
    createLockService(managingNode);
    checkAggregate(managingNode, 4);
   

    for (VM vm : getManagedNodeList()) {
      closeLockService(vm);
    }
    ensureProxyCleanup(managingNode);
    checkAggregate(managingNode, 1);
    closeLockService(managingNode);
    checkAggregate(managingNode, 0);

  }
  
  public void ensureProxyCleanup(final VM vm) {

    SerializableRunnable ensureProxyCleanup = new SerializableRunnable(
        "Ensure Proxy cleanup") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();
        final SystemManagementService service = (SystemManagementService) getManagementService();
 

        for (final DistributedMember member : otherMemberSet) {
          RegionMXBean bean = null;
          try {

            Wait.waitForCriterion(new WaitCriterion() {

              LockServiceMXBean bean = null;

              public String description() {
                return "Waiting for the proxy to get deleted at managing node";
              }

              public boolean done() {
                ObjectName objectName = service.getRegionMBeanName(member, LOCK_SERVICE_NAME);
                bean = service.getMBeanProxy(objectName, LockServiceMXBean.class);
                boolean done = (bean == null);
                return done;
              }

            }, MAX_WAIT, 500, true);

          } catch (Exception e) {
            fail("could not remove proxies in required time");

          }
          assertNull(bean);

        }

      }
    };
    vm.invoke(ensureProxyCleanup);
  }

  /**
   * Creates a grantor lock service
   * 
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void createGrantorLockService(final VM vm) {
    SerializableRunnable createGrantorLockService = new SerializableRunnable(
        "Create Grantor LockService") {
      public void run() {
        GemFireCacheImpl cache  = GemFireCacheImpl.getInstance();
        assertNull(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME));

        DLockService service = (DLockService) DistributedLockService.create(
            LOCK_SERVICE_NAME, cache.getDistributedSystem());

        assertSame(service, DistributedLockService
            .getServiceNamed(LOCK_SERVICE_NAME));

        InternalDistributedMember grantor = service.getLockGrantorId()
            .getLockGrantorMember();

        assertNotNull(grantor);

        LogWriterUtils.getLogWriter().info("In identifyLockGrantor - grantor is " + grantor);

       

        ManagementService mgmtService = getManagementService();

        LockServiceMXBean bean = mgmtService
            .getLocalLockServiceMBean(LOCK_SERVICE_NAME);

        assertNotNull(bean);

        assertTrue(bean.isDistributed());

        assertEquals(bean.getName(), LOCK_SERVICE_NAME);

        assertTrue(bean.isLockGrantor());

        assertEquals(cache.getDistributedSystem().getMemberId(), bean
            .fetchGrantorMember());

      }
    };
    vm.invoke(createGrantorLockService);
  }

  /**
   * Creates a named lock service
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void createLockService(final VM vm) {
    SerializableRunnable createLockService = new SerializableRunnable(
        "Create LockService") {
      public void run() {
        assertNull(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME));
        GemFireCacheImpl cache  = GemFireCacheImpl.getInstance();
        DistributedLockService service = DistributedLockService.create(
            LOCK_SERVICE_NAME, cache.getDistributedSystem());

        assertSame(service, DistributedLockService
            .getServiceNamed(LOCK_SERVICE_NAME));

        

        ManagementService mgmtService = getManagementService();

        LockServiceMXBean bean = mgmtService
            .getLocalLockServiceMBean(LOCK_SERVICE_NAME);

        assertNotNull(bean);

        assertTrue(bean.isDistributed());

        assertFalse(bean.isLockGrantor());
      }
    };
    vm.invoke(createLockService);
  }

  /**
   * Closes a named lock service
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void closeLockService(final VM vm) {
    SerializableRunnable closeLockService = new SerializableRunnable(
        "Close LockService") {
      public void run() {

        DistributedLockService service = DistributedLockService
            .getServiceNamed(LOCK_SERVICE_NAME);

        DistributedLockService.destroy(LOCK_SERVICE_NAME);

        ManagementService mgmtService = getManagementService();

        LockServiceMXBean bean = null;
        try {

          bean = mgmtService.getLocalLockServiceMBean(LOCK_SERVICE_NAME);

        } catch (ManagementException mgs) {

        }
        assertNull(bean);

      }
    };
    vm.invoke(closeLockService);
  }

  /**
   * Lock data related verifications
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void verifyLockData(final VM vm) {
    SerializableRunnable verifyLockData = new SerializableRunnable(
        "Verify LockService") {
      public void run() {

        DistributedLockService service = DistributedLockService
            .getServiceNamed(LOCK_SERVICE_NAME);

        final String LOCK_OBJECT = "lockObject_" + vm.getPid();

        Wait.waitForCriterion(new WaitCriterion() {
          DistributedLockService service = null;

          public String description() {
            return "Waiting for the lock service to be initialised";
          }

          public boolean done() {
            DistributedLockService service = DistributedLockService
                .getServiceNamed(LOCK_SERVICE_NAME);
            boolean done = service != null;
            return done;
          }

        }, MAX_WAIT, 500, true);

        service.lock(LOCK_OBJECT, 1000, -1);
        

        ManagementService mgmtService = getManagementService();

        LockServiceMXBean bean = null;
        try {

          bean = mgmtService.getLocalLockServiceMBean(LOCK_SERVICE_NAME);

        } catch (ManagementException mgs) {

        }
        assertNotNull(bean);
        String[] listHeldLock = bean.listHeldLocks();
        assertEquals(listHeldLock.length, 1);
        LogWriterUtils.getLogWriter().info("List Of Lock Object is  " + listHeldLock[0]);
        Map<String, String> lockThreadMap = bean.listThreadsHoldingLock();
        assertEquals(lockThreadMap.size(), 1);
        LogWriterUtils.getLogWriter().info(
            "List Of Lock Thread is  " + lockThreadMap.toString());
      }
    };
    vm.invoke(verifyLockData);
  }

  /**
   * Verify lock data from remote Managing node
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void verifyLockDataRemote(final VM vm) {
    SerializableRunnable verifyLockDataRemote = new SerializableRunnable(
        "Verify LockService Remote") {
      public void run() {

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();

        for (DistributedMember member : otherMemberSet) {
          LockServiceMXBean bean = null;
          try {
            bean = MBeanUtil.getLockServiceMbeanProxy(member, LOCK_SERVICE_NAME);
          } catch (Exception e) {
            InternalDistributedSystem.getLoggerI18n().fine(
                "Undesired Result , LockServiceMBean Should not be null" + e);

          }
          assertNotNull(bean);
          String[] listHeldLock = bean.listHeldLocks();
          assertEquals(listHeldLock.length, 1);
          LogWriterUtils.getLogWriter().info("List Of Lock Object is  " + listHeldLock[0]);
          Map<String, String> lockThreadMap = bean.listThreadsHoldingLock();
          assertEquals(lockThreadMap.size(), 1);
          LogWriterUtils.getLogWriter().info(
              "List Of Lock Thread is  " + lockThreadMap.toString());
        }

      }
    };
    vm.invoke(verifyLockDataRemote);
  }
  
  protected void checkNavigation(final VM vm,
      final DistributedMember lockServiceMember) {
    SerializableRunnable checkNavigation = new SerializableRunnable(
        "Check Navigation") {
      public void run() {

        final ManagementService service = getManagementService();

        DistributedSystemMXBean disMBean = service.getDistributedSystemMXBean();
        try {
          ObjectName expected = MBeanJMXAdapter
              .getDistributedLockServiceName(LOCK_SERVICE_NAME);
          ObjectName actual = disMBean
              .fetchDistributedLockServiceObjectName(LOCK_SERVICE_NAME);
          assertEquals(expected, actual);
        } catch (Exception e) {
          fail("Lock Service Navigation Failed " + e);
        }

        try {
          ObjectName expected = MBeanJMXAdapter.getLockServiceMBeanName(
              lockServiceMember.getId(), LOCK_SERVICE_NAME);
          ObjectName actual = disMBean.fetchLockServiceObjectName(
              lockServiceMember.getId(), LOCK_SERVICE_NAME);
          assertEquals(expected, actual);
        } catch (Exception e) {
          fail("Lock Service Navigation Failed " + e);
        }

      }
    };
    vm.invoke(checkNavigation);
  }

  /**
   * Verify Aggregate MBean
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void checkAggregate(final VM vm, final int expectedMembers) {
    SerializableRunnable checkAggregate = new SerializableRunnable(
        "Verify Aggregate MBean") {
      public void run() {
        
        final ManagementService service = getManagementService();
        if (expectedMembers == 0) {
          try {
            Wait.waitForCriterion(new WaitCriterion() {

              DistributedLockServiceMXBean bean = null;

              public String description() {
                return "Waiting for the proxy to get deleted at managing node";
              }

              public boolean done() {
                bean = service
                    .getDistributedLockServiceMXBean(LOCK_SERVICE_NAME);

                boolean done = (bean == null);
                return done;
              }

            }, MAX_WAIT, 500, true);

          } catch (Exception e) {
            fail("could not remove Aggregate Bean in required time");

          }
          return;
        }

        DistributedLockServiceMXBean bean = null;
          try {
            bean = MBeanUtil.getDistributedLockMbean(LOCK_SERVICE_NAME, expectedMembers);
          } catch (Exception e) {
            InternalDistributedSystem.getLoggerI18n().fine(
                "Undesired Result , LockServiceMBean Should not be null" + e);

          }
          assertNotNull(bean);
          assertEquals(bean.getName(),LOCK_SERVICE_NAME);
   
      }
    };
    vm.invoke(checkAggregate);
  }
}
