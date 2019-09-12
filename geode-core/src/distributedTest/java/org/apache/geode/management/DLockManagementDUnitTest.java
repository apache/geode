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
package org.apache.geode.management;

import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedLockServiceName;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getLockServiceMBeanName;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;


@SuppressWarnings({"serial", "unused"})
public class DLockManagementDUnitTest implements Serializable {

  private static final int MAX_WAIT_MILLIS = 2 * 60 * 1000; // 2 MINUTES

  private static final String LOCK_SERVICE_NAME =
      DLockManagementDUnitTest.class.getSimpleName() + "_testLockService";

  @Member
  private VM[] memberVMs;

  @Manager
  private VM managerVM;

  @Rule
  public ManagementTestRule managementTestRule =
      ManagementTestRule.builder().defineManagersFirst(false).start(true).build();

  @Test
  public void testLockServiceMXBean() throws Throwable {
    createLockServiceGrantor(this.memberVMs[0]);
    createLockService(this.memberVMs[1]);
    createLockService(this.memberVMs[2]);

    for (VM memberVM : this.memberVMs) {
      verifyLockServiceMXBeanInMember(memberVM);
    }
    verifyLockServiceMXBeanInManager(this.managerVM);

    for (VM memberVM : this.memberVMs) {
      closeLockService(memberVM);
    }
  }

  @Test
  public void testDistributedLockServiceMXBean() throws Throwable {
    createLockServiceGrantor(this.memberVMs[0]);
    createLockService(this.memberVMs[1]);
    createLockService(this.memberVMs[2]);

    verifyDistributedLockServiceMXBean(this.managerVM, 3);

    DistributedMember member = this.managementTestRule.getDistributedMember(this.memberVMs[2]);
    verifyFetchOperations(this.managerVM, member);

    createLockService(this.managerVM);
    verifyDistributedLockServiceMXBean(this.managerVM, 4);

    for (VM memberVM : this.memberVMs) {
      closeLockService(memberVM);
    }
    verifyProxyCleanupInManager(this.managerVM);
    verifyDistributedLockServiceMXBean(this.managerVM, 1);

    closeLockService(this.managerVM);
    verifyDistributedLockServiceMXBean(this.managerVM, 0);
  }

  private void verifyProxyCleanupInManager(final VM managerVM) {
    managerVM.invoke("verifyProxyCleanupInManager", () -> {
      List<DistributedMember> otherMembers = this.managementTestRule.getOtherNormalMembers();
      SystemManagementService service = this.managementTestRule.getSystemManagementService();

      for (final DistributedMember member : otherMembers) {
        ObjectName objectName = service.getRegionMBeanName(member, LOCK_SERVICE_NAME);
        GeodeAwaitility.await()
            .untilAsserted(() -> assertThat(lockServiceMXBeanIsGone(service, objectName)).isTrue());
      }
    });
  }

  private boolean lockServiceMXBeanIsGone(final SystemManagementService service,
      final ObjectName objectName) {
    return service.getMBeanProxy(objectName, LockServiceMXBean.class) == null;
  }

  private void createLockServiceGrantor(final VM memberVM) {
    memberVM.invoke("createLockServiceGrantor", () -> {
      assertThat(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME)).isNull();

      DLockService lockService = (DLockService) DistributedLockService.create(LOCK_SERVICE_NAME,
          this.managementTestRule.getCache().getDistributedSystem());
      DistributedMember grantor = lockService.getLockGrantorId().getLockGrantorMember();
      assertThat(grantor).isNotNull();

      LockServiceMXBean lockServiceMXBean = awaitLockServiceMXBean(LOCK_SERVICE_NAME);

      assertThat(lockServiceMXBean).isNotNull();
      assertThat(lockServiceMXBean.isDistributed()).isTrue();
      assertThat(lockServiceMXBean.getName()).isEqualTo(LOCK_SERVICE_NAME);
      assertThat(lockServiceMXBean.isLockGrantor()).isTrue();
      assertThat(lockServiceMXBean.fetchGrantorMember())
          .isEqualTo(this.managementTestRule.getDistributedMember().getId());
    });
  }

  private void createLockService(final VM anyVM) {
    anyVM.invoke("createLockService", () -> {
      assertThat(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME)).isNull();

      DistributedLockService.create(LOCK_SERVICE_NAME,
          this.managementTestRule.getCache().getDistributedSystem());

      LockServiceMXBean lockServiceMXBean = awaitLockServiceMXBean(LOCK_SERVICE_NAME);

      assertThat(lockServiceMXBean).isNotNull();
      assertThat(lockServiceMXBean.isDistributed()).isTrue();
      assertThat(lockServiceMXBean.isLockGrantor()).isFalse();
    });
  }

  private void closeLockService(final VM anyVM) {
    anyVM.invoke("closeLockService", () -> {
      assertThat(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME)).isNotNull();
      DistributedLockService.destroy(LOCK_SERVICE_NAME);

      awaitLockServiceMXBeanIsNull(LOCK_SERVICE_NAME);

      ManagementService service = this.managementTestRule.getManagementService();
      LockServiceMXBean lockServiceMXBean = service.getLocalLockServiceMBean(LOCK_SERVICE_NAME);
      assertThat(lockServiceMXBean).isNull();
    });
  }

  private void verifyLockServiceMXBeanInMember(final VM memberVM) {
    memberVM.invoke("verifyLockServiceMXBeanInManager", () -> {
      DistributedLockService lockService =
          DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
      lockService.lock("lockObject_" + identifyPid(), MAX_WAIT_MILLIS, -1);

      ManagementService service = this.managementTestRule.getManagementService();
      LockServiceMXBean lockServiceMXBean = service.getLocalLockServiceMBean(LOCK_SERVICE_NAME);
      assertThat(lockServiceMXBean).isNotNull();

      String[] listHeldLock = lockServiceMXBean.listHeldLocks();
      assertThat(listHeldLock).hasSize(1);

      Map<String, String> lockThreadMap = lockServiceMXBean.listThreadsHoldingLock();
      assertThat(lockThreadMap).hasSize(1);
    });
  }

  /**
   * Verify lock data from remote Managing node
   */
  private void verifyLockServiceMXBeanInManager(final VM managerVM) throws Exception {
    managerVM.invoke("verifyLockServiceMXBeanInManager", () -> {
      List<DistributedMember> otherMembers = this.managementTestRule.getOtherNormalMembers();

      for (DistributedMember member : otherMembers) {
        LockServiceMXBean lockServiceMXBean =
            awaitLockServiceMXBeanProxy(member, LOCK_SERVICE_NAME);
        assertThat(lockServiceMXBean).isNotNull();

        String[] listHeldLock = lockServiceMXBean.listHeldLocks();
        assertThat(listHeldLock).hasSize(1);

        Map<String, String> lockThreadMap = lockServiceMXBean.listThreadsHoldingLock();
        assertThat(lockThreadMap).hasSize(1);
      }
    });
  }

  private void verifyFetchOperations(final VM memberVM, final DistributedMember member) {
    memberVM.invoke("verifyFetchOperations", () -> {
      ManagementService service = this.managementTestRule.getManagementService();

      DistributedSystemMXBean distributedSystemMXBean = awaitDistributedSystemMXBean();
      ObjectName distributedLockServiceMXBeanName =
          getDistributedLockServiceName(LOCK_SERVICE_NAME);
      assertThat(distributedSystemMXBean.fetchDistributedLockServiceObjectName(LOCK_SERVICE_NAME))
          .isEqualTo(distributedLockServiceMXBeanName);

      ObjectName lockServiceMXBeanName = getLockServiceMBeanName(member.getId(), LOCK_SERVICE_NAME);
      assertThat(
          distributedSystemMXBean.fetchLockServiceObjectName(member.getId(), LOCK_SERVICE_NAME))
              .isEqualTo(lockServiceMXBeanName);
    });
  }

  /**
   * Verify Aggregate MBean
   */
  private void verifyDistributedLockServiceMXBean(final VM managerVM, final int memberCount) {
    managerVM.invoke("verifyDistributedLockServiceMXBean", () -> {
      ManagementService service = this.managementTestRule.getManagementService();

      if (memberCount == 0) {
        GeodeAwaitility.await().untilAsserted(
            () -> assertThat(service.getDistributedLockServiceMXBean(LOCK_SERVICE_NAME)).isNull());
        return;
      }

      DistributedLockServiceMXBean distributedLockServiceMXBean =
          awaitDistributedLockServiceMXBean(LOCK_SERVICE_NAME, memberCount);
      assertThat(distributedLockServiceMXBean).isNotNull();
      assertThat(distributedLockServiceMXBean.getName()).isEqualTo(LOCK_SERVICE_NAME);
    });
  }

  private DistributedSystemMXBean awaitDistributedSystemMXBean() {
    ManagementService service = this.managementTestRule.getManagementService();

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(service.getDistributedSystemMXBean()).isNotNull());

    return service.getDistributedSystemMXBean();
  }

  /**
   * Await and return a DistributedRegionMXBean proxy with specified member count.
   */
  private DistributedLockServiceMXBean awaitDistributedLockServiceMXBean(
      final String lockServiceName, final int memberCount) {
    ManagementService service = this.managementTestRule.getManagementService();

    GeodeAwaitility.await().untilAsserted(() -> {
      assertThat(service.getDistributedLockServiceMXBean(lockServiceName)).isNotNull();
      assertThat(service.getDistributedLockServiceMXBean(lockServiceName).getMemberCount())
          .isEqualTo(memberCount);
    });

    return service.getDistributedLockServiceMXBean(lockServiceName);
  }

  /**
   * Await and return a LockServiceMXBean proxy for a specific member and lockServiceName.
   */
  private LockServiceMXBean awaitLockServiceMXBeanProxy(final DistributedMember member,
      final String lockServiceName) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();
    ObjectName lockServiceMXBeanName = service.getLockServiceMBeanName(member, lockServiceName);

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(service.getMBeanProxy(lockServiceMXBeanName, LockServiceMXBean.class))
            .isNotNull());

    return service.getMBeanProxy(lockServiceMXBeanName, LockServiceMXBean.class);
  }

  /**
   * Await creation of local LockServiceMXBean for specified lockServiceName.
   */
  private LockServiceMXBean awaitLockServiceMXBean(final String lockServiceName) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(service.getLocalLockServiceMBean(lockServiceName)).isNotNull());

    return service.getLocalLockServiceMBean(lockServiceName);
  }

  /**
   * Await destruction of local LockServiceMXBean for specified lockServiceName.
   */
  private void awaitLockServiceMXBeanIsNull(final String lockServiceName) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(service.getLocalLockServiceMBean(lockServiceName)).isNull());
  }

}
