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

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.Region.*;
import static org.apache.geode.management.internal.MBeanJMXAdapter.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.TestObjectSizerImpl;
import org.apache.geode.internal.cache.lru.LRUStatistics;
import org.apache.geode.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This class checks and verifies various data and operations exposed through RegionMXBean
 * interface.
 * </p>
 * Goal of the Test : RegionMBean gets created once region is created. Data like Region Attributes
 * data and stats are of proper value
 * </p>
 * TODO: complete refactoring this test to use ManagementTestRule
 */
@Category(DistributedTest.class)
@SuppressWarnings({"serial", "unused"})
public class RegionManagementDUnitTest extends ManagementTestBase {

  private static final String REGION_NAME = "MANAGEMENT_TEST_REGION";
  private static final String PARTITIONED_REGION_NAME = "MANAGEMENT_PAR_REGION";
  private static final String FIXED_PR_NAME = "MANAGEMENT_FIXED_PR";
  private static final String LOCAL_REGION_NAME = "TEST_LOCAL_REGION";
  private static final String LOCAL_SUB_REGION_NAME = "TEST_LOCAL_SUB_REGION";

  private static final String REGION_PATH = SEPARATOR + REGION_NAME;
  private static final String PARTITIONED_REGION_PATH = SEPARATOR + PARTITIONED_REGION_NAME;
  private static final String FIXED_PR_PATH = SEPARATOR + FIXED_PR_NAME;
  private static final String LOCAL_SUB_REGION_PATH =
      SEPARATOR + LOCAL_REGION_NAME + SEPARATOR + LOCAL_SUB_REGION_NAME;

  // field used in manager VM
  private static Region fixedPartitionedRegion;

  private static final AtomicReference<List<Notification>> MEMBER_NOTIFICATIONS_REF =
      new AtomicReference<>();
  private static final AtomicReference<List<Notification>> SYSTEM_NOTIFICATIONS_REF =
      new AtomicReference<>();

  @Manager
  private VM managerVM;

  @Member
  private VM[] memberVMs;

  @Before
  public void before() throws Exception {
    this.managerVM = getHost(0).getVM(0);

    this.memberVMs = new VM[3];
    this.memberVMs[0] = getHost(0).getVM(1);
    this.memberVMs[1] = getHost(0).getVM(2);
    this.memberVMs[2] = getHost(0).getVM(3);
  }

  @After
  public void after() throws Exception {
    invokeInEveryVM(() -> MEMBER_NOTIFICATIONS_REF.set(null));
    invokeInEveryVM(() -> SYSTEM_NOTIFICATIONS_REF.set(null));
    disconnectAllFromDS_tmp();
  }

  /**
   * Tests all Region MBean related Management APIs
   * <p>
   * a) Notification propagated to member MBean while a region is created
   * <p>
   * b) Creates and check a Distributed Region
   */
  @Test
  public void testDistributedRegion() throws Exception {
    createMembersAndThenManagers_tmp();

    // Adding notification listener for remote cache memberVMs
    addMemberNotificationListener(this.managerVM, 3); // TODO: why?

    for (VM memberVM : this.memberVMs) {
      createDistributedRegion_tmp(memberVM, REGION_NAME);
      verifyReplicateRegionAfterCreate(memberVM);
    }

    verifyRemoteDistributedRegion(this.managerVM, 3);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, REGION_PATH);
      verifyReplicatedRegionAfterClose(memberVM);
    }

    verifyProxyCleanup(this.managerVM);

    verifyMemberNotifications(this.managerVM, REGION_NAME, 3);
  }

  /**
   * Tests all Region MBean related Management APIs
   * <p>
   * a) Notification propagated to member MBean while a region is created
   * <p>
   * b) Created and check a Partitioned Region
   */
  @Test
  public void testPartitionedRegion() throws Exception {
    createMembersAndThenManagers_tmp();

    // Adding notification listener for remote cache memberVMs
    addMemberNotificationListener(this.managerVM, 3); // TODO: why?

    for (VM memberVM : this.memberVMs) {
      createPartitionRegion_tmp(memberVM, PARTITIONED_REGION_NAME);
      verifyPartitionRegionAfterCreate(memberVM);
    }

    verifyRemotePartitionRegion(this.managerVM);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, PARTITIONED_REGION_PATH);
      verifyPartitionRegionAfterClose(memberVM);
    }

    verifyMemberNotifications(this.managerVM, PARTITIONED_REGION_NAME, 3);
  }

  /**
   * Tests all Region MBean related Management APIs
   * <p>
   * a) Notification propagated to member MBean while a region is created
   * <p>
   * b) Creates and check a Fixed Partitioned Region
   */
  @Test
  public void testFixedPRRegionMBean() throws Exception {
    createMembersAndThenManagers_tmp();

    // Adding notification listener for remote cache memberVMs
    addMemberNotificationListener(this.managerVM, 3); // TODO: why?

    int primaryIndex = 0;
    for (VM memberVM : this.memberVMs) {
      List<FixedPartitionAttributes> fixedPartitionAttributesList =
          createFixedPartitionList(primaryIndex + 1);
      memberVM.invoke(() -> createFixedPartitionRegion(fixedPartitionAttributesList));
      primaryIndex++;
    }

    verifyRemoteFixedPartitionRegion(this.managerVM);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, FIXED_PR_PATH);
    }

    verifyMemberNotifications(this.managerVM, FIXED_PR_PATH, 3);
  }

  /**
   * Tests a Distributed Region at Managing Node side while region is created in a member node
   * asynchronously.
   */
  @Test
  public void testRegionAggregate() throws Exception {
    createManagersAndThenMembers_tmp();

    // Adding notification listener for remote cache memberVMs
    addSystemNotificationListener(this.managerVM); // TODO: why?

    for (VM memberVM : this.memberVMs) {
      createDistributedRegion_tmp(memberVM, REGION_NAME);
    }

    verifyDistributedMBean(this.managerVM, 3);
    createDistributedRegion_tmp(this.managerVM, REGION_NAME);
    verifyDistributedMBean(this.managerVM, 4);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, REGION_PATH);
    }

    verifyProxyCleanup(this.managerVM);

    verifyDistributedMBean(this.managerVM, 1);
    closeRegion(this.managerVM, REGION_PATH);
    verifyDistributedMBean(this.managerVM, 0);

    verifySystemNotifications(this.managerVM, REGION_NAME, 3);
  }

  @Test
  public void testNavigationAPIS() throws Exception {
    createManagersAndThenMembers_tmp();

    for (VM memberVM : this.memberVMs) {
      createDistributedRegion_tmp(memberVM, REGION_NAME);
      createPartitionRegion_tmp(memberVM, PARTITIONED_REGION_NAME);
    }

    createDistributedRegion_tmp(this.managerVM, REGION_NAME);
    createPartitionRegion_tmp(this.managerVM, PARTITIONED_REGION_NAME);
    List<String> memberIds = new ArrayList<>();

    for (VM memberVM : this.memberVMs) {
      memberIds.add(getDistributedMemberId_tmp(memberVM));
    }

    verifyNavigationApis(this.managerVM, memberIds);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, REGION_PATH);
    }
    closeRegion(this.managerVM, REGION_PATH);
  }

  @Test
  public void testSubRegions() throws Exception {
    createMembersAndThenManagers_tmp();

    for (VM memberVM : this.memberVMs) {
      createLocalRegion_tmp(memberVM, LOCAL_REGION_NAME);
      createSubRegion_tmp(memberVM, LOCAL_REGION_NAME, LOCAL_SUB_REGION_NAME);
    }

    for (VM memberVM : this.memberVMs) {
      verifySubRegions(memberVM, LOCAL_SUB_REGION_PATH);
    }

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, LOCAL_REGION_NAME);
      verifyNullRegions(memberVM, LOCAL_SUB_REGION_NAME);
    }
  }

  @Test
  public void testSpecialRegions() throws Exception {
    createMembersAndThenManagers_tmp();
    createSpecialRegion(this.memberVMs[0]);
    verifySpecialRegion(this.managerVM);
  }

  @Test
  public void testLruStats() throws Exception {
    createMembersAndThenManagers_tmp();
    for (VM memberVM : this.memberVMs) {
      createDiskRegion(memberVM);
    }
    verifyEntrySize(this.managerVM, 3);
  }

  private void createMembersAndThenManagers_tmp() throws Exception {
    initManagement(false);
  }

  private void createManagersAndThenMembers_tmp() throws Exception {
    initManagement(true);
  }

  private void disconnectAllFromDS_tmp() {
    disconnectAllFromDS();
  }

  private ManagementService getManagementService_tmp() {
    return getManagementService();
  }

  private Cache getCache_tmp() {
    return getCache();
  }

  private void closeRegion(final VM anyVM, final String regionPath) {
    anyVM.invoke("closeRegion", () -> getCache_tmp().getRegion(regionPath).close());
  }

  private void createSpecialRegion(final VM memberVM) {
    memberVM.invoke("createSpecialRegion", () -> {
      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setValueConstraint(Portfolio.class);
      RegionAttributes regionAttributes = attributesFactory.create();

      Cache cache = getCache_tmp();
      cache.createRegion("p-os", regionAttributes);
      cache.createRegion("p_os", regionAttributes);
    });
  }

  private void verifySpecialRegion(final VM managerVM) {
    managerVM.invoke("verifySpecialRegion", () -> {
      awaitDistributedRegionMXBean("/p-os", 1); // TODO: why?
      awaitDistributedRegionMXBean("/p_os", 1);
    });
  }

  private void createDiskRegion(final VM memberVM) {
    memberVM.invoke("createDiskRegion", () -> {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(20,
          new TestObjectSizerImpl(), EvictionAction.LOCAL_DESTROY));

      Region region = getCache_tmp().createRegion(REGION_NAME, factory.create());

      LRUStatistics lruStats =
          ((AbstractRegion) region).getEvictionController().getLRUHelper().getStats();
      assertThat(lruStats).isNotNull();

      RegionMXBean regionMXBean = getManagementService_tmp().getLocalRegionMBean(REGION_PATH);
      assertThat(regionMXBean).isNotNull();

      int total;
      for (total = 0; total < 100; total++) { // TODO: why so many?
        int[] array = new int[250];
        array[0] = total;
        region.put(new Integer(total), array);
      }
      assertThat(regionMXBean.getEntrySize()).isGreaterThan(0);
    });
  }

  private void verifyEntrySize(final VM managerVM, final int expectedMembers) {
    managerVM.invoke("verifyEntrySize", () -> {
      DistributedRegionMXBean distributedRegionMXBean =
          awaitDistributedRegionMXBean(REGION_PATH, expectedMembers);
      assertThat(distributedRegionMXBean).isNotNull();
      assertThat(distributedRegionMXBean.getEntrySize()).isGreaterThan(0);
    });
  }

  private void verifySubRegions(final VM memberVM, final String subRegionPath) {
    memberVM.invoke("verifySubRegions", () -> {
      RegionMXBean regionMXBean = getManagementService_tmp().getLocalRegionMBean(subRegionPath);
      assertThat(regionMXBean).isNotNull();
    });
  }

  private void verifyNullRegions(final VM memberVM, final String subRegionPath) {
    memberVM.invoke("verifyNullRegions", () -> {
      RegionMXBean regionMXBean = getManagementService_tmp().getLocalRegionMBean(subRegionPath);
      assertThat(regionMXBean).isNull();
    });
  }

  private void verifyNavigationApis(final VM managerVM, final List<String> memberIds) {
    managerVM.invoke("verifyNavigationApis", () -> {
      ManagementService service = getManagementService_tmp();
      assertThat(service.getDistributedSystemMXBean()).isNotNull();

      awaitMemberCount(4);

      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
      assertThat(distributedSystemMXBean.listDistributedRegionObjectNames()).hasSize(2);

      assertThat(distributedSystemMXBean.fetchDistributedRegionObjectName(PARTITIONED_REGION_PATH))
          .isNotNull();
      assertThat(distributedSystemMXBean.fetchDistributedRegionObjectName(REGION_PATH)).isNotNull();

      ObjectName actualName =
          distributedSystemMXBean.fetchDistributedRegionObjectName(PARTITIONED_REGION_PATH);
      ObjectName expectedName = getDistributedRegionMbeanName(PARTITIONED_REGION_PATH);
      assertThat(actualName).isEqualTo(expectedName);

      actualName = distributedSystemMXBean.fetchDistributedRegionObjectName(REGION_PATH);
      expectedName = getDistributedRegionMbeanName(REGION_PATH);
      assertThat(actualName).isEqualTo(expectedName);

      for (String memberId : memberIds) {
        ObjectName objectName = getMemberMBeanName(memberId);
        awaitMemberMXBeanProxy(objectName);

        ObjectName[] objectNames = distributedSystemMXBean.fetchRegionObjectNames(objectName);
        assertThat(objectNames).isNotNull();
        assertThat(objectNames).hasSize(2);

        List<ObjectName> listOfNames = Arrays.asList(objectNames);

        expectedName = getRegionMBeanName(memberId, PARTITIONED_REGION_PATH);
        assertThat(listOfNames).contains(expectedName);

        expectedName = getRegionMBeanName(memberId, REGION_PATH);
        assertThat(listOfNames).contains(expectedName);
      }

      for (String memberId : memberIds) {
        ObjectName objectName = getMemberMBeanName(memberId);
        awaitMemberMXBeanProxy(objectName);

        expectedName = getRegionMBeanName(memberId, PARTITIONED_REGION_PATH);
        awaitRegionMXBeanProxy(expectedName);

        actualName =
            distributedSystemMXBean.fetchRegionObjectName(memberId, PARTITIONED_REGION_PATH);
        assertThat(actualName).isEqualTo(expectedName);

        expectedName = getRegionMBeanName(memberId, REGION_PATH);
        awaitRegionMXBeanProxy(expectedName);

        actualName = distributedSystemMXBean.fetchRegionObjectName(memberId, REGION_PATH);
        assertThat(actualName).isEqualTo(expectedName);
      }
    });
  }

  /**
   * Invoked in controller VM
   */
  private List<FixedPartitionAttributes> createFixedPartitionList(final int primaryIndex) {
    List<FixedPartitionAttributes> fixedPartitionAttributesList = new ArrayList<>();
    if (primaryIndex == 1) {
      fixedPartitionAttributesList
          .add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 2) {
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fixedPartitionAttributesList
          .add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 3) {
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fixedPartitionAttributesList
          .add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    }
    return fixedPartitionAttributesList;
  }

  /**
   * Invoked in member VMs
   */
  private void createFixedPartitionRegion(
      final List<FixedPartitionAttributes> fixedPartitionAttributesList) {
    SystemManagementService service = getSystemManagementService_tmp();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();

    partitionAttributesFactory.setRedundantCopies(2).setTotalNumBuckets(12);
    for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
      partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
    }
    partitionAttributesFactory.setPartitionResolver(new SingleHopQuarterPartitionResolver());

    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(partitionAttributesFactory.create());

    fixedPartitionedRegion = getCache_tmp().createRegion(FIXED_PR_NAME, attributesFactory.create());
    assertThat(fixedPartitionedRegion).isNotNull();

    RegionMXBean regionMXBean = service.getLocalRegionMBean(FIXED_PR_PATH);
    RegionAttributes regionAttributes = fixedPartitionedRegion.getAttributes();

    PartitionAttributesData partitionAttributesData = regionMXBean.listPartitionAttributes();
    verifyPartitionData(regionAttributes, partitionAttributesData);

    FixedPartitionAttributesData[] fixedPartitionAttributesData =
        regionMXBean.listFixedPartitionAttributes();
    assertThat(fixedPartitionAttributesData).isNotNull();
    assertThat(fixedPartitionAttributesData).hasSize(3);

    for (int i = 0; i < fixedPartitionAttributesData.length; i++) {
      // TODO: add real assertions
      // LogWriterUtils.getLogWriter().info("<ExpectedString> Fixed PR Data is " +
      // fixedPartitionAttributesData[i] + "</ExpectedString> ");
    }
  }

  private void addMemberNotificationListener(final VM managerVM, final int expectedMembers) {
    managerVM.invoke("addMemberNotificationListener", () -> {
      Set<DistributedMember> otherMemberSet = getOtherNormalMembers_tmp();
      assertThat(otherMemberSet).hasSize(expectedMembers);

      SystemManagementService service = getSystemManagementService_tmp();

      List<Notification> notifications = new ArrayList<>();
      MEMBER_NOTIFICATIONS_REF.set(notifications);

      for (DistributedMember member : otherMemberSet) {
        MemberNotificationListener listener = new MemberNotificationListener(notifications);
        ObjectName objectName = service.getMemberMBeanName(member);
        awaitMemberMXBeanProxy(objectName);

        getPlatformMBeanServer().addNotificationListener(objectName, listener, null, null);
      }
    });
  }

  /**
   * Add a Notification listener to DistributedSystemMBean which should gather all the notifications
   * which are propagated through all individual MemberMBeans Hence Region created/destroyed should
   * be visible to this listener
   */
  private void addSystemNotificationListener(final VM managerVM) {
    managerVM.invoke("addSystemNotificationListener", () -> {
      awaitDistributedSystemMXBean();

      List<Notification> notifications = new ArrayList<>();
      SYSTEM_NOTIFICATIONS_REF.set(notifications);

      DistributedSystemNotificationListener listener =
          new DistributedSystemNotificationListener(notifications);
      ObjectName objectName = MBeanJMXAdapter.getDistributedSystemName();
      getPlatformMBeanServer().addNotificationListener(objectName, listener, null, null);
    });
  }

  private void verifyMemberNotifications(final VM managerVM, final String regionName,
      final int expectedMembers) {
    managerVM.invoke("verifyMemberNotifications", () -> {
      assertThat(MEMBER_NOTIFICATIONS_REF.get()).isNotNull();
      assertThat(MEMBER_NOTIFICATIONS_REF.get()).hasSize(expectedMembers * 2);

      int regionCreatedCount = 0;
      int regionDestroyedCount = 0;
      for (Notification notification : MEMBER_NOTIFICATIONS_REF.get()) {
        if (JMXNotificationType.REGION_CREATED.equals(notification.getType())) {
          regionCreatedCount++;
          assertThat(notification.getMessage()).contains(regionName);
        } else if (JMXNotificationType.REGION_CLOSED.equals(notification.getType())) {
          regionDestroyedCount++;
          assertThat(notification.getMessage()).contains(regionName);
        } else {
          fail("Unexpected notification type: " + notification.getType());
        }
      }

      assertThat(regionCreatedCount).isEqualTo(expectedMembers);
      assertThat(regionDestroyedCount).isEqualTo(expectedMembers);
    });
  }

  // <[javax.management.Notification[source=10.118.33.232(17632)<v1>-32770][type=gemfire.distributedsystem.cache.region.created][message=Region
  // Created With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=10.118.33.232(17633)<v2>-32771][type=gemfire.distributedsystem.cache.region.created][message=Region
  // Created With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=10.118.33.232(17634)<v3>-32772][type=gemfire.distributedsystem.cache.region.created][message=Region
  // Created With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=10.118.33.232(17632)<v1>-32770][type=gemfire.distributedsystem.cache.region.closed][message=Region
  // Destroyed/Closed With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=10.118.33.232(17633)<v2>-32771][type=gemfire.distributedsystem.cache.region.closed][message=Region
  // Destroyed/Closed With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=10.118.33.232(17634)<v3>-32772][type=gemfire.distributedsystem.cache.region.closed][message=Region
  // Destroyed/Closed With Name /MANAGEMENT_TEST_REGION]]>

  private void verifySystemNotifications(final VM managerVM, final String regionName,
      final int expectedMembers) {
    managerVM.invoke("verifySystemNotifications", () -> {
      assertThat(SYSTEM_NOTIFICATIONS_REF.get()).isNotNull();
      assertThat(SYSTEM_NOTIFICATIONS_REF.get()).hasSize(expectedMembers + 2); // 2 for the manager


      int regionCreatedCount = 0;
      int regionDestroyedCount = 0;
      for (Notification notification : SYSTEM_NOTIFICATIONS_REF.get()) {
        if (JMXNotificationType.REGION_CREATED.equals(notification.getType())) {
          regionCreatedCount++;
          assertThat(notification.getMessage()).contains(regionName);
        } else if (JMXNotificationType.REGION_CLOSED.equals(notification.getType())) {
          regionDestroyedCount++;
          assertThat(notification.getMessage()).contains(regionName);
        } else {
          fail("Unexpected notification type: " + notification.getType());
        }
      }

      assertThat(regionCreatedCount).isEqualTo(1); // just the manager
      assertThat(regionDestroyedCount).isEqualTo(expectedMembers + 1); // all 3 members + manager
    });
  }

  // <[javax.management.Notification[source=192.168.1.72(18496)<v27>-32770][type=gemfire.distributedsystem.cache.region.created][message=Region
  // Created With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=192.168.1.72(18497)<v28>-32771][type=gemfire.distributedsystem.cache.region.closed][message=Region
  // Destroyed/Closed With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=192.168.1.72(18498)<v29>-32772][type=gemfire.distributedsystem.cache.region.closed][message=Region
  // Destroyed/Closed With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=192.168.1.72(18499)<v30>-32773][type=gemfire.distributedsystem.cache.region.closed][message=Region
  // Destroyed/Closed With Name /MANAGEMENT_TEST_REGION],
  // javax.management.Notification[source=192.168.1.72(18496)<v27>-32770][type=gemfire.distributedsystem.cache.region.closed][message=Region
  // Destroyed/Closed With Name /MANAGEMENT_TEST_REGION]]>

  private void verifyProxyCleanup(final VM managerVM) {
    managerVM.invoke("verifyProxyCleanup", () -> {
      SystemManagementService service = getSystemManagementService_tmp();

      Set<DistributedMember> otherMemberSet = getOtherNormalMembers_tmp();
      for (final DistributedMember member : otherMemberSet) {
        String alias = "Waiting for the proxy to get deleted at managing node";
        await(alias).until(
            () -> assertThat(service.getMBeanProxy(service.getRegionMBeanName(member, REGION_PATH),
                RegionMXBean.class)).isNull());
      }
    });
  }

  private void verifyRemoteDistributedRegion(final VM managerVM, final int expectedMembers) {
    managerVM.invoke("verifyRemoteDistributedRegion", () -> {
      Set<DistributedMember> otherMemberSet = getOtherNormalMembers_tmp();
      assertThat(otherMemberSet).hasSize(expectedMembers);

      for (DistributedMember member : otherMemberSet) {
        RegionMXBean regionMXBean = awaitRegionMXBeanProxy(member, REGION_PATH);

        RegionAttributesData regionAttributesData = regionMXBean.listRegionAttributes();
        assertThat(regionAttributesData).isNotNull();

        MembershipAttributesData membershipAttributesData = regionMXBean.listMembershipAttributes();
        assertThat(membershipAttributesData).isNotNull();

        EvictionAttributesData evictionAttributesData = regionMXBean.listEvictionAttributes();
        assertThat(evictionAttributesData).isNotNull();
      }

      DistributedRegionMXBean distributedRegionMXBean =
          awaitDistributedRegionMXBean(REGION_PATH, expectedMembers);

      assertThat(distributedRegionMXBean).isNotNull();
      assertThat(distributedRegionMXBean.getFullPath()).isEqualTo(REGION_PATH);
    });
  }

  private void verifyDistributedMBean(final VM managerVM, final int expectedMembers) {
    managerVM.invoke("verifyDistributedMBean", () -> {
      if (expectedMembers == 0) {
        ManagementService service = getManagementService_tmp();
        String alias = "Waiting for the proxy to get deleted at managing node";
        await(alias)
            .until(() -> assertThat(service.getDistributedRegionMXBean(REGION_PATH)).isNull());
        return;
      }

      DistributedRegionMXBean distributedRegionMXBean =
          awaitDistributedRegionMXBean(REGION_PATH, expectedMembers);

      assertThat(distributedRegionMXBean.getFullPath()).isEqualTo(REGION_PATH);
      assertThat(distributedRegionMXBean.getMemberCount()).isEqualTo(expectedMembers);
      assertThat(distributedRegionMXBean.getMembers()).hasSize(expectedMembers);

      // Check Stats related Data
      // LogWriterUtils.getLogWriter().info("<ExpectedString> CacheListenerCallsAvgLatency is " +
      // distributedRegionMXBean.getCacheListenerCallsAvgLatency() + "</ExpectedString> ");
      // LogWriterUtils.getLogWriter().info("<ExpectedString> CacheWriterCallsAvgLatency is " +
      // distributedRegionMXBean.getCacheWriterCallsAvgLatency() + "</ExpectedString> ");
      // LogWriterUtils.getLogWriter().info("<ExpectedString> CreatesRate is " +
      // distributedRegionMXBean.getCreatesRate() + "</ExpectedString> ");
    });
  }

  private void verifyRemotePartitionRegion(final VM managerVM) {
    managerVM.invoke("verifyRemotePartitionRegion", () -> {
      Set<DistributedMember> otherMemberSet = getOtherNormalMembers_tmp();

      for (DistributedMember member : otherMemberSet) {
        RegionMXBean regionMXBean = awaitRegionMXBeanProxy(member, PARTITIONED_REGION_PATH);
        PartitionAttributesData partitionAttributesData = regionMXBean.listPartitionAttributes();
        assertThat(partitionAttributesData).isNotNull();
      }

      ManagementService service = getManagementService_tmp();
      DistributedRegionMXBean distributedRegionMXBean =
          service.getDistributedRegionMXBean(PARTITIONED_REGION_PATH);
      assertThat(distributedRegionMXBean.getMembers()).hasSize(3);
    });
  }

  private void verifyReplicateRegionAfterCreate(final VM memberVM) {
    memberVM.invoke("verifyReplicateRegionAfterCreate", () -> {
      Cache cache = getCache_tmp();

      String memberId =
          MBeanJMXAdapter.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
      ObjectName objectName = ObjectName.getInstance("GemFire:type=Member,member=" + memberId);

      // List<Notification> notifications = new ArrayList<>();
      // MEMBER_NOTIFICATIONS_REF.set(notifications);
      //
      // MemberNotificationListener listener = new MemberNotificationListener(notifications);
      // ManagementFactory.getPlatformMBeanServer().addNotificationListener(objectName, listener,
      // null, null);

      SystemManagementService service = getSystemManagementService_tmp();
      RegionMXBean regionMXBean = service.getLocalRegionMBean(REGION_PATH);
      assertThat(regionMXBean).isNotNull();

      Region region = cache.getRegion(REGION_PATH);
      RegionAttributes regionAttributes = region.getAttributes();

      RegionAttributesData regionAttributesData = regionMXBean.listRegionAttributes();
      verifyRegionAttributes(regionAttributes, regionAttributesData);

      MembershipAttributesData membershipData = regionMXBean.listMembershipAttributes();
      assertThat(membershipData).isNotNull();

      EvictionAttributesData evictionData = regionMXBean.listEvictionAttributes();
      assertThat(evictionData).isNotNull();
    });
  }

  private void verifyPartitionRegionAfterCreate(final VM memberVM) {
    memberVM.invoke("verifyPartitionRegionAfterCreate", () -> {
      Region region = getCache_tmp().getRegion(PARTITIONED_REGION_PATH);

      SystemManagementService service = getSystemManagementService_tmp();
      RegionMXBean regionMXBean = service.getLocalRegionMBean(PARTITIONED_REGION_PATH);

      verifyPartitionData(region.getAttributes(), regionMXBean.listPartitionAttributes());
    });
  }

  private void verifyReplicatedRegionAfterClose(final VM memberVM) {
    memberVM.invoke("verifyReplicatedRegionAfterClose", () -> {
      SystemManagementService service = getSystemManagementService_tmp();
      RegionMXBean regionMXBean = service.getLocalRegionMBean(REGION_PATH);
      assertThat(regionMXBean).isNull();

      ObjectName objectName = service.getRegionMBeanName(
          getCache_tmp().getDistributedSystem().getDistributedMember(), REGION_PATH);
      assertThat(service.getLocalManager().getManagementResourceRepo()
          .getEntryFromLocalMonitoringRegion(objectName)).isNull();
    });
  }

  private void verifyPartitionRegionAfterClose(final VM memberVM) {
    memberVM.invoke("verifyPartitionRegionAfterClose", () -> {
      ManagementService service = getManagementService_tmp();
      RegionMXBean regionMXBean = service.getLocalRegionMBean(PARTITIONED_REGION_PATH);
      assertThat(regionMXBean).isNull();
    });
  }

  /**
   * Invoked in member VMs
   */
  private void verifyPartitionData(final RegionAttributes expectedRegionAttributes,
      final PartitionAttributesData partitionAttributesData) {
    PartitionAttributes expectedPartitionAttributes =
        expectedRegionAttributes.getPartitionAttributes();

    assertThat(partitionAttributesData.getRedundantCopies())
        .isEqualTo(expectedPartitionAttributes.getRedundantCopies());

    assertThat(partitionAttributesData.getTotalMaxMemory())
        .isEqualTo(expectedPartitionAttributes.getTotalMaxMemory());

    // Total number of buckets for whole region
    assertThat(partitionAttributesData.getTotalNumBuckets())
        .isEqualTo(expectedPartitionAttributes.getTotalNumBuckets());

    assertThat(partitionAttributesData.getLocalMaxMemory())
        .isEqualTo(expectedPartitionAttributes.getLocalMaxMemory());

    assertThat(partitionAttributesData.getColocatedWith())
        .isEqualTo(expectedPartitionAttributes.getColocatedWith());

    String partitionResolver = null;
    // TODO: these conditionals should be deterministic
    if (expectedPartitionAttributes.getPartitionResolver() != null) {
      partitionResolver = expectedPartitionAttributes.getPartitionResolver().getName();
    }
    assertThat(partitionAttributesData.getPartitionResolver()).isEqualTo(partitionResolver);

    assertThat(partitionAttributesData.getRecoveryDelay())
        .isEqualTo(expectedPartitionAttributes.getRecoveryDelay());

    assertThat(partitionAttributesData.getStartupRecoveryDelay())
        .isEqualTo(expectedPartitionAttributes.getStartupRecoveryDelay());

    if (expectedPartitionAttributes.getPartitionListeners() != null) {
      for (int i = 0; i < expectedPartitionAttributes.getPartitionListeners().length; i++) {
        // assertEquals((expectedPartitionAttributes.getPartitionListeners())[i].getClass().getCanonicalName(),
        // partitionAttributesData.getPartitionListeners()[i]);
        assertThat(partitionAttributesData.getPartitionListeners()[i]).isEqualTo(
            expectedPartitionAttributes.getPartitionListeners()[i].getClass().getCanonicalName());
      }
    }
  }

  /**
   * Invoked in member VMs
   */
  private void verifyRegionAttributes(final RegionAttributes regionAttributes,
      final RegionAttributesData regionAttributesData) {
    String compressorClassName = null;
    // TODO: these conditionals should be deterministic
    if (regionAttributes.getCompressor() != null) {
      compressorClassName = regionAttributes.getCompressor().getClass().getCanonicalName();
    }
    assertThat(regionAttributesData.getCompressorClassName()).isEqualTo(compressorClassName);

    String cacheLoaderClassName = null;
    if (regionAttributes.getCacheLoader() != null) {
      cacheLoaderClassName = regionAttributes.getCacheLoader().getClass().getCanonicalName();
    }
    assertThat(regionAttributesData.getCacheLoaderClassName()).isEqualTo(cacheLoaderClassName);

    String cacheWriteClassName = null;
    if (regionAttributes.getCacheWriter() != null) {
      cacheWriteClassName = regionAttributes.getCacheWriter().getClass().getCanonicalName();
    }
    assertThat(regionAttributesData.getCacheWriterClassName()).isEqualTo(cacheWriteClassName);

    String keyConstraintClassName = null;
    if (regionAttributes.getKeyConstraint() != null) {
      keyConstraintClassName = regionAttributes.getKeyConstraint().getName();
    }
    assertThat(regionAttributesData.getKeyConstraintClassName()).isEqualTo(keyConstraintClassName);

    String valueContstaintClassName = null;
    if (regionAttributes.getValueConstraint() != null) {
      valueContstaintClassName = regionAttributes.getValueConstraint().getName();
    }
    assertThat(regionAttributesData.getValueConstraintClassName())
        .isEqualTo(valueContstaintClassName);

    CacheListener[] listeners = regionAttributes.getCacheListeners();
    if (listeners != null) {
      String[] value = regionAttributesData.getCacheListeners();
      for (int i = 0; i < listeners.length; i++) {
        assertThat(listeners[i].getClass().getName()).isEqualTo(value[i]);
      }
    }

    assertThat(regionAttributesData.getRegionTimeToLive())
        .isEqualTo(regionAttributes.getRegionTimeToLive().getTimeout());

    assertThat(regionAttributesData.getRegionIdleTimeout())
        .isEqualTo(regionAttributes.getRegionIdleTimeout().getTimeout());

    assertThat(regionAttributesData.getEntryTimeToLive())
        .isEqualTo(regionAttributes.getEntryTimeToLive().getTimeout());

    assertThat(regionAttributesData.getEntryIdleTimeout())
        .isEqualTo(regionAttributes.getEntryIdleTimeout().getTimeout());

    String customEntryTimeToLive = null;
    Object o1 = regionAttributes.getCustomEntryTimeToLive();
    if (o1 != null) {
      customEntryTimeToLive = o1.toString();
    }
    assertThat(regionAttributesData.getCustomEntryTimeToLive()).isEqualTo(customEntryTimeToLive);

    String customEntryIdleTimeout = null;
    Object o2 = regionAttributes.getCustomEntryIdleTimeout();
    if (o2 != null) {
      customEntryIdleTimeout = o2.toString();
    }
    assertThat(regionAttributesData.getCustomEntryIdleTimeout()).isEqualTo(customEntryIdleTimeout);

    assertThat(regionAttributesData.isIgnoreJTA()).isEqualTo(regionAttributes.getIgnoreJTA());

    assertThat(regionAttributesData.getDataPolicy())
        .isEqualTo(regionAttributes.getDataPolicy().toString());

    assertThat(regionAttributesData.getScope()).isEqualTo(regionAttributes.getScope().toString());

    assertThat(regionAttributesData.getInitialCapacity())
        .isEqualTo(regionAttributes.getInitialCapacity());

    assertThat(regionAttributesData.getLoadFactor()).isEqualTo(regionAttributes.getLoadFactor());

    assertThat(regionAttributesData.isLockGrantor()).isEqualTo(regionAttributes.isLockGrantor());

    assertThat(regionAttributesData.isMulticastEnabled())
        .isEqualTo(regionAttributes.getMulticastEnabled());

    assertThat(regionAttributesData.getConcurrencyLevel())
        .isEqualTo(regionAttributes.getConcurrencyLevel());

    assertThat(regionAttributesData.isIndexMaintenanceSynchronous())
        .isEqualTo(regionAttributes.getIndexMaintenanceSynchronous());

    assertThat(regionAttributesData.isStatisticsEnabled())
        .isEqualTo(regionAttributes.getStatisticsEnabled());

    assertThat(regionAttributesData.isSubscriptionConflationEnabled())
        .isEqualTo(regionAttributes.getEnableSubscriptionConflation());

    assertThat(regionAttributesData.isAsyncConflationEnabled())
        .isEqualTo(regionAttributes.getEnableAsyncConflation());

    assertThat(regionAttributesData.getPoolName()).isEqualTo(regionAttributes.getPoolName());

    assertThat(regionAttributesData.isCloningEnabled())
        .isEqualTo(regionAttributes.getCloningEnabled());

    assertThat(regionAttributesData.getDiskStoreName())
        .isEqualTo(regionAttributes.getDiskStoreName());

    String interestPolicy = null;
    if (regionAttributes.getSubscriptionAttributes() != null) {
      interestPolicy = regionAttributes.getSubscriptionAttributes().getInterestPolicy().toString();
    }
    assertThat(regionAttributesData.getInterestPolicy()).isEqualTo(interestPolicy);

    assertThat(regionAttributesData.isDiskSynchronous())
        .isEqualTo(regionAttributes.isDiskSynchronous());
  }

  private void verifyRemoteFixedPartitionRegion(final VM managerVM) {
    managerVM.invoke("Verify Partition region", () -> {
      Set<DistributedMember> otherMemberSet = getOtherNormalMembers_tmp();

      for (DistributedMember member : otherMemberSet) {
        RegionMXBean bean = awaitRegionMXBeanProxy(member, FIXED_PR_PATH);

        PartitionAttributesData data = bean.listPartitionAttributes();
        assertThat(data).isNotNull();

        FixedPartitionAttributesData[] fixedPrData = bean.listFixedPartitionAttributes();
        assertThat(fixedPrData).isNotNull();
        assertThat(fixedPrData).hasSize(3);

        for (int i = 0; i < fixedPrData.length; i++) {
          // TODO: add real assertions
          // LogWriterUtils.getLogWriter().info("<ExpectedString> Remote PR Data is " +
          // fixedPrData[i] + "</ExpectedString> ");
        }
      }
    });
  }

  private void createDistributedRegion_tmp(final VM vm, final String regionName) {
    vm.invoke(() -> createDistributedRegion_tmp(regionName));
  }

  private void createDistributedRegion_tmp(final String regionName) {
    getCache_tmp().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
  }

  private void createPartitionRegion_tmp(final VM vm, final String partitionRegionName) {
    vm.invoke("Create Partitioned region", () -> {
      SystemManagementService service = getSystemManagementService_tmp();
      RegionFactory regionFactory =
          getCache_tmp().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
      regionFactory.create(partitionRegionName);
    });
  }

  private void createLocalRegion_tmp(final VM vm, final String localRegionName) {
    vm.invoke("Create Local region", () -> {
      SystemManagementService service = getSystemManagementService_tmp();
      RegionFactory regionFactory = getCache_tmp().createRegionFactory(RegionShortcut.LOCAL);
      regionFactory.create(localRegionName);
    });
  }

  private void createSubRegion_tmp(final VM vm, final String parentRegionPath,
      final String subregionName) {
    vm.invoke("Create Sub region", () -> {
      SystemManagementService service = getSystemManagementService_tmp();
      Region region = getCache_tmp().getRegion(parentRegionPath);
      region.createSubregion(subregionName, region.getAttributes());
    });
  }

  private String getDistributedMemberId_tmp(final VM vm) {
    return vm.invoke("getMemberId",
        () -> getCache_tmp().getDistributedSystem().getDistributedMember().getId());
  }

  private DistributedMember getDistributedMember_tmp(final VM anyVM) {
    return anyVM.invoke("getDistributedMember_tmp",
        () -> getCache_tmp().getDistributedSystem().getDistributedMember());
  }

  private SystemManagementService getSystemManagementService_tmp() {
    return (SystemManagementService) getManagementService_tmp();
  }

  private DM getDistributionManager_tmp() {
    return ((GemFireCacheImpl) getCache_tmp()).getDistributionManager();
  }

  private DistributedMember getDistributedMember_tmp() {
    return getCache_tmp().getDistributedSystem().getDistributedMember();
  }

  private Set<DistributedMember> getOtherNormalMembers_tmp() {
    Set<DistributedMember> allMembers =
        new HashSet<>(getDistributionManager_tmp().getNormalDistributionManagerIds());
    allMembers.remove(getDistributedMember_tmp());
    return allMembers;
  }

  private void awaitMemberCount(final int expectedCount) {
    DistributedSystemMXBean distributedSystemMXBean = awaitDistributedSystemMXBean();
    await()
        .until(() -> assertThat(distributedSystemMXBean.getMemberCount()).isEqualTo(expectedCount));
  }

  private DistributedRegionMXBean awaitDistributedRegionMXBean(final String name) {
    SystemManagementService service = getSystemManagementService_tmp();

    await().until(() -> assertThat(service.getDistributedRegionMXBean(name)).isNotNull());

    return service.getDistributedRegionMXBean(name);
  }

  private DistributedRegionMXBean awaitDistributedRegionMXBean(final String name,
      final int memberCount) {
    SystemManagementService service = getSystemManagementService_tmp();

    await().until(() -> assertThat(service.getDistributedRegionMXBean(name)).isNotNull());
    await().until(() -> assertThat(service.getDistributedRegionMXBean(name).getMemberCount())
        .isEqualTo(memberCount));

    return service.getDistributedRegionMXBean(name);
  }

  private RegionMXBean awaitRegionMXBeanProxy(final DistributedMember member, final String name) {
    SystemManagementService service = getSystemManagementService_tmp();
    ObjectName objectName = service.getRegionMBeanName(member, name);
    String alias = "awaiting RegionMXBean proxy for " + member;

    await(alias)
        .until(() -> assertThat(service.getMBeanProxy(objectName, RegionMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, RegionMXBean.class);
  }

  private RegionMXBean awaitRegionMXBeanProxy(final ObjectName objectName) {
    SystemManagementService service = getSystemManagementService_tmp();

    await()
        .until(() -> assertThat(service.getMBeanProxy(objectName, RegionMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, RegionMXBean.class);
  }

  private MemberMXBean awaitMemberMXBeanProxy(final DistributedMember member) {
    SystemManagementService service = getSystemManagementService_tmp();
    ObjectName objectName = service.getMemberMBeanName(member);
    String alias = "awaiting MemberMXBean proxy for " + member;

    await(alias)
        .until(() -> assertThat(service.getMBeanProxy(objectName, MemberMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, MemberMXBean.class);
  }

  private MemberMXBean awaitMemberMXBeanProxy(final ObjectName objectName) {
    SystemManagementService service = getSystemManagementService_tmp();
    await()
        .until(() -> assertThat(service.getMBeanProxy(objectName, MemberMXBean.class)).isNotNull());
    return service.getMBeanProxy(objectName, MemberMXBean.class);
  }

  private DistributedSystemMXBean awaitDistributedSystemMXBean() {
    ManagementService service = getSystemManagementService_tmp();

    await().until(() -> assertThat(service.getDistributedSystemMXBean()).isNotNull());

    return service.getDistributedSystemMXBean();
  }

  private ConditionFactory await() {
    return Awaitility.await().atMost(2, MINUTES);
  }

  private ConditionFactory await(final String alias) {
    return Awaitility.await(alias).atMost(2, MINUTES);
  }

  /**
   * Registered in manager VM
   *
   * User defined notification handler for Region creation handling
   */
  private static class MemberNotificationListener implements NotificationListener {

    private final List<Notification> notifications;

    private MemberNotificationListener(List<Notification> notifications) {
      this.notifications = notifications;
    }

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
      assertThat(notification).isNotNull();

      assertThat(JMXNotificationType.REGION_CREATED.equals(notification.getType())
          || JMXNotificationType.REGION_CLOSED.equals(notification.getType())).isTrue();

      notifications.add(notification);

      // TODO: add better validation
      // LogWriterUtils.getLogWriter().info("<ExpectedString> Member Level Notifications" +
      // notification + "</ExpectedString> ");
    }
  }

  /**
   * Registered in manager VM
   *
   * User defined notification handler for Region creation handling
   */
  private static class DistributedSystemNotificationListener implements NotificationListener {

    private final List<Notification> notifications;

    private DistributedSystemNotificationListener(List<Notification> notifications) {
      this.notifications = notifications;
    }

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
      assertThat(notification).isNotNull();

      notifications.add(notification);

      // TODO: add something that will be validated
      // LogWriterUtils.getLogWriter().info("<ExpectedString> Distributed System Notifications" +
      // notification + "</ExpectedString> ");
    }
  }
}
