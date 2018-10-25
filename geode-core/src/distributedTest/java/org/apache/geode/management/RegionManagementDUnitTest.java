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
import static java.util.Calendar.MONTH;
import static org.apache.geode.cache.EvictionAction.LOCAL_DESTROY;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedRegionMbeanName;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getMemberMBeanName;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getRegionMBeanName;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.TestObjectSizerImpl;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for {@link RegionMXBean}.
 *
 * <p>
 * This class checks and verifies various data and operations exposed through RegionMXBean
 * interface.
 *
 * <p>
 * Goal of the Test : RegionMBean gets created once region is created. Data like Region Attributes
 * data and stats are of proper value
 */
@SuppressWarnings("serial")
public class RegionManagementDUnitTest implements Serializable {

  private static final AtomicReference<List<Notification>> MEMBER_NOTIFICATIONS =
      new AtomicReference<>();
  private static final AtomicReference<List<Notification>> SYSTEM_NOTIFICATIONS =
      new AtomicReference<>();

  private String regionName;
  private String partitionedRegionName;
  private String subregionName;

  private VM managerVM;

  private VM[] memberVMs;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    partitionedRegionName = uniqueName + "_partitionedRegion";
    subregionName = uniqueName + "_subregion";

    managerVM = getVM(0);
    VM memberVM1 = getVM(1);
    VM memberVM2 = getVM(2);
    VM memberVM3 = getVM(3);

    memberVM1.invoke(() -> createMember(cacheRule));
    memberVM2.invoke(() -> createMember(cacheRule));
    memberVM3.invoke(() -> createMember(cacheRule));

    managerVM.invoke(() -> createManager(cacheRule));

    memberVMs = toArray(memberVM1, memberVM2, memberVM3);
  }

  @After
  public void tearDown() {
    disconnectAllFromDS();

    MEMBER_NOTIFICATIONS.set(null);
    SYSTEM_NOTIFICATIONS.set(null);
    invokeInEveryVM(() -> MEMBER_NOTIFICATIONS.set(null));
    invokeInEveryVM(() -> SYSTEM_NOTIFICATIONS.set(null));
  }

  /**
   * Tests all Region MBean related Management APIs:
   * <p>
   * a) Notification propagated to member MBean while a region is created<br>
   * b) Creates and check a Distributed Region
   */
  @Test
  public void testDistributedRegion() {
    // Adding notification listener for remote cache memberVMs
    managerVM.invoke(() -> addMemberNotificationListener(3));

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        getCache().createRegionFactory(REPLICATE).create(regionName);
        verifyReplicateRegionAfterCreate(regionName);
      });
    }

    managerVM.invoke(() -> verifyRemoteDistributedRegion(3, regionName));

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        getCache().getRegion(regionName).close();
        verifyReplicatedRegionAfterClose(regionName);
      });
    }

    managerVM.invoke(() -> verifyProxyCleanup(regionName));

    managerVM.invoke(() -> verifyMemberNotifications(regionName, 3));
  }

  /**
   * Tests all Region MBean related Management APIs:
   * <p>
   * a) Notification propagated to member MBean while a region is created<br>
   * b) Created and check a Partitioned Region
   */
  @Test
  public void testPartitionedRegion() {
    // Adding notification listener for remote cache memberVMs
    managerVM.invoke(() -> addMemberNotificationListener(3));

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        getCache().createRegionFactory(PARTITION_REDUNDANT).create(partitionedRegionName);
        verifyPartitionRegionAfterCreate(partitionedRegionName, false);
      });
    }

    managerVM.invoke(() -> verifyRemotePartitionRegion(partitionedRegionName));

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        getCache().getRegion(partitionedRegionName).close();
        verifyPartitionRegionAfterClose(partitionedRegionName);
      });
    }

    managerVM.invoke(() -> verifyMemberNotifications(partitionedRegionName, 3));
  }

  /**
   * Tests all Region MBean related Management APIs:
   * <p>
   * a) Notification propagated to member MBean while a region is created<br>
   * b) Creates and check a Fixed Partitioned Region
   */
  @Test
  public void testFixedPRRegionMBean() {
    // Adding notification listener for remote cache memberVMs
    managerVM.invoke(() -> addMemberNotificationListener(3));

    int primaryIndex = 0;
    for (VM memberVM : memberVMs) {
      List<FixedPartitionAttributes> fixedPartitionAttributesList =
          createFixedPartitionList(primaryIndex + 1);
      memberVM.invoke(
          () -> createFixedPartitionRegion(partitionedRegionName, fixedPartitionAttributesList));
      primaryIndex++;
    }

    managerVM.invoke(() -> verifyRemoteFixedPartitionRegion(partitionedRegionName));

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> getCache().getRegion(partitionedRegionName).close());
    }

    managerVM.invoke(() -> verifyMemberNotifications(partitionedRegionName, 3));
  }

  /**
   * Tests a Distributed Region at Managing Node side while region is created in a member node
   * asynchronously.
   */
  @Test
  public void testRegionAggregate() {
    // Adding notification listener for remote cache memberVMs
    managerVM.invoke(() -> addSystemNotificationListener());

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        getCache().createRegionFactory(REPLICATE).create(regionName);
      });
    }

    managerVM.invoke(() -> {
      verifyDistributedMBean(regionName, 3);
      getCache().createRegionFactory(REPLICATE).create(regionName);
      verifyDistributedMBean(regionName, 4);
    });

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> getCache().getRegion(regionName).close());
    }

    managerVM.invoke(() -> {
      verifyProxyCleanup(regionName);

      verifyDistributedMBean(regionName, 1);
      getCache().getRegion(regionName).close();
      verifyDistributedMBean(regionName, 0);

      // TODO: GEODE-1930: verifySystemNotifications is too flaky and needs to be fixed
      // verifySystemNotifications(regionName, 3);
    });
  }

  @Test
  public void testNavigationAPIS() {
    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        getCache().createRegionFactory(REPLICATE).create(regionName);
        getCache().createRegionFactory(PARTITION_REDUNDANT).create(partitionedRegionName);
      });
    }

    managerVM.invoke(() -> {
      getCache().createRegionFactory(REPLICATE).create(regionName);
      getCache().createRegionFactory(PARTITION_REDUNDANT).create(partitionedRegionName);
    });

    List<String> memberIds = new ArrayList<>();
    for (VM memberVM : memberVMs) {
      memberIds.add(
          memberVM.invoke(() -> getCache().getDistributedSystem().getDistributedMember().getId()));
    }

    managerVM.invoke(() -> verifyNavigationApis(memberIds));
  }

  @Test
  public void testSubRegions() {
    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        Region region = getCache().createRegionFactory(LOCAL).create(regionName);
        getCache().createRegionFactory(LOCAL).createSubregion(region, subregionName);
      });
    }

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> verifyRegionMXBeanIsNotNull(regionName, subregionName));
    }

    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> {
        getCache().getRegion(regionName).close();
        verifyRegionMXBeanIsNull(regionName, subregionName);
      });
    }
  }

  @Test
  public void testSpecialRegions() {
    memberVMs[0].invoke(() -> {
      RegionFactory<?, Portfolio> regionFactory = getCache().createRegionFactory(LOCAL);
      regionFactory.setValueConstraint(Portfolio.class);

      regionFactory.create("p-os");
      regionFactory.create("p_os");
    });

    managerVM.invoke(() -> {
      awaitDistributedRegionMXBean("p-os", 1);
      awaitDistributedRegionMXBean("p_os", 1);
    });
  }

  @Test
  public void testLruStats() {
    for (VM memberVM : memberVMs) {
      memberVM.invoke(() -> createDiskRegion(regionName));
    }

    managerVM.invoke(() -> verifyEntrySize(regionName, 3));
  }

  private InternalCache getCache() {
    return cacheRule.getCache();
  }

  private ManagementService getManagementService() {
    return ManagementService.getManagementService(cacheRule.getCache());
  }

  private SystemManagementService getSystemManagementService() {
    return (SystemManagementService) getManagementService();
  }

  private Set<DistributedMember> getOtherMembers() {
    Set<DistributedMember> allMembers =
        new HashSet<>(getCache().getDistributionManager().getNormalDistributionManagerIds());
    allMembers.remove(getCache().getDistributionManager().getId());
    return allMembers;
  }

  private String toPath(final String regionName) {
    return SEPARATOR + regionName;
  }

  private String toPath(final String regionName, final String subregionName) {
    return SEPARATOR + regionName + SEPARATOR + subregionName;
  }

  private void createDiskRegion(final String regionName) {
    EvictionAttributes evictionAttributes = EvictionAttributes.createLRUMemoryAttributes(20,
        new TestObjectSizerImpl(), LOCAL_DESTROY);

    RegionFactory<Integer, Object> regionFactory = getCache().createRegionFactory(LOCAL);
    regionFactory.setEvictionAttributes(evictionAttributes);

    Region<Integer, Object> region = regionFactory.create(regionName);

    EvictionCounters lruStats = ((InternalRegion) region).getEvictionController().getCounters();
    assertThat(lruStats).isNotNull();

    RegionMXBean regionMXBean = getManagementService().getLocalRegionMBean(toPath(regionName));
    assertThat(regionMXBean).isNotNull();

    for (int total = 0; total < 10; total++) {
      int[] array = new int[250];
      array[0] = total;
      region.put(total, array);
    }
    assertThat(regionMXBean.getEntrySize()).isGreaterThan(0);
  }

  private void createFixedPartitionRegion(final String regionName,
      final List<FixedPartitionAttributes> fixedPartitionAttributesList) {
    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(2);
    partitionAttributesFactory.setTotalNumBuckets(12);

    for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
      partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
    }

    partitionAttributesFactory.setPartitionResolver(new QuarterlyFixedPartitionResolver<>());

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    Region<?, ?> region = regionFactory.create(regionName);

    SystemManagementService service = getSystemManagementService();
    RegionMXBean regionMXBean = service.getLocalRegionMBean(toPath(regionName));
    RegionAttributes regionAttributes = region.getAttributes();

    PartitionAttributesData partitionAttributesData = regionMXBean.listPartitionAttributes();
    verifyPartitionData(regionAttributes, partitionAttributesData, true);

    FixedPartitionAttributesData[] fixedPartitionAttributesData =
        regionMXBean.listFixedPartitionAttributes();
    assertThat(fixedPartitionAttributesData).isNotNull();
    assertThat(fixedPartitionAttributesData).hasSize(3);

    for (FixedPartitionAttributesData aFixedPartitionAttributesData : fixedPartitionAttributesData) {
      // TODO: add real assertions for FixedPartitionAttributesData
      // LogWriterUtils.getLogWriter().info("<ExpectedString> Fixed PR Data is " +
      // fixedPartitionAttributesData[i] + "</ExpectedString> ");
    }
  }

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

  private void addMemberNotificationListener(final int expectedMembers)
      throws InstanceNotFoundException {
    Set<DistributedMember> otherMemberSet = getOtherMembers();
    assertThat(otherMemberSet).hasSize(expectedMembers);

    SystemManagementService service = getSystemManagementService();

    List<Notification> notifications = new ArrayList<>();
    MEMBER_NOTIFICATIONS.set(notifications);

    for (DistributedMember member : otherMemberSet) {
      MemberNotificationListener listener = new MemberNotificationListener(notifications);
      ObjectName objectName = service.getMemberMBeanName(member);
      awaitMemberMXBeanProxy(objectName);

      getPlatformMBeanServer().addNotificationListener(objectName, listener, null, null);
    }
  }

  /**
   * Add a Notification listener to DistributedSystemMBean which should gather all the notifications
   * which are propagated through all individual MemberMBeans Hence Region created/destroyed should
   * be visible to this listener
   */
  private void addSystemNotificationListener() throws InstanceNotFoundException {
    awaitDistributedSystemMXBean();

    List<Notification> notifications = new ArrayList<>();
    SYSTEM_NOTIFICATIONS.set(notifications);

    DistributedSystemNotificationListener listener =
        new DistributedSystemNotificationListener(notifications);
    ObjectName objectName = MBeanJMXAdapter.getDistributedSystemName();
    getPlatformMBeanServer().addNotificationListener(objectName, listener, null, null);
  }

  private void awaitMemberCount(final int expectedCount) {
    DistributedSystemMXBean distributedSystemMXBean = awaitDistributedSystemMXBean();
    await()
        .untilAsserted(
            () -> assertThat(distributedSystemMXBean.getMemberCount()).isEqualTo(expectedCount));
  }

  private DistributedRegionMXBean awaitDistributedRegionMXBean(final String regionName,
      final int memberCount) {
    SystemManagementService service = getSystemManagementService();

    await().untilAsserted(
        () -> assertThat(service.getDistributedRegionMXBean(toPath(regionName))).isNotNull());
    await()
        .untilAsserted(() -> assertThat(
            service.getDistributedRegionMXBean(toPath(regionName)).getMemberCount())
                .isEqualTo(memberCount));

    return service.getDistributedRegionMXBean(toPath(regionName));
  }

  private RegionMXBean awaitRegionMXBeanProxy(final DistributedMember member,
      final String regionName) {
    SystemManagementService service = getSystemManagementService();
    ObjectName objectName = service.getRegionMBeanName(member, toPath(regionName));
    String alias = "awaiting RegionMXBean proxy for " + member;

    await(alias)
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, RegionMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, RegionMXBean.class);
  }

  private RegionMXBean awaitRegionMXBeanProxy(final ObjectName objectName) {
    SystemManagementService service = getSystemManagementService();

    await()
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, RegionMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, RegionMXBean.class);
  }

  private MemberMXBean awaitMemberMXBeanProxy(final DistributedMember member) {
    SystemManagementService service = getSystemManagementService();
    ObjectName objectName = service.getMemberMBeanName(member);
    String alias = "awaiting MemberMXBean proxy for " + member;

    await(alias)
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, MemberMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, MemberMXBean.class);
  }

  private MemberMXBean awaitMemberMXBeanProxy(final ObjectName objectName) {
    SystemManagementService service = getSystemManagementService();
    await()
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, MemberMXBean.class)).isNotNull());
    return service.getMBeanProxy(objectName, MemberMXBean.class);
  }

  private DistributedSystemMXBean awaitDistributedSystemMXBean() {
    ManagementService service = getSystemManagementService();

    await().untilAsserted(() -> assertThat(service.getDistributedSystemMXBean()).isNotNull());

    return service.getDistributedSystemMXBean();
  }

  private void verifyEntrySize(String regionName, final int expectedMembers) {
    DistributedRegionMXBean distributedRegionMXBean =
        awaitDistributedRegionMXBean(regionName, expectedMembers);
    assertThat(distributedRegionMXBean).isNotNull();
    assertThat(distributedRegionMXBean.getEntrySize()).isGreaterThan(0);
  }

  private void verifyRegionMXBeanIsNotNull(final String regionName, final String subregionName) {
    RegionMXBean regionMXBean =
        getManagementService().getLocalRegionMBean(toPath(regionName, subregionName));
    assertThat(regionMXBean).isNotNull();
  }

  private void verifyRegionMXBeanIsNull(final String regionName, final String subregionName) {
    RegionMXBean regionMXBean =
        getManagementService().getLocalRegionMBean(toPath(regionName, subregionName));
    assertThat(regionMXBean).isNull();
  }

  private void verifyNavigationApis(final List<String> memberIds) throws Exception {
    ManagementService service = getManagementService();
    assertThat(service.getDistributedSystemMXBean()).isNotNull();

    // With the DUnit framework there is a locator, a manager and 3 members
    awaitMemberCount(5);

    DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
    assertThat(distributedSystemMXBean.listDistributedRegionObjectNames()).hasSize(2);

    assertThat(
        distributedSystemMXBean.fetchDistributedRegionObjectName(toPath(partitionedRegionName)))
            .isNotNull();
    assertThat(distributedSystemMXBean.fetchDistributedRegionObjectName(toPath(regionName)))
        .isNotNull();

    ObjectName actualName =
        distributedSystemMXBean.fetchDistributedRegionObjectName(toPath(partitionedRegionName));
    ObjectName expectedName = getDistributedRegionMbeanName(toPath(partitionedRegionName));
    assertThat(actualName).isEqualTo(expectedName);

    actualName = distributedSystemMXBean.fetchDistributedRegionObjectName(toPath(regionName));
    expectedName = getDistributedRegionMbeanName(toPath(regionName));
    assertThat(actualName).isEqualTo(expectedName);

    for (String memberId : memberIds) {
      ObjectName objectName = getMemberMBeanName(memberId);
      awaitMemberMXBeanProxy(objectName);

      ObjectName[] objectNames = distributedSystemMXBean.fetchRegionObjectNames(objectName);
      assertThat(objectNames).isNotNull();
      assertThat(objectNames).hasSize(2);

      List<ObjectName> listOfNames = Arrays.asList(objectNames);

      expectedName = getRegionMBeanName(memberId, toPath(partitionedRegionName));
      assertThat(listOfNames).contains(expectedName);

      expectedName = getRegionMBeanName(memberId, toPath(regionName));
      assertThat(listOfNames).contains(expectedName);
    }

    for (String memberId : memberIds) {
      ObjectName objectName = getMemberMBeanName(memberId);
      awaitMemberMXBeanProxy(objectName);

      expectedName = getRegionMBeanName(memberId, toPath(partitionedRegionName));
      awaitRegionMXBeanProxy(expectedName);

      actualName =
          distributedSystemMXBean.fetchRegionObjectName(memberId, toPath(partitionedRegionName));
      assertThat(actualName).isEqualTo(expectedName);

      expectedName = getRegionMBeanName(memberId, toPath(regionName));
      awaitRegionMXBeanProxy(expectedName);

      actualName = distributedSystemMXBean.fetchRegionObjectName(memberId, toPath(regionName));
      assertThat(actualName).isEqualTo(expectedName);
    }
  }

  private void verifyMemberNotifications(final String regionName, final int expectedMembers) {
    await()
        .untilAsserted(() -> assertThat(MEMBER_NOTIFICATIONS.get()).hasSize(expectedMembers * 2));

    int regionCreatedCount = 0;
    int regionDestroyedCount = 0;
    for (Notification notification : MEMBER_NOTIFICATIONS.get()) {
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
  }

  /**
   * Please don't delete verifySystemNotifications. We need to improve verification in this test
   * class and fix whatever flakiness is in this method.
   */
  @SuppressWarnings("unused")
  private void verifySystemNotifications(final String regionName, final int expectedMembers) {
    assertThat(SYSTEM_NOTIFICATIONS.get()).isNotNull();
    assertThat(SYSTEM_NOTIFICATIONS.get()).hasSize(expectedMembers + 2); // 2 for the manager


    int regionCreatedCount = 0;
    int regionDestroyedCount = 0;
    for (Notification notification : SYSTEM_NOTIFICATIONS.get()) {
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
  }

  private void verifyProxyCleanup(final String regionName) {
    SystemManagementService service = getSystemManagementService();

    Set<DistributedMember> otherMemberSet = getOtherMembers();
    for (DistributedMember member : otherMemberSet) {
      String alias = "Waiting for the proxy to get deleted at managing node";
      await(alias).untilAsserted(
          () -> assertThat(
              service.getMBeanProxy(service.getRegionMBeanName(member, toPath(regionName)),
                  RegionMXBean.class)).isNull());
    }
  }

  private void verifyRemoteDistributedRegion(final int expectedMembers, final String regionName) {
    Set<DistributedMember> otherMemberSet = getOtherMembers();
    assertThat(otherMemberSet).hasSize(expectedMembers);

    for (DistributedMember member : otherMemberSet) {
      RegionMXBean regionMXBean = awaitRegionMXBeanProxy(member, regionName);

      RegionAttributesData regionAttributesData = regionMXBean.listRegionAttributes();
      assertThat(regionAttributesData).isNotNull();

      MembershipAttributesData membershipAttributesData = regionMXBean.listMembershipAttributes();
      assertThat(membershipAttributesData).isNotNull();

      EvictionAttributesData evictionAttributesData = regionMXBean.listEvictionAttributes();
      assertThat(evictionAttributesData).isNotNull();
    }

    DistributedRegionMXBean distributedRegionMXBean =
        awaitDistributedRegionMXBean(regionName, expectedMembers);

    assertThat(distributedRegionMXBean).isNotNull();
    assertThat(distributedRegionMXBean.getFullPath()).isEqualTo(toPath(regionName));
  }

  private void verifyDistributedMBean(final String regionName, final int expectedMembers) {
    if (expectedMembers == 0) {
      ManagementService service = getManagementService();
      String alias = "Waiting for the proxy to get deleted at managing node";
      await(alias)
          .untilAsserted(
              () -> assertThat(service.getDistributedRegionMXBean(toPath(regionName))).isNull());
      return;
    }

    DistributedRegionMXBean distributedRegionMXBean =
        awaitDistributedRegionMXBean(regionName, expectedMembers);

    assertThat(distributedRegionMXBean.getFullPath()).isEqualTo(toPath(regionName));
    assertThat(distributedRegionMXBean.getMemberCount()).isEqualTo(expectedMembers);
    assertThat(distributedRegionMXBean.getMembers()).hasSize(expectedMembers);

    // Check Stats related Data
    // LogWriterUtils.getLogWriter().info("<ExpectedString> CacheListenerCallsAvgLatency is " +
    // distributedRegionMXBean.getCacheListenerCallsAvgLatency() + "</ExpectedString> ");
    // LogWriterUtils.getLogWriter().info("<ExpectedString> CacheWriterCallsAvgLatency is " +
    // distributedRegionMXBean.getCacheWriterCallsAvgLatency() + "</ExpectedString> ");
    // LogWriterUtils.getLogWriter().info("<ExpectedString> CreatesRate is " +
    // distributedRegionMXBean.getCreatesRate() + "</ExpectedString> ");
  }

  private void verifyRemotePartitionRegion(final String regionName) {
    Set<DistributedMember> otherMemberSet = getOtherMembers();

    for (DistributedMember member : otherMemberSet) {
      RegionMXBean regionMXBean = awaitRegionMXBeanProxy(member, regionName);
      PartitionAttributesData partitionAttributesData = regionMXBean.listPartitionAttributes();
      assertThat(partitionAttributesData).isNotNull();
    }

    ManagementService service = getManagementService();
    DistributedRegionMXBean distributedRegionMXBean =
        service.getDistributedRegionMXBean(toPath(regionName));
    assertThat(distributedRegionMXBean.getMembers()).hasSize(3);
  }

  private void verifyReplicateRegionAfterCreate(final String regionName)
      throws MalformedObjectNameException {
    Cache cache = getCache();

    String memberId =
        MBeanJMXAdapter.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
    ObjectName objectName = ObjectName.getInstance("GemFire:type=Member,member=" + memberId);

    // List<Notification> notifications = new ArrayList<>();
    // MEMBER_NOTIFICATIONS_REF.set(notifications);
    //
    // MemberNotificationListener listener = new MemberNotificationListener(notifications);
    // ManagementFactory.getPlatformMBeanServer().addNotificationListener(objectName, listener,
    // null, null);

    SystemManagementService service = getSystemManagementService();
    RegionMXBean regionMXBean = service.getLocalRegionMBean(toPath(regionName));
    assertThat(regionMXBean).isNotNull();

    Region region = cache.getRegion(regionName);
    RegionAttributes regionAttributes = region.getAttributes();

    RegionAttributesData regionAttributesData = regionMXBean.listRegionAttributes();
    verifyRegionAttributes(regionAttributes, regionAttributesData);

    MembershipAttributesData membershipData = regionMXBean.listMembershipAttributes();
    assertThat(membershipData).isNotNull();

    EvictionAttributesData evictionData = regionMXBean.listEvictionAttributes();
    assertThat(evictionData).isNotNull();
  }

  private void verifyPartitionRegionAfterCreate(final String regionName,
      final boolean hasPartitionResolver) {
    Region region = getCache().getRegion(regionName);

    SystemManagementService service = getSystemManagementService();
    RegionMXBean regionMXBean = service.getLocalRegionMBean(toPath(regionName));

    verifyPartitionData(region.getAttributes(), regionMXBean.listPartitionAttributes(),
        hasPartitionResolver);
  }

  private void verifyReplicatedRegionAfterClose(final String regionName) {
    SystemManagementService service = getSystemManagementService();
    RegionMXBean regionMXBean = service.getLocalRegionMBean(toPath(regionName));
    assertThat(regionMXBean).isNull();

    ObjectName objectName = service.getRegionMBeanName(
        getCache().getDistributedSystem().getDistributedMember(), toPath(regionName));
    assertThat(service.getLocalManager().getManagementResourceRepo()
        .getEntryFromLocalMonitoringRegion(objectName)).isNull();
  }

  private void verifyPartitionRegionAfterClose(String regionName) {
    ManagementService service = getManagementService();
    RegionMXBean regionMXBean = service.getLocalRegionMBean(toPath(regionName));
    assertThat(regionMXBean).isNull();
  }

  private void verifyPartitionData(final RegionAttributes expectedRegionAttributes,
      final PartitionAttributesData partitionAttributesData,
      final boolean hasPartitionResolver) {
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

    if (hasPartitionResolver) {
      PartitionResolver partitionResolver = expectedPartitionAttributes.getPartitionResolver();
      assertThat(partitionAttributesData.getPartitionResolver())
          .isEqualTo(partitionResolver.getName());

    } else {

      assertThat(partitionAttributesData.getPartitionResolver()).isNull();
    }

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

  private void verifyRegionAttributes(final RegionAttributes regionAttributes,
      final RegionAttributesData regionAttributesData) {
    assertThat(regionAttributesData.getCompressorClassName()).isNull();

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

  private void verifyRemoteFixedPartitionRegion(final String regionName) {
    Set<DistributedMember> otherMemberSet = getOtherMembers();

    for (DistributedMember member : otherMemberSet) {
      RegionMXBean bean = awaitRegionMXBeanProxy(member, regionName);

      PartitionAttributesData data = bean.listPartitionAttributes();
      assertThat(data).isNotNull();

      FixedPartitionAttributesData[] fixedPrData = bean.listFixedPartitionAttributes();
      assertThat(fixedPrData).isNotNull();
      assertThat(fixedPrData).hasSize(3);

      for (FixedPartitionAttributesData aFixedPrData : fixedPrData) {
        // TODO: add real assertions for FixedPartitionAttributesData
        // LogWriterUtils.getLogWriter().info("<ExpectedString> Remote PR Data is " +
        // fixedPrData[i] + "</ExpectedString> ");
      }
    }
  }

  private static void createManager(CacheRule cacheRule) {
    Properties config = new Properties();
    config.put(JMX_MANAGER, "true");
    config.put(JMX_MANAGER_START, "true");
    config.put(JMX_MANAGER_PORT, "0");
    config.put(HTTP_SERVICE_PORT, "0");
    config.put(ENABLE_TIME_STATISTICS, "true");
    config.put(STATISTIC_SAMPLING_ENABLED, "true");

    cacheRule.createCache(config);
  }

  private static void createMember(CacheRule cacheRule) {
    Properties config = new Properties();
    config.put(JMX_MANAGER, "false");
    config.put(ENABLE_TIME_STATISTICS, "true");
    config.put(STATISTIC_SAMPLING_ENABLED, "true");

    cacheRule.createCache(config);
  }

  /**
   * Registered in manager VM. Listens to notifications from a MemberMXBean.
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

      // TODO: add real assertions for Notifications
      // LogWriterUtils.getLogWriter().info("<ExpectedString> Member Level Notifications" +
      // notification + "</ExpectedString> ");
    }
  }

  /**
   * Registered in manager VM. Listens to notifications from a DistributedSystemMXBean.
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

      // TODO: add real assertions for Notifications
      // LogWriterUtils.getLogWriter().info("<ExpectedString> Distributed System Notifications" +
      // notification + "</ExpectedString> ");
    }
  }

  /**
   * Routes based on which quarter of the year that a month falls within.
   */
  private static class QuarterlyFixedPartitionResolver<K, V>
      implements FixedPartitionResolver<K, V> {

    @Override
    public String getPartitionName(EntryOperation<K, V> opDetails,
        @Deprecated Set<String> targetPartitions) {
      int month = getMonth(opDetails);

      if (month == 0 || month == 1 || month == 2) {
        return "Q1";
      } else if (month == 3 || month == 4 || month == 5) {
        return "Q2";
      } else if (month == 6 || month == 7 || month == 8) {
        return "Q3";
      } else if (month == 9 || month == 10 || month == 11) {
        return "Q4";
      } else {
        return "Invalid Quarter";
      }
    }

    @Override
    public Serializable getRoutingObject(EntryOperation<K, V> opDetails) {
      int month = getMonth(opDetails);

      switch (month) {
        case 0:
          return "January";
        case 1:
          return "February";
        case 2:
          return "March";
        case 3:
          return "April";
        case 4:
          return "May";
        case 5:
          return "June";
        case 6:
          return "July";
        case 7:
          return "August";
        case 8:
          return "September";
        case 9:
          return "October";
        case 10:
          return "November";
        case 11:
          return "December";
        default:
          return null;
      }
    }

    @Override
    public String getName() {
      return getClass().getSimpleName();
    }

    private int getMonth(EntryOperation<K, V> opDetails) {
      Date date = (Date) opDetails.getKey();
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      return calendar.get(MONTH);
    }
  }
}
