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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.management.JMException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.internal.LocalManager;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.NotificationHub.NotificationHubListener;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for {@link MemberMXBean}.
 * <p>
 *
 * This class checks and verifies various data and operations exposed through MemberMXBean
 * interface.
 * <p>
 *
 * Goal of the Test : MemberMBean gets created once cache is created. Data like config data and
 * stats are of proper value To check proper federation of MemberMBean including remote ops and
 * remote data access
 */

@SuppressWarnings({"unused"})
public class CacheManagementDUnitTest implements Serializable {

  /** used in memberVMs */
  private static final String NOTIFICATION_REGION_NAME = "NotifyTestRegion_";

  /** used in managerVM */
  private static final List<Notification> notifications = new CopyOnWriteArrayList<>();

  @Manager
  private VM managerVM;

  @SuppressWarnings("MismatchedReadAndWriteOfArray")
  @Member
  private VM[] memberVMs;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().build();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
    // noinspection Convert2MethodRef - this method reference not compatible with RMI usage
    managerVM.invoke(() -> notifications.clear());
  }

  @Test
  public void testGemFireConfigData() {
    managementTestRule.createMembers();
    managementTestRule.createManagers();

    Map<DistributedMember, DistributionConfig> configMap = new HashMap<>();
    for (VM memberVM : memberVMs) {
      Map<DistributedMember, DistributionConfig> configMapMember =
          memberVM.invoke(this::verifyConfigData);
      configMap.putAll(configMapMember);
    }

    managerVM.invoke(() -> verifyConfigDataRemote(configMap));
  }

  /**
   * Tests each and every operation that is defined on the MemberMXBean
   */
  @Test
  public void testMemberMBeanOperations() throws Exception {
    int i = 1;
    for (VM memberVM : memberVMs) {
      Properties props = new Properties();
      props.setProperty(LOG_FILE, temporaryFolder
          .newFile(testName.getMethodName() + "-VM" + i + ".log").getAbsolutePath());
      managementTestRule.createMember(memberVM, props);
      i++;
    }

    managementTestRule.createManagers();

    for (VM memberVM : memberVMs) {
      String logMessage = "This line should be in the log";
      memberVM.invoke(() -> managementTestRule.getCache().getLogger().info(logMessage));

      String log = memberVM.invoke(this::fetchLog);
      assertThat(log).isNotNull();
      assertThat(log).contains(logMessage);

      JVMMetrics jvmMetrics = memberVM.invoke(this::fetchJVMMetrics);

      OSMetrics osMetrics = memberVM.invoke(this::fetchOSMetrics);

      // TODO: need assertions

      memberVM.invoke(this::shutDownMember);
    }

    managerVM.invoke(this::verifyNoMembers);
  }

  /**
   * Invoke remote operations on MemberMBean
   */
  @Test
  public void testMemberMBeanOpsRemote() {
    managementTestRule.createMembers();
    managementTestRule.createManagers();
    managerVM.invoke(this::invokeRemoteMemberMXBeanOps);
  }

  /**
   * Creates and starts a managerVM. Multiple Managers
   */
  @Test
  public void testManager() {
    managementTestRule.createMember(memberVMs[0]);
    managementTestRule.createMember(memberVMs[1]);

    managementTestRule.createManager(memberVMs[2], false);

    managementTestRule.createManager(managerVM, false);

    memberVMs[2].invoke(this::startManager);

    // Now start Managing node managerVM. System will have two Managers now which
    // should be OK
    DistributedMember member = managementTestRule.getDistributedMember(memberVMs[2]);
    managementTestRule.startManager(managerVM);

    verifyManagerStarted(managerVM, member);
    managementTestRule.stopManager(managerVM);
  }

  /**
   * Creates and starts a managerVM. Multiple Managers
   */
  @Test
  public void testManagerShutdown() {
    managementTestRule.createMember(memberVMs[0]);
    managementTestRule.createMember(memberVMs[1]);
    managementTestRule.createMember(memberVMs[2]);

    managementTestRule.createManager(managerVM, false);
    managementTestRule.startManager(managerVM);

    verifyManagerStarted(managerVM,
        managementTestRule.getDistributedMember(memberVMs[0]));

    managementTestRule.stopManager(managerVM);
    verifyManagerStopped(managerVM, memberVMs.length);
  }

  @Test
  public void closeCacheShouldStopLocalManager() {
    managementTestRule.createMember(memberVMs[0]);
    managementTestRule.createMember(memberVMs[1]);

    managementTestRule.createManager(memberVMs[2], false);

    // Only creates a cache in Managing Node
    // Does not start the managerVM
    managementTestRule.createManager(managerVM, false);

    memberVMs[2].invoke(this::startManager);

    memberVMs[2].invoke(() -> {
      SystemManagementService service = managementTestRule.getSystemManagementService();
      LocalManager localManager = service.getLocalManager();
      managementTestRule.getCache().close();
      assertThat(localManager.isRunning()).isFalse();
      assertThat(service.isManager()).isFalse();
      assertThat(service.getLocalManager()).isNull();
    });
  }

  @Test
  public void testGetMBean() {
    managementTestRule.createMember(memberVMs[0]);
    managementTestRule.createMember(memberVMs[1]);
    managementTestRule.createMember(memberVMs[2]);

    managementTestRule.createManager(managerVM, false);

    managementTestRule.startManager(managerVM);

    verifyGetMBeanInstance(managerVM);
  }

  @Test
  public void testQueryMBeans() {
    managementTestRule.createMember(memberVMs[0]);
    managementTestRule.createMember(memberVMs[1]);
    managementTestRule.createMember(memberVMs[2]);

    managementTestRule.createManager(managerVM, false);

    managementTestRule.startManager(managerVM);

    verifyQueryMBeans(managerVM);
  }

  @Test
  public void testNotification() {
    // Step : 1 : Create Managed Node Caches
    managementTestRule.createMember(memberVMs[0]);
    managementTestRule.createMember(memberVMs[1]);
    managementTestRule.createMember(memberVMs[2]);

    // Step : 2 : Create Managing Node Cache, start managerVM, add a notification
    // handler to DistributedSystemMXBean
    managementTestRule.createManager(managerVM, false);
    managementTestRule.startManager(managerVM);
    attachListenerToDistributedSystemMXBean(managerVM);

    // Step : 3 : Verify Notification count, notification region sizes
    verifyNotificationsAndRegionSize(memberVMs[0], memberVMs[1], memberVMs[2],
        managerVM);
  }

  @Test
  public void testNotificationManagingNodeFirst() {
    // Step : 1 : Create Managing Node Cache, start managerVM, add a notification
    // handler to DistributedSystemMXBean
    managementTestRule.createManager(managerVM, false);
    managementTestRule.startManager(managerVM);

    attachListenerToDistributedSystemMXBean(managerVM);

    // Step : 2 : Create Managed Node Caches
    managementTestRule.createMember(memberVMs[0]);
    managementTestRule.createMember(memberVMs[1]);
    managementTestRule.createMember(memberVMs[2]);

    // Step : 3 : Verify Notification count, notification region sizes
    verifyNotificationsAndRegionSize(memberVMs[0], memberVMs[1], memberVMs[2],
        managerVM);
  }

  @Test
  public void testRedundancyZone() {
    String redundancyZone = "ARMY_ZONE";

    Properties props = new Properties();
    props.setProperty(REDUNDANCY_ZONE, redundancyZone);

    managementTestRule.createMember(memberVMs[0], props);

    memberVMs[0].invoke("verifyRedundancyZone", () -> {
      ManagementService service = managementTestRule.getExistingManagementService();
      MemberMXBean memberMXBean = service.getMemberMXBean();
      assertThat(memberMXBean.getRedundancyZone()).isEqualTo(redundancyZone);
    });
  }

  private void verifyQueryMBeans(final VM managerVM) {
    managerVM.invoke("validateQueryMBeans", () -> {
      SystemManagementService service = managementTestRule.getSystemManagementService();
      Set<DistributedMember> otherMembers = managementTestRule.getOtherNormalMembers();
      Set<ObjectName> superSet = new HashSet<>();

      for (DistributedMember member : otherMembers) {
        ObjectName memberMBeanName = service.getMemberMBeanName(member);

        MXBeanAwaitility.awaitMemberMXBeanProxy(member, service);

        Set<ObjectName> objectNames = service.queryMBeanNames(member);
        superSet.addAll(objectNames);
        assertThat(objectNames.contains(memberMBeanName)).isTrue();
      }

      Set<ObjectName> names =
          service.queryMBeanNames(managementTestRule.getDistributedMember());
      ObjectName[] arrayOfNames = names.toArray(new ObjectName[0]);
      assertThat(superSet).doesNotContain(arrayOfNames);
    });
  }

  private void verifyGetMBeanInstance(final VM managerVM) {
    managerVM.invoke("verifyGetMBeanInstance", () -> {
      SystemManagementService service = managementTestRule.getSystemManagementService();
      Set<DistributedMember> otherMembers = managementTestRule.getOtherNormalMembers();

      for (DistributedMember member : otherMembers) {
        ObjectName memberMBeanName = service.getMemberMBeanName(member);

        MXBeanAwaitility.awaitMemberMXBeanProxy(member, service);

        MemberMXBean memberMXBean = service.getMBeanInstance(memberMBeanName, MemberMXBean.class);
        assertThat(memberMXBean).isNotNull();
      }

      DistributedMember distributedMember = managementTestRule.getDistributedMember();
      ObjectName memberMBeanName = service.getMemberMBeanName(distributedMember);
      MemberMXBean memberMXBean = service.getMBeanInstance(memberMBeanName, MemberMXBean.class);
      assertThat(memberMXBean).isNotNull();
    });
  }

  private void verifyManagerStarted(final VM managerVM, final DistributedMember otherMember) {
    managerVM.invoke("verifyManagerStarted", () -> {
      SystemManagementService service = managementTestRule.getSystemManagementService();
      assertThat(service.isManager()).isTrue();

      assertThat(service.getLocalManager().isRunning()).isTrue();

      assertThat(service.getLocalManager().getFederationScheduler().isShutdown()).isFalse();

      ObjectName memberMBeanName = service.getMemberMBeanName(otherMember);

      GeodeAwaitility.await().untilAsserted(
          () -> assertThat(service.getMBeanProxy(memberMBeanName, MemberMXBean.class)).isNotNull());
      MemberMXBean memberMXBean = service.getMBeanProxy(memberMBeanName, MemberMXBean.class);

      // Ensure Data getting federated from Managing node
      long start = memberMXBean.getMemberUpTime();
      GeodeAwaitility.await()
          .untilAsserted(() -> assertThat(memberMXBean.getMemberUpTime()).isGreaterThan(start));
    });
  }

  /**
   * Add any Manager clean up asserts here
   */
  private void verifyManagerStopped(final VM managerVM, final int otherMembersCount) {
    managerVM.invoke("verifyManagerStopped", () -> {
      SystemManagementService service = managementTestRule.getSystemManagementService();

      assertThat(service.isManager()).isFalse();
      assertThat(service.getLocalManager().isRunning()).isTrue();
      assertThat(service.getLocalManager().getFederationScheduler().isShutdown()).isFalse();

      // Check for Proxies
      Set<DistributedMember> otherMembers = managementTestRule.getOtherNormalMembers();
      assertThat(otherMembers).hasSize(otherMembersCount);

      for (DistributedMember member : otherMembers) {
        Set<ObjectName> proxyNames =
            service.getFederatingManager().proxyFactory().findAllProxies(member);
        assertThat(proxyNames).isEmpty();

        ObjectName proxyMBeanName = service.getMemberMBeanName(member);
        assertThat(MBeanJMXAdapter.mbeanServer.isRegistered(proxyMBeanName)).isFalse();
      }
    });
  }

  private Map<DistributedMember, DistributionConfig> verifyConfigData() {
    ManagementService service = managementTestRule.getManagementService();
    InternalDistributedSystem ids =
        (InternalDistributedSystem) managementTestRule.getCache().getDistributedSystem();
    DistributionConfig config = ids.getConfig();

    MemberMXBean bean = service.getMemberMXBean();
    GemFireProperties data = bean.listGemFireProperties();
    verifyGemFirePropertiesData(config, data);

    Map<DistributedMember, DistributionConfig> configMap = new HashMap<>();
    configMap.put(ids.getDistributedMember(), config);
    return configMap;
  }

  /**
   * This is to check whether the config data has been propagated to the Managing node properly or
   * not.
   */
  private void verifyConfigDataRemote(final Map<DistributedMember, DistributionConfig> configMap) {
    SystemManagementService service = managementTestRule.getSystemManagementService();
    Set<DistributedMember> otherMembers = managementTestRule.getOtherNormalMembers();

    for (DistributedMember member : otherMembers) {
      MemberMXBean memberMXBean = MXBeanAwaitility.awaitMemberMXBeanProxy(member, service);

      GemFireProperties data = memberMXBean.listGemFireProperties();
      DistributionConfig config = configMap.get(member);
      verifyGemFirePropertiesData(config, data);
    }
  }

  /**
   * Asserts that distribution config and gemfireProperty composite types hold the same values
   */
  @SuppressWarnings("deprecation")
  private void verifyGemFirePropertiesData(final DistributionConfig config,
      final GemFireProperties data) {
    assertThat(data.getMemberName()).isEqualTo(config.getName());

    // **TODO **
    String memberGroups = null;

    assertThat(data.getMcastPort()).isEqualTo(config.getMcastPort());
    assertThat(data.getMcastAddress()).isEqualTo(config.getMcastAddress().getHostAddress());
    assertThat(data.getBindAddress()).isEqualTo(config.getBindAddress());
    assertThat(data.getTcpPort()).isEqualTo(config.getTcpPort());
    assertThat(removeVMDir(data.getCacheXMLFile()))
        .isEqualTo(removeVMDir(config.getCacheXmlFile().getAbsolutePath()));

    // **TODO **
    assertThat(data.getMcastTTL()).isEqualTo(config.getMcastTtl());
    assertThat(data.getServerBindAddress()).isEqualTo(config.getServerBindAddress());
    assertThat(data.getLocators()).isEqualTo(config.getLocators());

    // The start locator may contain a directory
    assertThat(removeVMDir(data.getStartLocator()))
        .isEqualTo(removeVMDir(config.getStartLocator()));
    assertThat(removeVMDir(data.getLogFile()))
        .isEqualTo(removeVMDir(config.getLogFile().getAbsolutePath()));
    assertThat(data.getLogLevel()).isEqualTo(config.getLogLevel());
    assertThat(data.isStatisticSamplingEnabled()).isEqualTo(config.getStatisticSamplingEnabled());
    assertThat(removeVMDir(data.getStatisticArchiveFile()))
        .isEqualTo(removeVMDir(config.getStatisticArchiveFile().getAbsolutePath()));

    // ** TODO **
    String includeFile = null;
    assertThat(data.getAckWaitThreshold()).isEqualTo(config.getAckWaitThreshold());
    assertThat(data.getAckSevereAlertThreshold()).isEqualTo(config.getAckSevereAlertThreshold());
    assertThat(data.getArchiveFileSizeLimit()).isEqualTo(config.getArchiveFileSizeLimit());
    assertThat(data.getArchiveDiskSpaceLimit()).isEqualTo(config.getArchiveDiskSpaceLimit());
    assertThat(data.getLogFileSizeLimit()).isEqualTo(config.getLogFileSizeLimit());
    assertThat(data.getLogDiskSpaceLimit()).isEqualTo(config.getLogDiskSpaceLimit());
    assertThat(data.isClusterSSLEnabled()).isEqualTo(config.getClusterSSLEnabled());

    assertThat(data.getClusterSSLCiphers()).isEqualTo(config.getClusterSSLCiphers());
    assertThat(data.getClusterSSLProtocols()).isEqualTo(config.getClusterSSLProtocols());
    assertThat(data.isClusterSSLRequireAuthentication())
        .isEqualTo(config.getClusterSSLRequireAuthentication());
    assertThat(data.getSocketLeaseTime()).isEqualTo(config.getSocketLeaseTime());
    assertThat(data.getSocketBufferSize()).isEqualTo(config.getSocketBufferSize());
    assertThat(data.getMcastSendBufferSize()).isEqualTo(config.getMcastSendBufferSize());
    assertThat(data.getMcastRecvBufferSize()).isEqualTo(config.getMcastRecvBufferSize());
    assertThat(data.getMcastByteAllowance())
        .isEqualTo(config.getMcastFlowControl().getByteAllowance());
    assertThat(data.getMcastRechargeThreshold())
        .isEqualTo(config.getMcastFlowControl().getRechargeThreshold());
    assertThat(data.getMcastRechargeBlockMs())
        .isEqualTo(config.getMcastFlowControl().getRechargeBlockMs());
    assertThat(data.getUdpFragmentSize()).isEqualTo(config.getUdpFragmentSize());
    assertThat(data.getUdpSendBufferSize()).isEqualTo(config.getUdpSendBufferSize());
    assertThat(data.getUdpRecvBufferSize()).isEqualTo(config.getUdpRecvBufferSize());
    assertThat(data.isDisableTcp()).isEqualTo(config.getDisableTcp());
    assertThat(data.isEnableTimeStatistics()).isEqualTo(config.getEnableTimeStatistics());
    assertThat(data.isEnableNetworkPartitionDetection())
        .isEqualTo(config.getEnableNetworkPartitionDetection());
    assertThat(data.getMemberTimeout()).isEqualTo(config.getMemberTimeout());

    assertThat(data.getMembershipPortRange()).containsExactly(config.getMembershipPortRange());

    assertThat(data.isConserveSockets()).isEqualTo(config.getConserveSockets());
    assertThat(data.getRoles()).isEqualTo(config.getRoles());
    assertThat(data.getMaxWaitTimeForReconnect()).isEqualTo(config.getMaxWaitTimeForReconnect());
    assertThat(data.getMaxNumReconnectTries()).isEqualTo(config.getMaxNumReconnectTries());
    assertThat(data.getAsyncDistributionTimeout()).isEqualTo(config.getAsyncDistributionTimeout());
    assertThat(data.getAsyncMaxQueueSize()).isEqualTo(config.getAsyncMaxQueueSize());
    assertThat(data.getClientConflation()).isEqualTo(config.getClientConflation());
    assertThat(data.getDurableClientId()).isEqualTo(config.getDurableClientId());
    assertThat(data.getDurableClientTimeout()).isEqualTo(config.getDurableClientTimeout());
    assertThat(data.getSecurityClientAuthInit()).isEqualTo(config.getSecurityClientAuthInit());
    assertThat(data.getSecurityClientAuthenticator())
        .isEqualTo(config.getSecurityClientAuthenticator());
    assertThat(data.getSecurityClientDHAlgo()).isEqualTo(config.getSecurityClientDHAlgo());
    assertThat(data.getSecurityPeerAuthInit()).isEqualTo(config.getSecurityPeerAuthInit());
    assertThat(data.getSecurityClientAuthenticator())
        .isEqualTo(config.getSecurityPeerAuthenticator());
    assertThat(data.getSecurityClientAccessor()).isEqualTo(config.getSecurityClientAccessor());
    assertThat(data.getSecurityClientAccessorPP()).isEqualTo(config.getSecurityClientAccessorPP());
    assertThat(data.getSecurityLogLevel()).isEqualTo(config.getSecurityLogLevel());
    assertThat(removeVMDir(data.getSecurityLogFile()))
        .isEqualTo(removeVMDir(config.getSecurityLogFile().getAbsolutePath()));
    assertThat(data.getSecurityPeerMembershipTimeout())
        .isEqualTo(config.getSecurityPeerMembershipTimeout());
    assertThat(data.isRemoveUnresponsiveClient()).isEqualTo(config.getRemoveUnresponsiveClient());
    assertThat(data.isDeltaPropagation()).isEqualTo(config.getDeltaPropagation());
    assertThat(data.getRedundancyZone()).isEqualTo(config.getRedundancyZone());
    assertThat(data.isEnforceUniqueHost()).isEqualTo(config.getEnforceUniqueHost());
    assertThat(data.getStatisticSampleRate()).isEqualTo(config.getStatisticSampleRate());
  }

  private void startManager() throws JMException {
    ManagementService service = managementTestRule.getManagementService();
    MemberMXBean memberMXBean = service.getMemberMXBean();
    if (memberMXBean.isManagerCreated()) {
      return;
    }

    // TODO: cleanup this mess
    // When the cache is created if jmx-managerVM is true then we create the managerVM.
    // So it may already exist when we get here.

    assertThat(memberMXBean.createManager()).isTrue();
    assertThat(memberMXBean.isManagerCreated()).isTrue();

    ManagerMXBean managerMXBean = service.getManagerMXBean();
    managerMXBean.start();

    assertThat(managerMXBean.isRunning()).isTrue();
    assertThat(memberMXBean.isManager()).isTrue();
    assertThat(service.isManager()).isTrue();
  }

  private String fetchLog() {
    ManagementService service = managementTestRule.getManagementService();
    MemberMXBean memberMXBean = service.getMemberMXBean();
    return memberMXBean.showLog(30);
  }

  private JVMMetrics fetchJVMMetrics() {
    ManagementService service = managementTestRule.getManagementService();
    MemberMXBean memberMXBean = service.getMemberMXBean();
    return memberMXBean.showJVMMetrics();
  }

  private OSMetrics fetchOSMetrics() {
    ManagementService service = managementTestRule.getManagementService();
    MemberMXBean memberMXBean = service.getMemberMXBean();
    return memberMXBean.showOSMetrics();
  }

  private void shutDownMember() {
    ManagementService service = managementTestRule.getManagementService();
    MemberMXBean memberMXBean = service.getMemberMXBean();
    memberMXBean.shutDownMember();
  }

  private void verifyNoMembers() {
    String alias = "awaiting " + managementTestRule.getOtherNormalMembers() + " to be empty";
    GeodeAwaitility.await(alias)
        .untilAsserted(() -> assertThat(managementTestRule.getOtherNormalMembers()).isEmpty());
  }

  private void invokeRemoteMemberMXBeanOps() {
    SystemManagementService service = managementTestRule.getSystemManagementService();
    Set<DistributedMember> otherMembers = managementTestRule.getOtherNormalMembers();

    for (DistributedMember member : otherMembers) {
      MemberMXBean memberMXBean = MXBeanAwaitility.awaitMemberMXBeanProxy(member, service);

      JVMMetrics metrics = memberMXBean.showJVMMetrics();

      String value = metrics.toString();
      boolean isManager = memberMXBean.isManager();
    }
  }

  private void attachListenerToDistributedSystemMXBean(final VM managerVM) {
    managerVM.invoke("attachListenerToDistributedSystemMXBean", () -> {
      ManagementService service = managementTestRule.getManagementService();
      assertThat(service.isManager()).isTrue();

      NotificationListener listener = (final Notification notification, final Object handback) -> {
        if (notification.getType().equals(JMXNotificationType.REGION_CREATED)) {
          notifications.add(notification);
        }
      };

      ManagementFactory.getPlatformMBeanServer().addNotificationListener(
          MBeanJMXAdapter.getDistributedSystemName(), listener, null, null);
    });
  }

  private void verifyNotificationsAndRegionSize(final VM memberVM1, final VM memberVM2,
      final VM memberVM3, final VM managerVM) {
    InternalDistributedMember member1 =
        (InternalDistributedMember) managementTestRule.getDistributedMember(memberVM1);
    InternalDistributedMember member2 =
        (InternalDistributedMember) managementTestRule.getDistributedMember(memberVM2);
    InternalDistributedMember member3 =
        (InternalDistributedMember) managementTestRule.getDistributedMember(memberVM3);

    String memberId1 = MBeanJMXAdapter.getUniqueIDForMember(member1);
    String memberId2 = MBeanJMXAdapter.getUniqueIDForMember(member2);
    String memberId3 = MBeanJMXAdapter.getUniqueIDForMember(member3);

    memberVM1.invoke("createNotificationRegion", () -> createNotificationRegion(memberId1));
    memberVM2.invoke("createNotificationRegion", () -> createNotificationRegion(memberId2));
    memberVM3.invoke("createNotificationRegion", () -> createNotificationRegion(memberId3));

    managerVM.invoke("verify notifications size", () -> {
      GeodeAwaitility.await().untilAsserted(() -> assertThat(notifications.size()).isEqualTo(45));

      Cache cache = managementTestRule.getCache();

      Region<?, ?> region1 =
          cache.getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + memberId1);
      Region<?, ?> region2 =
          cache.getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + memberId2);
      Region<?, ?> region3 =
          cache.getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + memberId3);

      // Even though we got 15 notification only 10 should be there due to
      // eviction attributes set in notification region

      GeodeAwaitility.await().untilAsserted(() -> assertThat(region1).hasSize(10));
      GeodeAwaitility.await().untilAsserted(() -> assertThat(region2).hasSize(10));
      GeodeAwaitility.await().untilAsserted(() -> assertThat(region3).hasSize(10));
    });
  }

  private void createNotificationRegion(final String memberId) {
    SystemManagementService service = managementTestRule.getSystemManagementService();
    Map<ObjectName, NotificationHubListener> notificationHubListenerMap =
        service.getNotificationHub().getListenerObjectMap();

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(notificationHubListenerMap.size()).isEqualTo(1));

    RegionFactory<?, ?> regionFactory =
        managementTestRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    for (int i = 1; i <= 15; i++) {
      regionFactory.create(NOTIFICATION_REGION_NAME + i);
    }
    Region<?, ?> region = managementTestRule.getCache()
        .getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + memberId);

    assertThat(region).isEmpty();
  }

  private static String removeVMDir(String string) {
    return string.replaceAll("vm.", "");
  }
}
