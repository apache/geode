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

import static java.lang.management.ManagementFactory.*;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.geode.management.internal.MBeanJMXAdapter.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.apache.geode.test.dunit.IgnoredException.*;
import static org.apache.geode.test.dunit.Invoke.*;
import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.management.internal.AlertDetails;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.NotificationHub;
import org.apache.geode.management.internal.NotificationHub.NotificationHubListener;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.MemberMBean;
import org.apache.geode.management.internal.beans.SequenceNumber;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Distributed System management tests
 * </p>
 * a) For all the notifications i) gemfire.distributedsystem.member.joined ii)
 * gemfire.distributedsystem.member.left iii) gemfire.distributedsystem.member.suspect iv ) All
 * notifications emitted by member mbeans vi) Alerts
 * </p>
 * b) Concurrently modify proxy list by removing member and accessing the distributed system MBean
 * </p>
 * c) Aggregate Operations like shutDownAll
 * </p>
 * d) Member level operations like fetchJVMMetrics()
 * </p>
 * e ) Statistics
 * </p>
 * TODO: break up the large tests into smaller tests
 */
@Category(DistributedTest.class)
@SuppressWarnings({"serial", "unused"})
public class DistributedSystemDUnitTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final String WARNING_LEVEL_MESSAGE = "Warning Level Alert Message";
  private static final String SEVERE_LEVEL_MESSAGE = "Severe Level Alert Message";

  private static List<Notification> notifications;
  private static Map<ObjectName, NotificationListener> notificationListenerMap;

  @Manager
  private VM managerVM;

  @Member
  private VM[] memberVMs;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().build();

  @Before
  public void before() throws Exception {
    notifications = new ArrayList<>();
    notificationListenerMap = new HashMap<>();

    invokeInEveryVM(() -> notifications = new ArrayList<>());
    invokeInEveryVM(() -> notificationListenerMap = new HashMap<>());
  }

  @After
  public void after() throws Exception {
    resetAlertCounts(this.managerVM);
  }

  /**
   * Tests each and every operations that is defined on the MemberMXBean
   */
  @Test
  public void testDistributedSystemAggregate() throws Exception {
    this.managementTestRule.createManager(this.managerVM);
    addNotificationListener(this.managerVM);

    for (VM memberVM : this.memberVMs) {
      this.managementTestRule.createMember(memberVM);
    }

    verifyDistributedSystemMXBean(this.managerVM);
  }

  /**
   * Tests each and every operations that is defined on the MemberMXBean
   */
  @Test
  public void testAlertManagedNodeFirst() throws Exception {
    for (VM memberVM : this.memberVMs) {
      this.managementTestRule.createMember(memberVM);
      generateWarningAlert(memberVM);
      generateSevereAlert(memberVM);
    }

    this.managementTestRule.createManager(this.managerVM);
    addAlertListener(this.managerVM);
    verifyAlertCount(this.managerVM, 0, 0);

    DistributedMember managerDistributedMember =
        this.managementTestRule.getDistributedMember(this.managerVM);

    // Before we start we need to ensure that the initial (implicit) SEVERE alert has propagated
    // everywhere.
    for (VM memberVM : this.memberVMs) {
      verifyAlertAppender(memberVM, managerDistributedMember, Alert.SEVERE);
    }

    setAlertLevel(this.managerVM, AlertDetails.getAlertLevelAsString(Alert.WARNING));

    for (VM memberVM : this.memberVMs) {
      verifyAlertAppender(memberVM, managerDistributedMember, Alert.WARNING);
      generateWarningAlert(memberVM);
      generateSevereAlert(memberVM);
    }

    verifyAlertCount(this.managerVM, 3, 3);
    resetAlertCounts(this.managerVM);

    setAlertLevel(this.managerVM, AlertDetails.getAlertLevelAsString(Alert.SEVERE));

    for (VM memberVM : this.memberVMs) {
      verifyAlertAppender(memberVM, managerDistributedMember, Alert.SEVERE);
      generateWarningAlert(memberVM);
      generateSevereAlert(memberVM);
    }

    verifyAlertCount(this.managerVM, 3, 0);
  }

  /**
   * Tests each and every operations that is defined on the MemberMXBean
   */
  @Test
  public void testShutdownAll() throws Exception {
    VM memberVM1 = getHost(0).getVM(0);
    VM memberVM2 = getHost(0).getVM(1);
    VM memberVM3 = getHost(0).getVM(2);

    VM managerVM = getHost(0).getVM(3);

    // managerVM Node is created first
    this.managementTestRule.createManager(managerVM);

    this.managementTestRule.createMember(memberVM1);
    this.managementTestRule.createMember(memberVM2);
    this.managementTestRule.createMember(memberVM3);

    shutDownAll(managerVM);
  }

  @Test
  public void testNavigationAPIS() throws Exception {
    this.managementTestRule.createManager(this.managerVM);

    for (VM memberVM : this.memberVMs) {
      this.managementTestRule.createMember(memberVM);
    }

    verifyFetchMemberObjectName(this.managerVM, this.memberVMs.length + 1);
  }

  @Test
  public void testNotificationHub() throws Exception {
    this.managementTestRule.createMembers();
    this.managementTestRule.createManagers();

    class NotificationHubTestListener implements NotificationListener {

      @Override
      public synchronized void handleNotification(Notification notification, Object handback) {
        logger.info("Notification received {}", notification);
        notifications.add(notification);
      }
    }

    this.managerVM.invoke("addListenerToMemberMXBean", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();

      await().until(() -> assertThat(distributedSystemMXBean.listMemberObjectNames()).hasSize(5));

      for (ObjectName objectName : distributedSystemMXBean.listMemberObjectNames()) {
        NotificationHubTestListener listener = new NotificationHubTestListener();
        getPlatformMBeanServer().addNotificationListener(objectName, listener, null, null);
        notificationListenerMap.put(objectName, listener);
      }
    });

    // Check in all VMS

    for (VM memberVM : this.memberVMs) {
      memberVM.invoke("checkNotificationHubListenerCount", () -> {
        SystemManagementService service = this.managementTestRule.getSystemManagementService();
        NotificationHub notificationHub = service.getNotificationHub();
        Map<ObjectName, NotificationHubListener> listenerMap =
            notificationHub.getListenerObjectMap();
        assertThat(listenerMap.keySet()).hasSize(1);

        ObjectName memberMBeanName =
            getMemberMBeanName(this.managementTestRule.getDistributedMember());
        NotificationHubListener listener = listenerMap.get(memberMBeanName);

        /*
         * Counter of listener should be 2 . One for default Listener which is added for each member
         * mbean by distributed system mbean One for the added listener in test
         */
        assertThat(listener.getNumCounter()).isEqualTo(2);

        // Raise some notifications

        NotificationBroadcasterSupport notifier = (MemberMBean) service.getMemberMXBean();
        String memberSource = getMemberNameOrId(this.managementTestRule.getDistributedMember());

        // Only a dummy notification , no actual region is created
        Notification notification = new Notification(JMXNotificationType.REGION_CREATED,
            memberSource, SequenceNumber.next(), System.currentTimeMillis(),
            ManagementConstants.REGION_CREATED_PREFIX + "/test");
        notifier.sendNotification(notification);
      });
    }

    this.managerVM.invoke("checkNotificationsAndRemoveListeners", () -> {
      await().until(() -> assertThat(notifications).hasSize(3));

      notifications.clear();

      for (ObjectName objectName : notificationListenerMap.keySet()) {
        NotificationListener listener = notificationListenerMap.get(objectName);
        getPlatformMBeanServer().removeNotificationListener(objectName, listener);
      }
    });

    // Check in all VMS again

    for (VM memberVM : this.memberVMs) {
      memberVM.invoke("checkNotificationHubListenerCountAgain", () -> {
        SystemManagementService service = this.managementTestRule.getSystemManagementService();
        NotificationHub hub = service.getNotificationHub();
        Map<ObjectName, NotificationHubListener> listenerObjectMap = hub.getListenerObjectMap();
        assertThat(listenerObjectMap.keySet().size()).isEqualTo(1);

        ObjectName memberMBeanName =
            getMemberMBeanName(this.managementTestRule.getDistributedMember());
        NotificationHubListener listener = listenerObjectMap.get(memberMBeanName);

        /*
         * Counter of listener should be 1 for the default Listener which is added for each member
         * mbean by distributed system mbean.
         */
        assertThat(listener.getNumCounter()).isEqualTo(1);
      });
    }

    this.managerVM.invoke("removeListenerFromMemberMXBean", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();

      await().until(() -> assertThat(distributedSystemMXBean.listMemberObjectNames()).hasSize(5));

      for (ObjectName objectName : distributedSystemMXBean.listMemberObjectNames()) {
        NotificationHubTestListener listener = new NotificationHubTestListener();
        try {
          getPlatformMBeanServer().removeNotificationListener(objectName, listener); // because new
                                                                                     // instance!!
        } catch (ListenerNotFoundException e) {
          // TODO: [old] apparently there is never a notification listener on any these mbeans at
          // this point [fix this]
          // fix this test so it doesn't hit these unexpected exceptions -- getLogWriter().error(e);
        }
      }
    });

    for (VM memberVM : this.memberVMs) {
      memberVM.invoke("verifyNotificationHubListenersWereRemoved", () -> {
        SystemManagementService service = this.managementTestRule.getSystemManagementService();
        NotificationHub notificationHub = service.getNotificationHub();
        notificationHub.cleanUpListeners();
        assertThat(notificationHub.getListenerObjectMap()).isEmpty();

        for (ObjectName objectName : notificationListenerMap.keySet()) {
          NotificationListener listener = notificationListenerMap.get(objectName);
          assertThatThrownBy(
              () -> getPlatformMBeanServer().removeNotificationListener(objectName, listener))
                  .isExactlyInstanceOf(ListenerNotFoundException.class);
        }
      });
    }
  }

  /**
   * Tests each and every operations that is defined on the MemberMXBean
   */
  @Test
  public void testAlert() throws Exception {
    this.managementTestRule.createManager(this.managerVM);
    addAlertListener(this.managerVM);
    resetAlertCounts(this.managerVM);

    DistributedMember managerDistributedMember =
        this.managementTestRule.getDistributedMember(this.managerVM);

    generateWarningAlert(this.managerVM);
    generateSevereAlert(this.managerVM);
    verifyAlertCount(this.managerVM, 1, 0);
    resetAlertCounts(this.managerVM);

    for (VM memberVM : this.memberVMs) {
      this.managementTestRule.createMember(memberVM);

      verifyAlertAppender(memberVM, managerDistributedMember, Alert.SEVERE);

      generateWarningAlert(memberVM);
      generateSevereAlert(memberVM);
    }

    verifyAlertCount(this.managerVM, 3, 0);
    resetAlertCounts(this.managerVM);
    setAlertLevel(this.managerVM, AlertDetails.getAlertLevelAsString(Alert.WARNING));

    for (VM memberVM : this.memberVMs) {
      verifyAlertAppender(memberVM, managerDistributedMember, Alert.WARNING);
      generateWarningAlert(memberVM);
      generateSevereAlert(memberVM);
    }

    verifyAlertCount(this.managerVM, 3, 3);

    resetAlertCounts(this.managerVM);

    setAlertLevel(this.managerVM, AlertDetails.getAlertLevelAsString(Alert.OFF));

    for (VM memberVM : this.memberVMs) {
      verifyAlertAppender(memberVM, managerDistributedMember, Alert.OFF);
      generateWarningAlert(memberVM);
      generateSevereAlert(memberVM);
    }

    verifyAlertCount(this.managerVM, 0, 0);
  }

  private void verifyAlertAppender(final VM memberVM, final DistributedMember member,
      final int alertLevel) {
    memberVM.invoke("verifyAlertAppender",
        () -> await().until(
            () -> assertThat(AlertAppender.getInstance().hasAlertListener(member, alertLevel))
                .isTrue()));
  }

  private void verifyAlertCount(final VM managerVM, final int expectedSevereAlertCount,
      final int expectedWarningAlertCount) {
    managerVM.invoke("verifyAlertCount", () -> {
      AlertNotificationListener listener = AlertNotificationListener.getInstance();

      await().until(
          () -> assertThat(listener.getSevereAlertCount()).isEqualTo(expectedSevereAlertCount));
      await().until(
          () -> assertThat(listener.getWarningAlertCount()).isEqualTo(expectedWarningAlertCount));
    });
  }

  private void setAlertLevel(final VM managerVM, final String alertLevel) {
    managerVM.invoke("setAlertLevel", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
      distributedSystemMXBean.changeAlertLevel(alertLevel);
    });
  }

  private void generateWarningAlert(final VM anyVM) {
    anyVM.invoke("generateWarningAlert", () -> {
      IgnoredException ignoredException = addIgnoredException(WARNING_LEVEL_MESSAGE);
      logger.warn(WARNING_LEVEL_MESSAGE);
      ignoredException.remove();
    });
  }

  private void resetAlertCounts(final VM managerVM) {
    managerVM.invoke("resetAlertCounts", () -> {
      AlertNotificationListener listener = AlertNotificationListener.getInstance();
      listener.resetCount();
    });
  }

  private void generateSevereAlert(final VM anyVM) {
    anyVM.invoke("generateSevereAlert", () -> {
      IgnoredException ignoredException = addIgnoredException(SEVERE_LEVEL_MESSAGE);
      logger.fatal(SEVERE_LEVEL_MESSAGE);
      ignoredException.remove();
    });
  }

  private void addAlertListener(final VM managerVM) {
    managerVM.invoke("addAlertListener", () -> {
      AlertNotificationListener listener = AlertNotificationListener.getInstance();
      listener.resetCount();

      NotificationFilter notificationFilter = (Notification notification) -> notification.getType()
          .equals(JMXNotificationType.SYSTEM_ALERT);

      getPlatformMBeanServer().addNotificationListener(getDistributedSystemName(), listener,
          notificationFilter, null);
    });
  }

  /**
   * Check aggregate related functions and attributes
   */
  private void verifyDistributedSystemMXBean(final VM managerVM) {
    managerVM.invoke("verifyDistributedSystemMXBean", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();

      await().until(() -> assertThat(distributedSystemMXBean.getMemberCount()).isEqualTo(5));

      Set<DistributedMember> otherMemberSet = this.managementTestRule.getOtherNormalMembers();
      for (DistributedMember member : otherMemberSet) {
        // TODO: create some assertions (this used to just print JVMMetrics and OSMetrics)
      }
    });
  }

  private void addNotificationListener(final VM managerVM) {
    managerVM.invoke("addNotificationListener", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
      assertThat(distributedSystemMXBean).isNotNull();

      DistributedSystemNotificationListener listener = new DistributedSystemNotificationListener();
      getPlatformMBeanServer().addNotificationListener(getDistributedSystemName(), listener, null,
          null);
    });
  }

  private void shutDownAll(final VM managerVM) {
    managerVM.invoke("shutDownAll", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
      distributedSystemMXBean.shutDownAllMembers();

      await().until(() -> assertThat(this.managementTestRule.getOtherNormalMembers()).hasSize(0));
    });
  }

  private void verifyFetchMemberObjectName(final VM managerVM, final int memberCount) {
    managerVM.invoke("verifyFetchMemberObjectName", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();

      await().until(
          () -> assertThat(distributedSystemMXBean.listMemberObjectNames()).hasSize(memberCount));

      String memberId = this.managementTestRule.getDistributedMember().getId();
      ObjectName thisMemberName = getMemberMBeanName(memberId);
      ObjectName memberName = distributedSystemMXBean.fetchMemberObjectName(memberId);
      assertThat(memberName).isEqualTo(thisMemberName);
    });
  }

  private ConditionFactory await() {
    return Awaitility.await().atMost(2, MINUTES);
  }

  private static class DistributedSystemNotificationListener implements NotificationListener {

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
      assertThat(notification).isNotNull();
    }
  }

  private static class AlertNotificationListener implements NotificationListener {

    private static AlertNotificationListener listener = new AlertNotificationListener();

    private int warningAlertCount = 0;

    private int severeAlertCount = 0;

    static AlertNotificationListener getInstance() { // TODO: get rid of singleton
      return listener;
    }

    @Override
    public synchronized void handleNotification(final Notification notification,
        final Object handback) {
      assertThat(notification).isNotNull();

      Map<String, String> notificationUserData = (Map<String, String>) notification.getUserData();

      if (notificationUserData.get(JMXNotificationUserData.ALERT_LEVEL)
          .equalsIgnoreCase("warning")) {
        assertThat(notification.getMessage()).isEqualTo(WARNING_LEVEL_MESSAGE);
        warningAlertCount++;
      }
      if (notificationUserData.get(JMXNotificationUserData.ALERT_LEVEL)
          .equalsIgnoreCase("severe")) {
        assertThat(notification.getMessage()).isEqualTo(SEVERE_LEVEL_MESSAGE);
        severeAlertCount++;
      }
    }

    void resetCount() {
      warningAlertCount = 0;
      severeAlertCount = 0;
    }

    int getWarningAlertCount() {
      return warningAlertCount;
    }

    int getSevereAlertCount() {
      return severeAlertCount;
    }
  }
}
