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
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.management.JMXNotificationType.REGION_CREATED;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getMemberMBeanName;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getMemberNameOrId;
import static org.apache.geode.management.internal.ManagementConstants.REGION_CREATED_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.standalone.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.NotificationHub;
import org.apache.geode.management.internal.NotificationHub.NotificationHubListener;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.MemberMBean;
import org.apache.geode.management.internal.beans.SequenceNumber;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.ManagementTest;

/**
 * Distributed tests for {@link DistributedSystemMXBean} notifications. Extracted from
 * {@link DistributedSystemMXBeanDistributedTest}.
 *
 * <pre>
 * a) gemfire.distributedsystem.member.joined
 * b) gemfire.distributedsystem.member.left
 * c) gemfire.distributedsystem.member.suspect
 * d) All notifications emitted by member mbeans
 * d) Alerts
 * </pre>
 */
@Category(ManagementTest.class)
@SuppressWarnings("serial")
public class DistributedSystemMXBeanWithNotificationsDistributedTest implements Serializable {

  private static InternalCache cache;
  private static InternalDistributedMember distributedMember;
  private static SystemManagementService managementService;
  private static List<Notification> notifications;
  private static Map<ObjectName, NotificationListener> notificationListenerMap;
  private static DistributedSystemMXBean distributedSystemMXBean;

  private VM managerVM;
  private VM memberVM1;
  private VM memberVM2;
  private VM memberVM3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    managerVM = getVM(0);
    memberVM1 = getVM(1);
    memberVM2 = getVM(2);
    memberVM3 = getVM(3);

    managerVM.invoke(() -> createManager());

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> createMember(memberVM.getId()));
    }
  }

  @After
  public void tearDown() throws Exception {
    for (VM vm : toArray(managerVM, memberVM1, memberVM2, memberVM3)) {
      vm.invoke(() -> {
        if (cache != null) {
          cache.close();
        }
        cache = null;
        distributedMember = null;
        managementService = null;
        notifications = null;
        notificationListenerMap = null;
        distributedSystemMXBean = null;
      });
    }
  }

  @Test
  public void testNotificationHub() {
    managerVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(distributedSystemMXBean.listMemberObjectNames()).hasSize(5));

      for (ObjectName objectName : distributedSystemMXBean.listMemberObjectNames()) {
        SpyNotificationListener listener = new SpyNotificationListener();
        getPlatformMBeanServer().addNotificationListener(objectName, listener, null, null);
        notificationListenerMap.put(objectName, listener);
      }
    });

    // Check in all VMS

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        NotificationHub notificationHub = managementService.getNotificationHub();
        Map<ObjectName, NotificationHubListener> listenerMap =
            notificationHub.getListenerObjectMap();
        assertThat(listenerMap.keySet()).hasSize(1);

        ObjectName memberMBeanName = getMemberMBeanName(distributedMember);
        NotificationHubListener listener = listenerMap.get(memberMBeanName);

        /*
         * Counter of listener should be 2 . One for default Listener which is added for each member
         * mbean by distributed system mbean One for the added listener in test
         */
        assertThat(listener.getNumCounter()).isEqualTo(2);

        // Raise some notifications

        NotificationBroadcasterSupport notifier = (MemberMBean) managementService.getMemberMXBean();
        String memberSource = getMemberNameOrId(distributedMember);

        // Only a dummy notification , no actual region is created
        Notification notification = new Notification(REGION_CREATED,
            memberSource, SequenceNumber.next(), System.currentTimeMillis(),
            REGION_CREATED_PREFIX + "/test");
        notifier.sendNotification(notification);
      });
    }

    managerVM.invoke(() -> {
      await().untilAsserted(() -> assertThat(notifications).hasSize(3));

      notifications.clear();

      for (ObjectName objectName : notificationListenerMap.keySet()) {
        NotificationListener listener = notificationListenerMap.get(objectName);
        getPlatformMBeanServer().removeNotificationListener(objectName, listener);
      }
    });

    // Check in all VMS again

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        NotificationHub hub = managementService.getNotificationHub();
        Map<ObjectName, NotificationHubListener> listenerObjectMap = hub.getListenerObjectMap();

        assertThat(listenerObjectMap.keySet()).hasSize(1);

        NotificationHubListener listener =
            listenerObjectMap.get(getMemberMBeanName(distributedMember));

        /*
         * Counter of listener should be 1 for the default Listener which is added for each member
         * mbean by distributed system mbean.
         */
        assertThat(listener.getNumCounter()).isEqualTo(1);
      });
    }

    managerVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(distributedSystemMXBean.listMemberObjectNames()).hasSize(5));
    });

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        NotificationHub notificationHub = managementService.getNotificationHub();
        notificationHub.cleanUpListeners();

        assertThat(notificationHub.getListenerObjectMap()).isEmpty();
      });
    }
  }

  private void createManager() {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, "managerVM");
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(HTTP_SERVICE_PORT, "0");

    cache = (InternalCache) new CacheFactory(config).create();
    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
    notifications = Collections.synchronizedList(new ArrayList<>());
    notificationListenerMap = Collections.synchronizedMap(new HashMap<>());

    distributedSystemMXBean = managementService.getDistributedSystemMXBean();
  }

  private void createMember(int vmId) {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, "memberVM-" + vmId);
    config.setProperty(JMX_MANAGER, "false");

    cache = (InternalCache) new CacheFactory(config).create();
    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
  }

  // TODO:KIRK: convert to mockito spy
  private static class SpyNotificationListener implements NotificationListener {

    @Override
    public synchronized void handleNotification(Notification notification, Object handback) {
      notifications.add(notification);
    }
  }
}
