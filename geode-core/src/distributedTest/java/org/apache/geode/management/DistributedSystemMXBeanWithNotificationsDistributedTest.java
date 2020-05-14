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
import static org.apache.geode.management.internal.MBeanJMXAdapter.getMemberNameOrUniqueId;
import static org.apache.geode.management.internal.ManagementConstants.REGION_CREATED_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
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
 * <p>
 * TODO: test all notifications emitted by DistributedSystemMXBean:
 *
 * <pre>
 * a) gemfire.distributedsystem.member.joined
 * b) gemfire.distributedsystem.member.left
 * c) gemfire.distributedsystem.member.suspect
 * d) All notifications emitted by MemberMXBeans
 * </pre>
 */
@Category(ManagementTest.class)
@SuppressWarnings("serial")
public class DistributedSystemMXBeanWithNotificationsDistributedTest implements Serializable {

  private static final long TIMEOUT = getTimeout().toMillis();
  private static final String MANAGER_NAME = "managerVM";
  private static final String MEMBER_NAME = "memberVM-";

  /** One NotificationListener is added for the DistributedSystemMXBean in Manager VM. */
  private static final int ONE_LISTENER_FOR_MANAGER = 1;

  /** One NotificationListener is added for spying by the test. */
  private static final int ONE_LISTENER_FOR_SPYING = 1;

  /** Three Member VMs, one Manager VM and one DUnit Locator VM. */
  private static final int CLUSTER_SIZE = 5;

  /** Three Member VMs. */
  private static final int THREE_MEMBERS = 3;

  private static InternalCache cache;
  private static InternalDistributedMember distributedMember;
  private static SystemManagementService managementService;
  private static NotificationListener notificationListener;
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
        distributedSystemMXBean = null;
      });
    }
  }

  @Test
  public void testNotificationHub() {
    // add spy notificationListener to each MemberMXBean in cluster
    managerVM.invoke(() -> {
      // wait until Manager VM has MemberMXBean for each node in cluster
      await().untilAsserted(
          () -> assertThat(distributedSystemMXBean.listMemberObjectNames()).hasSize(CLUSTER_SIZE));

      for (ObjectName objectName : distributedSystemMXBean.listMemberObjectNames()) {
        getPlatformMBeanServer().addNotificationListener(objectName, notificationListener, null,
            null);
      }
    });

    // verify each Member VM has one spy listener in addition to one for DistributedSystemMXBean
    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        Map<ObjectName, NotificationHubListener> listenerObjectMap =
            managementService.getNotificationHub().getListenerObjectMap();
        NotificationHubListener hubListener =
            listenerObjectMap.get(getMemberMBeanName(distributedMember));

        assertThat(hubListener.getNumCounter())
            .isEqualTo(ONE_LISTENER_FOR_SPYING + ONE_LISTENER_FOR_MANAGER);
      });
    }

    // send a dummy notification from each Member VM (no actual region is created)
    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        Notification notification =
            new Notification(REGION_CREATED, getMemberNameOrUniqueId(distributedMember),
                SequenceNumber.next(), System.currentTimeMillis(), REGION_CREATED_PREFIX + "/test");
        NotificationBroadcasterSupport notifier = (MemberMBean) managementService.getMemberMXBean();
        notifier.sendNotification(notification);
      });
    }

    // remove spy notificationListener from each MemberMXBean in cluster
    managerVM.invoke(() -> {
      verify(notificationListener, timeout(TIMEOUT).times(THREE_MEMBERS))
          .handleNotification(isA(Notification.class), isNull());

      for (ObjectName objectName : distributedSystemMXBean.listMemberObjectNames()) {
        getPlatformMBeanServer().removeNotificationListener(objectName, notificationListener);
      }
    });

    // verify each Member VM has just one listener for DistributedSystemMXBean
    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        Map<ObjectName, NotificationHubListener> listenerObjectMap =
            managementService.getNotificationHub().getListenerObjectMap();
        NotificationHubListener hubListener =
            listenerObjectMap.get(getMemberMBeanName(distributedMember));

        assertThat(hubListener.getNumCounter()).isEqualTo(ONE_LISTENER_FOR_MANAGER);
      });
    }

    // verify NotificationHub#cleanUpListeners() behavior in each Member VM
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
    config.setProperty(NAME, MANAGER_NAME);
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(HTTP_SERVICE_PORT, "0");

    cache = (InternalCache) new CacheFactory(config).create();
    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
    notificationListener = spy(NotificationListener.class);

    distributedSystemMXBean = managementService.getDistributedSystemMXBean();
  }

  private void createMember(int vmId) {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, MEMBER_NAME + vmId);
    config.setProperty(JMX_MANAGER, "false");

    cache = (InternalCache) new CacheFactory(config).create();
    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
  }
}
