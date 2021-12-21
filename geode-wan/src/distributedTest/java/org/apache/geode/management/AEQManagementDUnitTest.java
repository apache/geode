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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;

@Category({AEQTest.class})
public class AEQManagementDUnitTest implements Serializable {

  private MemberVM locator, server1, server2;

  private final String aeqID = "aeqMBeanTest";
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  public AEQManagementDUnitTest() {
    super();
  }

  @Before
  public void setup() {
    locator = clusterStartupRule.startLocatorVM(0, locatorProperties());
    locator.invoke(() -> startManagerService());
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator.getPort());
  }

  private void startManagerService() {
    SystemManagementService service = (SystemManagementService) ManagementService
        .getManagementService(ClusterStartupRule.getCache());
    service.createManager();
  }

  @Test
  public void testCreateAsyncEventQueueAndVerifyBeansRegistered() {
    server1.invoke(() -> createAsyncEventQueueAndVerifyBeanRegisteration("server1", false));

    server2.invoke(() -> createAsyncEventQueueAndVerifyBeanRegisteration("server2", false));

    final SerializableCallableIF<InternalDistributedMember> memberSerializableCallableIF =
        () -> ClusterStartupRule.getCache().getMyId();

    DistributedMember memberServer1 = server1.invoke(memberSerializableCallableIF);

    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer1);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer1);

      assertThat(aeqBean).isNotNull();
    });

    DistributedMember memberServer2 = server2.invoke(
        memberSerializableCallableIF);

    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer2);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer2);

      assertThat(aeqBean).isNotNull();
    });

  }

  @Test
  public void testDestroyAsyncEventQueueAndVerifyBeansAreUpdated() {
    server1.invoke(() -> createAsyncEventQueueAndVerifyBeanRegisteration("server1", false));

    server2.invoke(() -> createAsyncEventQueueAndVerifyBeanRegisteration("server2", false));

    final SerializableCallableIF<InternalDistributedMember> memberSerializableCallableIF =
        () -> ClusterStartupRule.getCache().getMyId();

    DistributedMember memberServer1 = server1.invoke(memberSerializableCallableIF);
    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer1);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer1);

      assertThat(aeqBean).isNotNull();
    });


    DistributedMember memberServer2 = server2.invoke(memberSerializableCallableIF);
    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer2);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer2);

      assertThat(aeqBean).isNotNull();
    });

    server1.invoke(() -> {
      destroyAsyncEventQueue();
      ManagementService mService = waitForAeqBeanToUnRegister();

      assertThat(mService.getLocalAsyncEventQueueMXBean(aeqID)).isNull();
    });

    server2.invoke(() -> {
      destroyAsyncEventQueue();
      ManagementService mService = waitForAeqBeanToUnRegister();

      assertThat(mService.getLocalAsyncEventQueueMXBean(aeqID)).isNull();
    });


    locator.invoke(() -> {
      waitForProxyBeansRemoval(memberServer1);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer1);

      assertThat(aeqBean).isNull();
    });

    locator.invoke(() -> {
      waitForProxyBeansRemoval(memberServer2);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer2);

      assertThat(aeqBean).isNull();
    });

  }

  @Test
  public void testCreateAEQWithDispatcherInPausedStateAndVerifyUsingMBean() {
    server1.invoke(() -> {
      createAsyncEventQueueAndVerifyBeanRegisteration("server1", true);

      ManagementService mService =
          ManagementService.getManagementService(ClusterStartupRule.getCache());
      AsyncEventQueueMXBean aeqBean = mService.getLocalAsyncEventQueueMXBean(aeqID);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });

    server2.invoke(() -> {
      createAsyncEventQueueAndVerifyBeanRegisteration("server2", true);

      ManagementService mService =
          ManagementService.getManagementService(ClusterStartupRule.getCache());
      AsyncEventQueueMXBean aeqBean = mService.getLocalAsyncEventQueueMXBean(aeqID);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });

    final SerializableCallableIF<InternalDistributedMember> memberSerializableCallableIF =
        () -> ClusterStartupRule.getCache().getMyId();

    DistributedMember memberServer1 = server1.invoke(memberSerializableCallableIF);

    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer1);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer1);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });

    DistributedMember memberServer2 = server2.invoke(
        memberSerializableCallableIF);

    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer2);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer2);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });
  }

  @Test
  public void testCreateAEQWithDispatcherInPausedStateAndResumeAndVerifyUsingMBean() {
    server1.invoke(() -> {
      createAsyncEventQueueAndVerifyBeanRegisteration("server1", true);

      ManagementService mService =
          ManagementService.getManagementService(ClusterStartupRule.getCache());
      AsyncEventQueueMXBean aeqBean = mService.getLocalAsyncEventQueueMXBean(aeqID);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });

    server2.invoke(() -> {
      createAsyncEventQueueAndVerifyBeanRegisteration("server2", true);

      ManagementService mService =
          ManagementService.getManagementService(ClusterStartupRule.getCache());
      AsyncEventQueueMXBean aeqBean = mService.getLocalAsyncEventQueueMXBean(aeqID);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });

    final SerializableCallableIF<InternalDistributedMember> memberSerializableCallableIF =
        () -> ClusterStartupRule.getCache().getMyId();

    DistributedMember memberServer1 = server1.invoke(memberSerializableCallableIF);

    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer1);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer1);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });

    DistributedMember memberServer2 = server2.invoke(
        memberSerializableCallableIF);

    locator.invoke(() -> {
      waitForProxyBeansArrival(memberServer2);

      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer2);

      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    });

    server1.invoke(() -> {
      AsyncEventQueue asyncEventQueue = ClusterStartupRule.getCache().getAsyncEventQueue(aeqID);
      asyncEventQueue.resumeEventDispatching();

      ManagementService mService =
          ManagementService.getManagementService(ClusterStartupRule.getCache());

      GeodeAwaitility.await().untilAsserted(() -> {
        AsyncEventQueueMXBean aeqBean = mService.getLocalAsyncEventQueueMXBean(aeqID);
        assertThat(mService.getLocalAsyncEventQueueMXBean(aeqID)).isNotNull();
        assertThat(aeqBean.isDispatchingPaused()).isEqualTo(false);
      });
    });

    locator.invoke(() -> GeodeAwaitility.await().untilAsserted(() -> {
      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer1);
      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(true);
    }));

    server2.invoke(() -> {
      AsyncEventQueue asyncEventQueue = ClusterStartupRule.getCache().getAsyncEventQueue(aeqID);
      asyncEventQueue.resumeEventDispatching();

      ManagementService mService =
          ManagementService.getManagementService(ClusterStartupRule.getCache());

      GeodeAwaitility.await().untilAsserted(() -> {
        AsyncEventQueueMXBean aeqBean = mService.getLocalAsyncEventQueueMXBean(aeqID);
        assertThat(mService.getLocalAsyncEventQueueMXBean(aeqID)).isNotNull();
        assertThat(aeqBean.isDispatchingPaused()).isEqualTo(false);
      });
    });

    locator.invoke(() -> GeodeAwaitility.await().untilAsserted(() -> {
      AsyncEventQueueMXBean aeqBean = getAsyncEventQueueMXBean(memberServer2);
      assertThat(aeqBean.isDispatchingPaused()).isEqualTo(false);
    }));

  }

  private AsyncEventQueueMXBean getAsyncEventQueueMXBean(DistributedMember memberServer1) {
    SystemManagementService service =
        (SystemManagementService) SystemManagementService
            .getManagementService(ClusterStartupRule.getCache());
    ObjectName queueMBeanName = service.getAsyncEventQueueMBeanName(memberServer1, aeqID);
    return service.getMBeanProxy(queueMBeanName, AsyncEventQueueMXBean.class);
  }

  private void createAsyncEventQueueAndVerifyBeanRegisteration(String diskStoreName,
      boolean dispatcherPaused) {
    createAsyncEventQueue(aeqID, false, 100, 100, false, false,
        diskStoreName, false, dispatcherPaused);
    waitForAeqBeanToRegister();
  }

  private void destroyAsyncEventQueue() {
    AsyncEventQueueImpl aeq =
        (AsyncEventQueueImpl) ClusterStartupRule.getCache().getAsyncEventQueue(aeqID);
    assertThat(aeq).isNotNull();
    aeq.destroy();
  }

  private void waitForProxyBeansArrival(DistributedMember member) {
    GeodeAwaitility.await().untilAsserted(() -> {
      SystemManagementService service =
          (SystemManagementService) SystemManagementService.getManagementService(
              ClusterStartupRule.getCache());
      ObjectName queueMBeanName = service.getAsyncEventQueueMBeanName(member, aeqID);
      assertThat(queueMBeanName).isNotNull();
      assertThat(service.getMBeanProxy(queueMBeanName, AsyncEventQueueMXBean.class)).isNotNull();
    });
  }

  private void waitForProxyBeansRemoval(DistributedMember member) {
    GeodeAwaitility.await().untilAsserted(() -> {
      SystemManagementService service =
          (SystemManagementService) SystemManagementService.getManagementService(
              ClusterStartupRule.getCache());
      ObjectName queueMBeanName = service.getAsyncEventQueueMBeanName(member, aeqID);
      assertThat(service.getMBeanProxy(queueMBeanName, AsyncEventQueueMXBean.class)).isNull();
    });
  }

  private static int getLocatorPort() {
    return Locator.getLocators().get(0).getPort();
  }

  private ManagementService waitForAeqBeanToRegister() {
    ManagementService mService =
        ManagementService.getManagementService(ClusterStartupRule.getCache());

    GeodeAwaitility.await().untilAsserted(() -> {
      assertThat(mService.getLocalAsyncEventQueueMXBean(aeqID)).isNotNull();
    });

    return mService;
  }

  private ManagementService waitForAeqBeanToUnRegister() {
    ManagementService mService =
        ManagementService.getManagementService(ClusterStartupRule.getCache());

    GeodeAwaitility.await().untilAsserted(() -> {
      assertThat(mService.getLocalAsyncEventQueueMXBean(aeqID)).isNull();
    });

    return mService;
  }

  private Properties locatorProperties() {
    int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);

    return props;
  }

  public void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous, boolean dispatcherPaused) {

    if (diskStoreName != null) {
      File directory = new File(
          asyncChannelId + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      directory.mkdir();
      File[] dirs1 = new File[] {directory};
      DiskStoreFactory dsf = ClusterStartupRule.getCache().createDiskStoreFactory();
      dsf.setDiskDirs(dirs1);
      dsf.create(diskStoreName);
    }

    AsyncEventListener asyncEventListener = new MyAsyncEventListener();

    AsyncEventQueueFactory factory = ClusterStartupRule.getCache().createAsyncEventQueueFactory();
    factory.setBatchSize(batchSize);
    factory.setPersistent(isPersistent);
    factory.setDiskStoreName(diskStoreName);
    factory.setDiskSynchronous(isDiskSynchronous);
    factory.setBatchConflationEnabled(isConflation);
    factory.setMaximumQueueMemory(maxMemory);
    factory.setParallel(isParallel);
    factory.setDispatcherThreads(3);
    if (dispatcherPaused) {
      factory.pauseEventDispatching();
    }
    factory.create(asyncChannelId, asyncEventListener);
  }

}
