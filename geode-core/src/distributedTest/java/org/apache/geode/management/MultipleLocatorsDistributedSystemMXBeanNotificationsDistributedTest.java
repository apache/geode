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
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.management.internal.ManagementConstants.REGION_CREATED_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.InstanceNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.ManagementTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.management.NotificationAssert;
import org.apache.geode.test.management.NotificationBuilder;

@Category(ManagementTest.class)
@SuppressWarnings("serial")
public class MultipleLocatorsDistributedSystemMXBeanNotificationsDistributedTest
    implements Serializable {

  private static final String MANAGER_NAME = "locatorVM";
  private static final String MEMBER_NAME = "memberVM-";

  /** One NotificationListener is added for the DistributedSystemMXBean in Locator VM. */
  private static final int ONE_LISTENER_FOR_MANAGER = 1;

  /** One NotificationListener is added for spying by the test. */
  private static final int ONE_LISTENER_FOR_SPYING = 1;

  /** 2 Server VMs and 2 Locator VMs. */
  private static final int CLUSTER_SIZE = 4;

  /** 2 Server VMs. */
  private static final int TWO_SERVERS = 2;

  private static final LocatorLauncher DUMMY_LOCATOR = mock(LocatorLauncher.class);
  private static final ServerLauncher DUMMY_SERVER = mock(ServerLauncher.class);

  private static final AtomicReference<LocatorLauncher> LOCATOR =
      new AtomicReference<>(DUMMY_LOCATOR);
  private static final AtomicReference<ServerLauncher> SERVER =
      new AtomicReference<>(DUMMY_SERVER);

  private static InternalCache cache;
  private static InternalDistributedMember distributedMember;
  private static SystemManagementService managementService;
  private static NotificationListener notificationListener;
  private static DistributedSystemMXBean distributedSystemMXBean;

  private int locator1Port;
  private int locator2Port;
  private int locatorJmx1Port;
  private int locatorJmx2Port;
  private String locator1Name;
  private String locator2Name;
  private String server1Name;
  private String server2Name;
  private String locators;
  private File server1Dir;
  private File server2Dir;
  private String regionName;

  private VM locatorVM1;
  private VM locatorVM2;
  private VM serverVM1;
  private VM serverVM2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    locatorVM1 = getVM(0);
    locatorVM2 = getVM(1);
    serverVM1 = getVM(2);
    serverVM2 = getVM(-1);

    regionName = "region";
    locator1Name = "locator1";
    locator2Name = "locator2";
    server1Name = "server1";
    server2Name = "server2";
    File locator1Dir = temporaryFolder.newFolder(locator1Name);
    File locator2Dir = temporaryFolder.newFolder(locator2Name);
    server1Dir = temporaryFolder.newFolder(server1Name);
    server2Dir = temporaryFolder.newFolder(server2Name);

    int[] port = getRandomAvailableTCPPorts(4);
    locator1Port = port[0];
    locator2Port = port[1];
    locatorJmx1Port = port[2];
    locatorJmx2Port = port[3];
    locators = "localhost[" + locator1Port + "],localhost[" + locator2Port + "]";

    locatorVM1.invoke(() -> {
      startLocator(locator1Name, locator1Dir, locator1Port, locatorJmx1Port, locators);
    });
    locatorVM2.invoke(() -> {
      startLocator(locator2Name, locator2Dir, locator2Port, locatorJmx2Port, locators);
    });

    // serverVM1.invoke(() -> startServer(serverName(serverVM1), server1Dir, locators));
    // serverVM2.invoke(() -> startServer(serverName(serverVM2), server2Dir, locators));
  }

  @After
  public void tearDown() throws Exception {
    for (VM vm : toArray(serverVM1, serverVM2, locatorVM2, locatorVM1)) {
      vm.invoke(() -> {
        LOCATOR.getAndSet(DUMMY_LOCATOR).stop();
        SERVER.getAndSet(DUMMY_SERVER).stop();
        cache = null;
        distributedMember = null;
        managementService = null;
        distributedSystemMXBean = null;
      });
    }
  }

  @Test
  public void notificationsForLocator2OnLocator1() {
    locatorVM1.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).times(3))
          .handleNotification(captor.capture(), isNull());

      NotificationBuilder notifications = new NotificationBuilder();

      NotificationAssert.assertThat(captor.getAllValues())
          .containsExactly(
              notifications
                  .source("locator2")
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined locator2")
                  .userData(null)
                  .create(),
              notifications
                  .source("locator2")
                  .type("gemfire.distributedsystem.cache.lockservice.created")
                  .message("LockService Created With Name __CLUSTER_CONFIG_LS")
                  .userData("locator2")
                  .create(),
              notifications
                  .source("locator2")
                  .type("gemfire.distributedsystem.cache.disk.created")
                  .message("DiskStore Created With Name cluster_config")
                  .userData("locator2")
                  .create());
    });
  }

  @Test
  @Ignore("zero interactions with this mock")
  public void notificationsForLocator1OnLocator2() {
    locatorVM2.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).times(3))
          .handleNotification(captor.capture(), isNull());

      NotificationBuilder notifications = new NotificationBuilder();

      NotificationAssert.assertThat(captor.getAllValues())
          .containsExactly(
              notifications
                  .source("locator1")
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined locator1")
                  .userData(null)
                  .create(),
              notifications
                  .source("locator1")
                  .type("gemfire.distributedsystem.cache.lockservice.created")
                  .message("LockService Created With Name __CLUSTER_CONFIG_LS")
                  .userData("locator1")
                  .create(),
              notifications
                  .source("locator1")
                  .type("gemfire.distributedsystem.cache.disk.created")
                  .message("DiskStore Created With Name cluster_config")
                  .userData("locator1")
                  .create());
    });
  }

  @Test
  public void notificationsForStartServerOnLocators() {
    locatorVM1.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).times(3))
          .handleNotification(captor.capture(), isNull());

      reset(notificationListener);
    });

    serverVM1.invoke(() -> startServer(server1Name, server1Dir, locators));
    serverVM2.invoke(() -> startServer(server2Name, server2Dir, locators));

    locatorVM1.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).times(2))
          .handleNotification(captor.capture(), isNull());

      NotificationBuilder notifications = new NotificationBuilder();

      NotificationAssert.assertThat(captor.getAllValues())
          .containsExactly(
              notifications
                  .source("server1")
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined server1")
                  .userData("server1")
                  .create(),
              notifications
                  .source("server2")
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined server2")
                  .userData("server2")
                  .create());
    });
  }

  @Test
  public void gemfire_distributedsystem_cache_region_created() {
    locatorVM1.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).times(3))
          .handleNotification(captor.capture(), isNull());

      reset(notificationListener);
    });

    // serverVM1.invoke(() -> startServer(server1Name, server1Dir, locators));
    serverVM2.invoke(() -> startServer(server2Name, server2Dir, locators));

    locatorVM1.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).times(1))
          .handleNotification(captor.capture(), isNull());

      reset(notificationListener);
    });

    // for (VM memberVM : toArray(serverVM1, serverVM2)) {
    // memberVM.invoke(() -> {
    // cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    // });
    // }

    serverVM2.invoke(() -> {
      Region<?, ?> region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      InternalRegion internalRegion = (InternalRegion) region;
      assertThat(internalRegion.isInternalRegion()).isFalse();
      assertThat(internalRegion.isUsedForMetaRegion()).isFalse();
    });

    locatorVM1.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).times(1))
          .handleNotification(captor.capture(), isNull());

      NotificationBuilder notifications = new NotificationBuilder();

      NotificationAssert.assertThat(captor.getAllValues())
          .containsExactly(
              // notifications
              // .source("server1")
              // .type("gemfire.distributedsystem.cache.region.created")
              // .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
              // .userData("server1")
              // .create(),
              notifications
                  .source("server2")
                  .type("gemfire.distributedsystem.cache.region.created")
                  .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
                  .userData("server2")
                  .create());

      // for (Notification notification : captor.getAllValues()) {
      // assertThat(notification.getType())
      // .isEqualTo(GEMFIRE_PREFIX + "distributedsystem.cache.member.joined");
      // assertThat(notification.getMessage())
      // .isEqualTo(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName);
      // assertThat(notification.getSource())
      // .isIn("server1", "server2");
      // assertThat(notification.getUserData())
      // .isIn(server1Name, server2Name);
      // }

      // for (ObjectName objectName : distributedSystemMXBean.listMemberObjectNames()) {
      // getPlatformMBeanServer().removeNotificationListener(objectName, notificationListener);
      // }
    });

    // verify each Member VM has just one listener for DistributedSystemMXBean
    // for (VM memberVM : toArray(serverVM1, serverVM2, serverVM3)) {
    // memberVM.invoke(() -> {
    // Map<ObjectName, NotificationHubListener> listenerObjectMap =
    // managementService.getNotificationHub().getListenerObjectMap();
    // NotificationHubListener hubListener =
    // listenerObjectMap.get(getMemberMBeanName(distributedMember));
    //
    // assertThat(hubListener.getNumCounter()).isEqualTo(ONE_LISTENER_FOR_MANAGER);
    // });
    // }

    // verify NotificationHub#cleanUpListeners() behavior in each Member VM
    // for (VM memberVM : toArray(serverVM1, serverVM2, serverVM3)) {
    // memberVM.invoke(() -> {
    // NotificationHub notificationHub = managementService.getNotificationHub();
    // notificationHub.cleanUpListeners();
    //
    // assertThat(notificationHub.getListenerObjectMap()).isEmpty();
    // });
    // }
  }

  private static void startLocator(String name, File workingDirectory, int locatorPort, int jmxPort,
      String locators)
      throws InstanceNotFoundException {
    LOCATOR.set(new LocatorLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setMemberName(name)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .build());

    LOCATOR.get().start();

    awaitSharedConfigurationRunning((InternalLocator) LOCATOR.get().getLocator());

    cache = (InternalCache) LOCATOR.get().getCache();

    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
    notificationListener = spy(NotificationListener.class);
    distributedSystemMXBean = managementService.getDistributedSystemMXBean();

    ObjectName objectName = managementService.getDistributedSystemMBeanName();
    getPlatformMBeanServer().addNotificationListener(objectName, notificationListener, null, null);
  }

  private static void startServer(String name, File workingDirectory, String locators) {
    SERVER.set(new ServerLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setDisableDefaultServer(true)
        .setMemberName(name)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .build());

    SERVER.get().start();

    cache = (InternalCache) SERVER.get().getCache();
    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
  }

  private static void awaitSharedConfigurationRunning(InternalLocator locator) {
    await().untilAsserted(() -> {
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private static Duration getTimeout() {
    return Duration.ofSeconds(120);
  }
}
