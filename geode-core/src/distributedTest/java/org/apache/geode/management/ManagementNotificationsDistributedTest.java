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
import static java.util.Arrays.asList;
import static javax.management.ObjectName.getInstance;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.management.internal.ManagementConstants.REGION_CREATED_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.ManagementTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.management.NotificationAssert;
import org.apache.geode.test.management.NotificationBuilder;

@Category(ManagementTest.class)
@SuppressWarnings("serial")
public class ManagementNotificationsDistributedTest implements Serializable {

  private static final String MANAGER_NAME = "locatorVM";
  private static final String MEMBER_NAME = "memberVM-";

  /** One NotificationListener is added for the DistributedSystemMXBean in Locator VM. */
  private static final int ONE_LISTENER_FOR_MANAGER = 1;

  /** One NotificationListener is added for spying by the test. */
  private static final int ONE_LISTENER_FOR_SPYING = 1;

  /** 2 Server VMs and 1 Locator VMs. */
  private static final int CLUSTER_SIZE = 3;

  /** 2 Server VMs. */
  private static final int TWO_SERVERS = 2;

  private static final LocatorLauncher DUMMY_LOCATOR = mock(LocatorLauncher.class);
  private static final ServerLauncher DUMMY_SERVER = mock(ServerLauncher.class);

  private static final AtomicReference<LocatorLauncher> LOCATOR =
      new AtomicReference<>(DUMMY_LOCATOR);
  private static final AtomicReference<ServerLauncher> SERVER =
      new AtomicReference<>(DUMMY_SERVER);

  private static InternalCache cache;
  private static SystemManagementService managementService;
  private static NotificationListener notificationListener;

  private final NotificationBuilder notifications = new NotificationBuilder();

  private int locatorPort;
  private int locatorJmxPort;
  private String locatorName;
  private String server1Name;
  private String server2Name;
  private String locators;
  private File server1Dir;
  private File server2Dir;
  private String regionName;

  private VM locatorVM;
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
    locatorVM = getVM(0);
    serverVM1 = getVM(2);
    serverVM2 = getVM(-1);

    regionName = "region";
    locatorName = "locator1";
    server1Name = "server1";
    server2Name = "server2";
    File locatorDir = temporaryFolder.newFolder(locatorName);
    server1Dir = temporaryFolder.newFolder(server1Name);
    server2Dir = temporaryFolder.newFolder(server2Name);

    int[] port = getRandomAvailableTCPPorts(2);
    locatorPort = port[0];
    locatorJmxPort = port[1];
    locators = "localhost[" + locatorPort + "]";

    locatorVM.invoke(() -> {
      startLocator(locatorName, locatorDir, locatorPort, locatorJmxPort, locators);
    });
  }

  @After
  public void tearDown() throws Exception {
    for (VM vm : toArray(serverVM1, serverVM2, locatorVM)) {
      vm.invoke(() -> {
        LOCATOR.getAndSet(DUMMY_LOCATOR).stop();
        SERVER.getAndSet(DUMMY_SERVER).stop();
        cache = null;
        managementService = null;
      });
    }
  }

  @Test
  public void zeroNotificationsByDefault() {
    locatorVM.invoke(() -> {
      verifyNoInteractions(notificationListener);
    });
  }

  @Test
  public void memberJoinedNotificationForEachServer() {
    serverVM1.invoke(() -> startServer(server1Name, server1Dir, locators));
    serverVM2.invoke(() -> startServer(server2Name, server2Dir, locators));

    locatorVM.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).atLeast(2))
          .handleNotification(captor.capture(), isNull());

      NotificationAssert.assertThat(captor.getAllValues())
          .containsExactly(
              notifications
                  .source(server1Name)
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined " + server1Name)
                  .userData(server1Name)
                  .create(),
              notifications
                  .source(server2Name)
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined " + server2Name)
                  .userData(server2Name)
                  .create());
    });
  }

  @Test
  public void notificationsWhenOneServerCreatesRegion() {
    serverVM1.invoke(() -> {
      startServer(server1Name, server1Dir, locators);
    });

    locatorVM.invoke(() -> {
      awaitMXBeanExists(memberMXBean(server1Name));
      addNotificationListener(memberMXBean(server1Name), notificationListener);
    });

    serverVM1.invoke(() -> {
      Region<?, ?> region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      InternalRegion internalRegion = (InternalRegion) region;

      assertThat(internalRegion.isSecret()).isFalse();
      assertThat(internalRegion.isUsedForMetaRegion()).isFalse();
      assertThat(internalRegion.isUsedForPartitionedRegionAdmin()).isFalse();
      assertThat(internalRegion.isUsedForPartitionedRegionBucket()).isFalse();

      assertThat(internalRegion.isInternalRegion()).isFalse();
    });

    locatorVM.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).atLeast(3))
          .handleNotification(captor.capture(), isNull());

      NotificationAssert.assertThat(captor.getAllValues())
          .containsExactly(
              notifications
                  .source(server1Name)
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined " + server1Name)
                  .userData(server1Name)
                  .create(),
              notifications
                  .source(server1Name)
                  .type("gemfire.distributedsystem.cache.region.created")
                  .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
                  .userData(server1Name)
                  .create(),
              notifications
                  .source(server1Name)
                  .type("gemfire.distributedsystem.cache.region.created")
                  .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
                  .userData(server1Name)
                  .create());
    });
  }

  @Test
  public void notificationsWhenTwoServersCreateRegion() {
    serverVM1.invoke(() -> {
      startServer(server1Name, server1Dir, locators);
      awaitMXBeanExists(memberMXBean(server1Name));
    });
    serverVM2.invoke(() -> {
      startServer(server2Name, server2Dir, locators);
      awaitMXBeanExists(memberMXBean(server2Name));
    });

    locatorVM.invoke(() -> {
      for (String serverName : asList(server1Name, server2Name)) {
        awaitMXBeanExists(memberMXBean(serverName));
        addNotificationListener(memberMXBean(serverName), notificationListener);
      }
    });

    serverVM1.invoke(() -> {
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      awaitMXBeanExists(regionMXBean(server1Name, regionName));
    });
    serverVM2.invoke(() -> {
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      awaitMXBeanExists(regionMXBean(server2Name, regionName));
    });

    locatorVM.invoke(() -> {
      ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationListener, timeout(getTimeout().toMillis()).atLeast(6))
          .handleNotification(captor.capture(), isNull());

      NotificationAssert.assertThat(captor.getAllValues())
          .containsExactly(
              notifications
                  .source(server1Name)
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined " + server1Name)
                  .userData(server1Name)
                  .create(),
              notifications
                  .source(server2Name)
                  .type("gemfire.distributedsystem.cache.member.joined")
                  .message("Member Joined " + server2Name)
                  .userData(server2Name)
                  .create(),
              notifications
                  .source(server1Name)
                  .type("gemfire.distributedsystem.cache.region.created")
                  .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
                  .userData(server1Name)
                  .create(),
              notifications
                  .source(server1Name)
                  .type("gemfire.distributedsystem.cache.region.created")
                  .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
                  .userData(server1Name)
                  .create(),
              notifications
                  .source(server2Name)
                  .type("gemfire.distributedsystem.cache.region.created")
                  .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
                  .userData(server2Name)
                  .create(),
              notifications
                  .source(server2Name)
                  .type("gemfire.distributedsystem.cache.region.created")
                  .message(REGION_CREATED_PREFIX + Region.SEPARATOR + regionName)
                  .userData(server2Name)
                  .create());
    });
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
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
    notificationListener = spy(NotificationListener.class);
    ObjectName objectName = managementService.getDistributedSystemMBeanName();
    addNotificationListener(objectName, notificationListener);
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
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
    notificationListener = spy(NotificationListener.class);
  }

  private static void awaitSharedConfigurationRunning(InternalLocator locator) {
    await().untilAsserted(() -> {
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private static void awaitMXBeanExists(ObjectName objectName) {
    await().untilAsserted(() -> {
      assertThat(getPlatformMBeanServer().queryNames(objectName, null))
          .as("MBean named " + objectName)
          .isNotEmpty();
    });
  }

  private static <T> T awaitMXBeanProxy(ObjectName objectName, Class<T> interfaceClass) {
    AtomicReference<T> value = new AtomicReference<>();
    await().untilAsserted(() -> {
      value.set(JMX.newMXBeanProxy(getPlatformMBeanServer(), objectName, interfaceClass));
      assertThat(value.get()).isNotNull();
    });
    return value.get();
  }

  private static Duration getTimeout() {
    if (true) {
      return GeodeAwaitility.getTimeout();
    }
    return Duration.ofSeconds(60);
  }

  private static ObjectName memberMXBean(String memberName) throws MalformedObjectNameException {
    return getInstance("GemFire:type=Member,member=" + memberName);
  }

  private static ObjectName regionMXBean(String memberName, String regionName)
      throws MalformedObjectNameException {
    return getInstance("GemFire:service=Region,name=/" + regionName + ",type=Member,member=" +
        memberName);
  }

  private static void addNotificationListener(ObjectName objectName, NotificationListener listener)
      throws InstanceNotFoundException {
    getPlatformMBeanServer().addNotificationListener(objectName, listener, null, null);
  }
}
