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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.management.ObjectName.getInstance;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.addReconnectListener;
import static org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper.crashDistributedSystem;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem.ReconnectListener;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(JMXTest.class)
@SuppressWarnings("serial")
public class JMXMBeanReconnectDUnitTest implements Serializable {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();
  private static final LocatorLauncher DUMMY_LOCATOR = mock(LocatorLauncher.class);
  private static final ServerLauncher DUMMY_SERVER = mock(ServerLauncher.class);

  private static final AtomicReference<CountDownLatch> BEFORE =
      new AtomicReference<>(new CountDownLatch(0));
  private static final AtomicReference<CountDownLatch> AFTER =
      new AtomicReference<>(new CountDownLatch(0));

  private static final AtomicReference<LocatorLauncher> LOCATOR =
      new AtomicReference<>(DUMMY_LOCATOR);
  private static final AtomicReference<ServerLauncher> SERVER =
      new AtomicReference<>(DUMMY_SERVER);

  private VM locator1VM;
  private VM locator2VM;
  private VM serverVM;

  private String locator1Name;
  private String locator2Name;
  private String serverName;
  private String locators;
  private int locator1Port;
  private int locator2Port;
  private int locator1JmxPort;
  private int locator2JmxPort;
  private String regionName;
  private Set<ObjectName> mxbeansOnServer;
  private Set<ObjectName> mxbeansOnLocator1;
  private Set<ObjectName> mxbeansOnLocator2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();
  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setUp() throws Exception {
    locator1VM = getVM(1);
    locator2VM = getVM(-1);
    serverVM = getVM(0);

    locator1Name = "locator1";
    locator2Name = "locator2";
    serverName = "server1";
    File locator1Dir = temporaryFolder.newFolder(locator1Name);
    File locator2Dir = temporaryFolder.newFolder(locator2Name);
    File serverDir = temporaryFolder.newFolder(serverName);

    int[] port = getRandomAvailableTCPPorts(4);
    locator1Port = port[0];
    locator2Port = port[1];
    locator1JmxPort = port[2];
    locator2JmxPort = port[3];
    locators = "localhost[" + locator1Port + "],localhost[" + locator2Port + "]";

    locator1VM.invoke(() -> {
      startLocator(locator1Name, locator1Dir, locator1Port, locator1JmxPort, locators);
    });
    locator2VM.invoke(() -> {
      startLocator(locator2Name, locator2Dir, locator2Port, locator2JmxPort, locators);
    });

    serverVM.invoke(() -> startServer(serverName, serverDir, locators));

    gfsh.connectAndVerify(locator1JmxPort, PortType.jmxManager);

    regionName = "region1";
    String createRegionCommand = "create region --type=REPLICATE --name=" + SEPARATOR + regionName;
    gfsh.executeAndAssertThat(createRegionCommand).statusIsSuccess();

    addIgnoredException(CacheClosedException.class);
    addIgnoredException(CancelException.class);
    addIgnoredException(DistributedSystemDisconnectedException.class);
    addIgnoredException(ForcedDisconnectException.class);
    addIgnoredException(MemberDisconnectedException.class);
    addIgnoredException("Possible loss of quorum");

    mxbeansOnServer = serverVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .containsAll(expectedServerMXBeans(serverName, regionName));
      });
      return getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null);
    });

    mxbeansOnLocator1 = locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .containsAll(expectedServerMXBeans(serverName, regionName))
            .containsAll(expectedLocatorMXBeans(locator1Name))
            .containsAll(expectedLocatorMXBeans(locator2Name))
            .containsAll(expectedDistributedMXBeans(regionName));
      });

      return getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null);
    });

    mxbeansOnLocator2 = locator2VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator2")
            .containsAll(expectedServerMXBeans(serverName, regionName))
            .containsAll(expectedLocatorMXBeans(locator2Name))
            .containsAll(expectedLocatorMXBeans(locator1Name))
            .containsAll(expectedDistributedMXBeans(regionName));
      });

      return getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null);
    });
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      BEFORE.get().countDown();
      AFTER.get().countDown();
      SERVER.getAndSet(DUMMY_SERVER).stop();
      LOCATOR.getAndSet(DUMMY_LOCATOR).stop();
    });
    disconnectAllFromDS();
  }

  @Test
  public void serverHasMemberTypeMXBeans() {
    serverVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .containsAll(expectedServerMXBeans(serverName, regionName));
      });
    });
  }

  @Test
  public void locatorsHaveMemberTypeMXBeansForServer() {
    for (VM locatorVM : toArray(locator1VM, locator2VM)) {
      locatorVM.invoke(() -> {
        await().untilAsserted(() -> {
          assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
              .as("GemFire mbeans on server1")
              .containsAll(expectedServerMXBeans(serverName, regionName));
        });
      });
    }
  }

  @Test
  public void locatorHasMemberTypeMXBeansForBothLocators() {
    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .containsAll(expectedLocatorMXBeans(locator1Name));
      });
    });

    locator2VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator2")
            .containsAll(expectedLocatorMXBeans(locator2Name));
      });
    });
  }

  @Test
  public void locatorsHaveDistributedTypeMXBeans() {
    for (VM locatorVM : toArray(locator1VM, locator2VM)) {
      locatorVM.invoke(() -> {
        await().untilAsserted(() -> {
          assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
              .as("GemFire mbeans on locator" + getVMId())
              .containsAll(expectedDistributedMXBeans(regionName));
        });
      });
    }
  }

  /**
   * Test that a server's local MBeans are not affected by a locator crashing
   */
  @Test
  public void serverMXBeansOnServerAreUnaffectedByLocatorCrash() {
    locator1VM.invoke(() -> {
      crashDistributedSystem(LOCATOR.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .isEmpty();
      });
    });

    serverVM.invoke(() -> {
      assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
          .as("GemFire mbeans on server1")
          .containsExactlyElementsOf(mxbeansOnServer);
    });

    locator1VM.invoke(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR.get().getLocator();

      await().untilAsserted(() -> {
        boolean isReconnected = locator.isReconnected();
        boolean isSharedConfigurationRunning = locator.isSharedConfigurationRunning();
        Set<ObjectName> mbeanNames =
            getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null);
        assertThat(isReconnected)
            .as("Locator is reconnected on locator1")
            .isTrue();
        assertThat(isSharedConfigurationRunning)
            .as("Locator shared configuration is running on locator1")
            .isTrue();
        assertThat(mbeanNames)
            .as("GemFire mbeans on locator1")
            .isEqualTo(mxbeansOnLocator1);
      });
    });

    serverVM.invoke(() -> {
      assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
          .as("GemFire mbeans on server1")
          .containsExactlyElementsOf(mxbeansOnServer);
    });
  }

  /**
   * Test that a locator's local MBeans are not affected by a server crashing
   */
  @Test
  public void serverMXBeansOnLocatorAreRestoredAfterCrashedServerReturns() {
    serverVM.invoke(() -> {
      crashDistributedSystem(SERVER.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .isEmpty();
      });
    });

    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .doesNotContainAnyElementsOf(mxbeansOnServer);
      });
    });

    serverVM.invoke(() -> {
      InternalCache cache = (InternalCache) SERVER.get().getCache();
      InternalDistributedSystem system = cache.getInternalDistributedSystem();

      await().untilAsserted(() -> {
        assertThat(system.isReconnecting())
            .as("System is reconnecting on server1")
            .isTrue();
      });

      system.waitUntilReconnected(TIMEOUT_MILLIS, MILLISECONDS);

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .containsExactlyElementsOf(mxbeansOnServer);
      });
    });

    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .containsAll(mxbeansOnServer)
            .containsExactlyElementsOf(mxbeansOnLocator1);
      });
    });
  }

  /**
   * Test MBean consistency when disconnecting and reconnecting the lead locator. MBeans should
   * remain the same after a member reconnects as they were before the disconnect. MBeans (other
   * than local MBeans, which are filtered for this test) should be consistent between locators.
   * All MBeans not related to the killed member should remain the same when a member is killed.
   */
  @Test
  public void locatorMXBeansOnOtherLocatorAreRestoredAfterCrashedLocatorReturns() {
    locator1VM.invoke(() -> {
      BEFORE.set(new CountDownLatch(1));

      addReconnectListener(new ReconnectListener() {
        @Override
        public void reconnecting(InternalDistributedSystem oldSystem) {
          try {
            BEFORE.get().await(TIMEOUT_MILLIS, MILLISECONDS);
          } catch (InterruptedException e) {
            errorCollector.addError(e);
          }
        }
      });

      crashDistributedSystem(LOCATOR.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .isEmpty();
      });
    });

    locator2VM.invoke(() -> {
      Collection<ObjectName> locator1MXBeans = new ArrayList<>(mxbeansOnLocator1);
      locator1MXBeans.removeAll(expectedServerMXBeans(serverName, regionName));
      locator1MXBeans.removeAll(expectedLocatorMXBeans(locator2Name));
      locator1MXBeans.removeAll(expectedDistributedMXBeans(regionName));

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator2")
            .doesNotContainAnyElementsOf(locator1MXBeans);
      });
    });

    locator1VM.invoke(() -> {
      BEFORE.get().countDown();

      await().untilAsserted(() -> {
        InternalLocator locator = (InternalLocator) LOCATOR.get().getLocator();

        assertThat(locator.isSharedConfigurationRunning())
            .as("Locator shared configuration is running on locator1")
            .isTrue();
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .containsAll(mxbeansOnLocator1);
      });
    });
  }

  /**
   * Test MBean consistency when disconnecting and reconnecting a server. MBeans should
   * remain the same after a member reconnects as they were before the disconnect. MBeans (other
   * than local MBeans, which are filtered for this test) should be consistent between locators.
   * All MBeans not related to the killed member should remain the same when a member is killed.
   */
  @Test
  public void serverMXBeansAreRestoredOnBothLocatorsAfterCrashedServerReturns() {
    serverVM.invoke(() -> {
      BEFORE.set(new CountDownLatch(1));
      AFTER.set(new CountDownLatch(1));

      addReconnectListener(new ReconnectListener() {
        @Override
        public void reconnecting(InternalDistributedSystem oldSystem) {
          try {
            BEFORE.get().await(TIMEOUT_MILLIS, MILLISECONDS);
          } catch (InterruptedException e) {
            errorCollector.addError(e);
          }
        }

        @Override
        public void onReconnect(InternalDistributedSystem oldSystem,
            InternalDistributedSystem newSystem) {
          AFTER.get().countDown();
        }
      });

      crashDistributedSystem(SERVER.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .isEmpty();
      });
    });

    for (VM locatorVM : toArray(locator1VM, locator2VM)) {
      locatorVM.invoke(() -> {
        await().untilAsserted(() -> {
          assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
              .as("GemFire mbeans on locator" + locatorVM.getId())
              .isNotEmpty()
              .doesNotContainAnyElementsOf(mxbeansOnServer);
        });
      });
    }

    serverVM.invoke(() -> {
      BEFORE.get().countDown();
      AFTER.get().await(TIMEOUT_MILLIS, MILLISECONDS);

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .isEqualTo(mxbeansOnServer);
      });
    });

    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .containsAll(mxbeansOnLocator1);
      });
    });

    locator2VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator2")
            .containsAll(mxbeansOnLocator2);
      });
    });
  }

  private static void startLocator(String name, File workingDirectory, int locatorPort, int jmxPort,
      String locators) {
    LOCATOR.set(new LocatorLauncher.Builder()
        .setMemberName(name)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    LOCATOR.get().start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR.get().getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private static void startServer(String name, File workingDirectory, String locators) {
    SERVER.set(new ServerLauncher.Builder()
        .setDisableDefaultServer(true)
        .setMemberName(name)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    SERVER.get().start();
  }

  private static Set<ObjectName> expectedServerMXBeans(String memberName, String regionName)
      throws MalformedObjectNameException {
    return new HashSet<>(asList(
        getInstance("GemFire:type=Member,member=" + memberName),
        getInstance("GemFire:service=Region,name=/" + regionName + ",type=Member,member=" +
            memberName)));
  }

  private static Set<ObjectName> expectedLocatorMXBeans(String memberName)
      throws MalformedObjectNameException {
    return new HashSet<>(asList(
        getInstance("GemFire:service=DiskStore,name=cluster_config,type=Member,member=" +
            memberName),
        getInstance("GemFire:service=Locator,type=Member,member=" + memberName),
        getInstance("GemFire:service=LockService,name=__CLUSTER_CONFIG_LS,type=Member,member=" +
            memberName),
        getInstance("GemFire:type=Member,member=" + memberName),
        getInstance("GemFire:service=Manager,type=Member,member=" + memberName)));
  }

  private static Set<ObjectName> expectedDistributedMXBeans(String regionName)
      throws MalformedObjectNameException {
    return new HashSet<>(asList(
        getInstance("GemFire:service=AccessControl,type=Distributed"),
        getInstance("GemFire:service=FileUploader,type=Distributed"),
        getInstance("GemFire:service=LockService,name=__CLUSTER_CONFIG_LS,type=Distributed"),
        getInstance("GemFire:service=Region,name=/" + regionName + ",type=Distributed"),
        getInstance("GemFire:service=System,type=Distributed")));
  }
}
