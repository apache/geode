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
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.alerting.internal.spi.AlertingIOException;
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
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(JMXTest.class)
@SuppressWarnings({"serial", "CodeBlock2Expr", "SameParameterValue"})
public class JmxServerReconnectDistributedTest implements Serializable {

  private static final String LOCATOR_1_NAME = "locator1";
  private static final String LOCATOR_2_NAME = "locator2";
  private static final String SERVER_NAME = "server";
  private static final String REGION_NAME = "region";
  private static final QueryExp QUERY_ALL = null;

  private static final ObjectName GEMFIRE_MXBEANS =
      execute(() -> getInstance("GemFire:*"));
  private static final Set<ObjectName> EXPECTED_SERVER_MXBEANS =
      execute(() -> expectedServerMXBeans(SERVER_NAME, REGION_NAME));
  private static final Set<ObjectName> EXPECTED_LOCATOR_1_MXBEANS =
      execute(() -> expectedLocatorMXBeans(LOCATOR_1_NAME));
  private static final Set<ObjectName> EXPECTED_LOCATOR_2_MXBEANS =
      execute(() -> expectedLocatorMXBeans(LOCATOR_2_NAME));
  private static final Set<ObjectName> EXPECTED_DISTRIBUTED_MXBEANS =
      execute(() -> expectedDistributedMXBeans(REGION_NAME));

  private static final AtomicReference<CountDownLatch> BEFORE =
      new AtomicReference<>(new CountDownLatch(0));
  private static final AtomicReference<CountDownLatch> AFTER =
      new AtomicReference<>(new CountDownLatch(0));

  private VM locator1VM;
  private VM locator2VM;
  private VM serverVM;

  private String locators;
  private int locator1Port;
  private int locator2Port;
  private int locator1JmxPort;
  private int locator2JmxPort;
  private Set<ObjectName> mxbeansOnServer;
  private Set<ObjectName> mxbeansOnLocator1;
  private Set<ObjectName> mxbeansOnLocator2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule(3);
  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();
  @Rule
  public DistributedReference<LocatorLauncher> locatorLauncher = new DistributedReference<>();
  @Rule
  public DistributedReference<ServerLauncher> serverLauncher = new DistributedReference<>();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setUp() throws Exception {
    locator1VM = getVM(0).initializeAsLocatorVM();
    locator2VM = getVM(1).initializeAsLocatorVM();
    serverVM = getVM(2);

    File locator1Dir = temporaryFolder.newFolder(LOCATOR_1_NAME);
    File locator2Dir = temporaryFolder.newFolder(LOCATOR_2_NAME);
    File serverDir = temporaryFolder.newFolder(SERVER_NAME);

    int[] port = getRandomAvailableTCPPorts(4);
    locator1Port = port[0];
    locator2Port = port[1];
    locator1JmxPort = port[2];
    locator2JmxPort = port[3];
    locators = "localhost[" + locator1Port + "],localhost[" + locator2Port + "]";

    locator1VM.invoke(() -> {
      locatorLauncher.set(
          startLocator(LOCATOR_1_NAME, locator1Dir, locator1Port, locator1JmxPort, locators));
    });
    locator2VM.invoke(() -> {
      locatorLauncher.set(
          startLocator(LOCATOR_2_NAME, locator2Dir, locator2Port, locator2JmxPort, locators));
    });

    serverVM.invoke(() -> serverLauncher.set(startServer(serverDir, locators)));

    gfsh.connectAndVerify(locator1JmxPort, PortType.jmxManager);

    String createRegionCommand = "create region --type=REPLICATE --name=" + SEPARATOR + REGION_NAME;
    gfsh.executeAndAssertThat(createRegionCommand).statusIsSuccess();

    addIgnoredException(AlertingIOException.class);
    addIgnoredException(CacheClosedException.class);
    addIgnoredException(CancelException.class);
    addIgnoredException(DistributedSystemDisconnectedException.class);
    addIgnoredException(ForcedDisconnectException.class);
    addIgnoredException(MemberDisconnectedException.class);
    addIgnoredException("Possible loss of quorum");

    mxbeansOnServer = serverVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on server")
            .containsAll(EXPECTED_SERVER_MXBEANS);
      });
      return getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL);
    });

    mxbeansOnLocator1 = locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
            .containsAll(EXPECTED_SERVER_MXBEANS)
            .containsAll(EXPECTED_LOCATOR_1_MXBEANS)
            .containsAll(EXPECTED_LOCATOR_2_MXBEANS)
            .containsAll(EXPECTED_DISTRIBUTED_MXBEANS);
      });
      return getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL);
    });

    mxbeansOnLocator2 = locator2VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator2")
            .containsAll(EXPECTED_SERVER_MXBEANS)
            .containsAll(EXPECTED_LOCATOR_2_MXBEANS)
            .containsAll(EXPECTED_LOCATOR_1_MXBEANS)
            .containsAll(EXPECTED_DISTRIBUTED_MXBEANS);
      });
      return getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL);
    });
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      BEFORE.get().countDown();
      AFTER.get().countDown();
    });
  }

  @Test
  public void serverHasMemberTypeMXBeans() {
    serverVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on server")
            .containsAll(EXPECTED_SERVER_MXBEANS);
      });
    });
  }

  @Test
  public void locatorsHaveMemberTypeMXBeansForServer() {
    for (VM locatorVM : toArray(locator1VM, locator2VM)) {
      locatorVM.invoke(() -> {
        await().untilAsserted(() -> {
          assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
              .as("GemFire mxbeans on server")
              .containsAll(EXPECTED_SERVER_MXBEANS);
        });
      });
    }
  }

  @Test
  public void locatorHasMemberTypeMXBeansForBothLocators() {
    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
            .containsAll(EXPECTED_LOCATOR_1_MXBEANS);
      });
    });

    locator2VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator2")
            .containsAll(EXPECTED_LOCATOR_2_MXBEANS);
      });
    });
  }

  @Test
  public void locatorsHaveDistributedTypeMXBeans() {
    for (VM locatorVM : toArray(locator1VM, locator2VM)) {
      locatorVM.invoke(() -> {
        await().untilAsserted(() -> {
          assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
              .as("GemFire mxbeans on locator" + getVMId())
              .containsAll(EXPECTED_DISTRIBUTED_MXBEANS);
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
      crashDistributedSystem(locatorLauncher.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
            .isEmpty();
      });
    });

    serverVM.invoke(() -> {
      assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
          .as("GemFire mxbeans on server")
          .containsExactlyInAnyOrderElementsOf(mxbeansOnServer);
    });

    locator1VM.invoke(() -> {
      InternalLocator locator = (InternalLocator) locatorLauncher.get().getLocator();

      await().untilAsserted(() -> {
        boolean isReconnected = locator.isReconnected();
        boolean isSharedConfigurationRunning = locator.isSharedConfigurationRunning();
        Set<ObjectName> mbeanNames =
            getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL);

        assertThat(isReconnected)
            .as("Locator is reconnected on locator1")
            .isTrue();
        assertThat(isSharedConfigurationRunning)
            .as("Locator shared configuration is running on locator1")
            .isTrue();
        assertThat(mbeanNames)
            .as("GemFire mxbeans on locator1")
            .isEqualTo(mxbeansOnLocator1);
      });
    });

    serverVM.invoke(() -> {
      assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
          .as("GemFire mxbeans on server")
          .containsExactlyInAnyOrderElementsOf(mxbeansOnServer);
    });
  }

  /**
   * Test that a locator's local MBeans are not affected by a server crashing
   */
  @Test
  public void serverMXBeansOnLocatorAreRestoredAfterCrashedServerReturns() {
    serverVM.invoke(() -> {
      crashDistributedSystem(serverLauncher.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on server")
            .isEmpty();
      });
    });

    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
            .doesNotContainAnyElementsOf(mxbeansOnServer);
      });
    });

    serverVM.invoke(() -> {
      InternalCache cache = (InternalCache) serverLauncher.get().getCache();
      InternalDistributedSystem system = cache.getInternalDistributedSystem();

      await().untilAsserted(() -> {
        assertThat(system.isReconnecting())
            .as("System is reconnecting on server")
            .isTrue();
      });

      system.waitUntilReconnected(getTimeout().toMillis(), MILLISECONDS);

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on server")
            .containsExactlyInAnyOrderElementsOf(mxbeansOnServer);
      });
    });

    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
            .containsAll(mxbeansOnServer)
            .containsExactlyInAnyOrderElementsOf(mxbeansOnLocator1);
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
            BEFORE.get().await(getTimeout().toMillis(), MILLISECONDS);
          } catch (InterruptedException e) {
            errorCollector.addError(e);
          }
        }
      });

      crashDistributedSystem(locatorLauncher.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
            .isEmpty();
      });
    });

    locator2VM.invoke(() -> {
      Collection<ObjectName> locator1MXBeans = new ArrayList<>(mxbeansOnLocator1);
      locator1MXBeans.removeAll(EXPECTED_SERVER_MXBEANS);
      locator1MXBeans.removeAll(EXPECTED_LOCATOR_2_MXBEANS);
      locator1MXBeans.removeAll(EXPECTED_DISTRIBUTED_MXBEANS);

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator2")
            .doesNotContainAnyElementsOf(locator1MXBeans);
      });
    });

    locator1VM.invoke(() -> {
      BEFORE.get().countDown();

      InternalLocator locator = (InternalLocator) locatorLauncher.get().getLocator();

      await().untilAsserted(() -> {
        assertThat(locator.isSharedConfigurationRunning())
            .as("Locator shared configuration is running on locator1")
            .isTrue();
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
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
            BEFORE.get().await(getTimeout().toMillis(), MILLISECONDS);
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

      crashDistributedSystem(serverLauncher.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on server")
            .isEmpty();
      });
    });

    for (VM locatorVM : toArray(locator1VM, locator2VM)) {
      locatorVM.invoke(() -> {
        await().untilAsserted(() -> {
          assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
              .as("GemFire mxbeans on locator" + locatorVM.getId())
              .isNotEmpty()
              .doesNotContainAnyElementsOf(mxbeansOnServer);
        });
      });
    }

    serverVM.invoke(() -> {
      BEFORE.get().countDown();
      AFTER.get().await(getTimeout().toMillis(), MILLISECONDS);

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on server")
            .isEqualTo(mxbeansOnServer);
      });
    });

    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator1")
            .containsAll(mxbeansOnLocator1);
      });
    });

    locator2VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator2")
            .containsAll(mxbeansOnLocator2);
      });
    });
  }

  private static LocatorLauncher startLocator(String name, File workingDirectory, int locatorPort,
      int jmxPort, String locators) {
    LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
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
        .build();

    locatorLauncher.start();

    InternalLocator locator = (InternalLocator) locatorLauncher.getLocator();

    await().untilAsserted(() -> {
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });

    return locatorLauncher;
  }

  private static ServerLauncher startServer(File workingDirectory, String locators) {
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setDisableDefaultServer(true)
        .setMemberName(SERVER_NAME)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, SERVER_NAME + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build();

    serverLauncher.start();

    return serverLauncher;
  }

  private static Set<ObjectName> expectedServerMXBeans(String memberName, String regionName)
      throws MalformedObjectNameException {
    return new HashSet<>(asList(
        getInstance("GemFire:type=Member,member=" + memberName),
        getInstance("GemFire:service=Region,name=" + SEPARATOR + regionName +
            ",type=Member,member=" + memberName)));
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
        getInstance("GemFire:service=Region,name=" + SEPARATOR + regionName + ",type=Distributed"),
        getInstance("GemFire:service=System,type=Distributed")));
  }

  private static <V> V execute(Callable<V> task) {
    try {
      return task.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
