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
import static java.util.concurrent.TimeUnit.MINUTES;
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
import static org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper.crashDistributedSystem;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.alerting.internal.spi.AlertingIOException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class JMXMBeanLocatorRestartTest implements Serializable {

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

  private VM locatorVM;
  private VM serverVM;

  private String locatorName;
  private String serverName;
  private String locators;
  private int locatorPort;
  private int locatorJmxPort;
  private String regionName;
  private Set<ObjectName> mxbeansOnServer;
  private Set<ObjectName> mxbeansOnLocator;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();
  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();
  @Rule
  public DistributedRestoreSystemProperties restoreProps = new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    locatorVM = getVM(1);
    serverVM = getVM(0);

    for (VM vm : asList(locatorVM, serverVM)) {
      vm.invoke(() -> System.setProperty(GEMFIRE_PREFIX + "standard-output-always-on", "true"));
    }

    locatorName = "locator1";
    serverName = "server1";
    File locator1Dir = temporaryFolder.newFolder(locatorName);
    File serverDir = temporaryFolder.newFolder(serverName);

    int[] port = getRandomAvailableTCPPorts(4);
    locatorPort = port[0];
    locatorJmxPort = port[2];
    locators = "localhost[" + locatorPort + "]";

    locatorVM.invoke(() -> {
      startLocator(locatorName, locator1Dir, locatorPort, locatorJmxPort, locators);
    });

    serverVM.invoke(() -> startServer(serverName, serverDir, locators));

    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);

    regionName = "region1";
    String createRegionCommand = "create region --type=REPLICATE --name=" + SEPARATOR + regionName;
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
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .containsAll(expectedServerMXBeans(serverName, regionName));
      });
      return getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null);
    });

    mxbeansOnLocator = locatorVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator1")
            .containsAll(expectedServerMXBeans(serverName, regionName))
            .containsAll(expectedLocatorMXBeans(locatorName))
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
        getInstance(
            "GemFire:service=Region,name=" + SEPARATOR + regionName + ",type=Member,member=" +
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
        getInstance("GemFire:service=Region,name=" + SEPARATOR + regionName + ",type=Distributed"),
        getInstance("GemFire:service=System,type=Distributed")));
  }

  private static ConditionFactory await() {
    return GeodeAwaitility.await().atMost(1, MINUTES);
  }

  @Test
  public void locatorHasMemberTypeMXBeanForServer() {
    locatorVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on server1")
            .containsAll(expectedServerMXBeans(serverName, regionName));
      });
    });
  }

  @Test
  public void serverMXBeanOnServerIsUnaffectedByLocatorCrash() {
    locatorVM.invoke(() -> {
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

    locatorVM.invoke(() -> {
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
            .isEqualTo(mxbeansOnLocator);
      });
    });

    serverVM.invoke(() -> {
      assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
          .as("GemFire mbeans on server1")
          .containsExactlyElementsOf(mxbeansOnServer);
    });
  }
}
