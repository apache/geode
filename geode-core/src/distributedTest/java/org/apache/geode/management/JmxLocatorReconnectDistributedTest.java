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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;

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
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(JMXTest.class)
@SuppressWarnings({"serial", "CodeBlock2Expr", "SameParameterValue"})
public class JmxLocatorReconnectDistributedTest implements Serializable {

  private static final String LOCATOR_NAME = "locator";
  private static final String SERVER_NAME = "server";
  private static final String REGION_NAME = "region";
  private static final QueryExp QUERY_ALL = null;

  private static final ObjectName GEMFIRE_MXBEANS =
      execute(() -> getInstance("GemFire:*"));
  private static final Set<ObjectName> EXPECTED_SERVER_MXBEANS =
      execute(() -> expectedServerMXBeans(SERVER_NAME, REGION_NAME));
  private static final Set<ObjectName> EXPECTED_LOCATOR_MXBEANS =
      execute(() -> expectedLocatorMXBeans(LOCATOR_NAME));
  private static final Set<ObjectName> EXPECTED_DISTRIBUTED_MXBEANS =
      execute(() -> expectedDistributedMXBeans(REGION_NAME));

  private VM locatorVM;
  private VM serverVM;

  private String locators;
  private int locatorPort;
  private int locatorJmxPort;
  private Set<ObjectName> mxbeansOnServer;
  private Set<ObjectName> mxbeansOnLocator;

  @Rule
  public DistributedRule distributedRule = new DistributedRule(2);
  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();
  @Rule
  public DistributedReference<LocatorLauncher> locatorLauncher = new DistributedReference<>();
  @Rule
  public DistributedReference<ServerLauncher> serverLauncher = new DistributedReference<>();
  @Rule
  public DistributedRestoreSystemProperties restoreProps = new DistributedRestoreSystemProperties();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setUp() throws Exception {
    locatorVM = getVM(0);
    serverVM = getVM(1);

    File locatorDir = temporaryFolder.newFolder(LOCATOR_NAME);
    File serverDir = temporaryFolder.newFolder(SERVER_NAME);

    Invoke.invokeInEveryVM(() -> System.setProperty("jdk.serialFilter", "*"));

    for (VM vm : asList(locatorVM, serverVM)) {
      vm.invoke(() -> System.setProperty(GEMFIRE_PREFIX + "standard-output-always-on", "true"));
    }

    int[] port = getRandomAvailableTCPPorts(2);
    locatorPort = port[0];
    locatorJmxPort = port[1];
    locators = "localhost[" + locatorPort + "]";

    locatorVM.invoke(() -> {
      locatorLauncher.set(startLocator(locatorDir, locatorPort, locatorJmxPort, locators));
    });

    serverVM.invoke(() -> serverLauncher.set(startServer(serverDir, locators)));

    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);

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

    mxbeansOnLocator = locatorVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator")
            .containsAll(EXPECTED_SERVER_MXBEANS)
            .containsAll(EXPECTED_LOCATOR_MXBEANS)
            .containsAll(EXPECTED_DISTRIBUTED_MXBEANS);
      });
      return getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL);
    });
  }

  @Test
  public void serverMXBeanOnServerIsUnaffectedByLocatorCrash() {
    locatorVM.invoke(() -> {
      crashDistributedSystem(locatorLauncher.get().getCache().getDistributedSystem());

      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
            .as("GemFire mxbeans on locator")
            .isEmpty();
      });
    });

    serverVM.invoke(() -> {
      assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
          .as("GemFire mxbeans on server")
          .containsExactlyInAnyOrderElementsOf(mxbeansOnServer);
    });

    locatorVM.invoke(() -> {
      InternalLocator locator = (InternalLocator) locatorLauncher.get().getLocator();

      await().untilAsserted(() -> {
        boolean isReconnected = locator.isReconnected();
        boolean isSharedConfigurationRunning = locator.isSharedConfigurationRunning();
        Set<ObjectName> mbeanNames =
            getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL);

        assertThat(isReconnected)
            .as("Locator is reconnected on locator")
            .isTrue();
        assertThat(isSharedConfigurationRunning)
            .as("Locator shared configuration is running on locator")
            .isTrue();
        assertThat(mbeanNames)
            .as("GemFire mxbeans on locator")
            .isEqualTo(mxbeansOnLocator);
      });
    });

    serverVM.invoke(() -> {
      assertThat(getPlatformMBeanServer().queryNames(GEMFIRE_MXBEANS, QUERY_ALL))
          .as("GemFire mxbeans on server")
          .containsExactlyInAnyOrderElementsOf(mxbeansOnServer);
    });
  }

  private static LocatorLauncher startLocator(File workingDirectory, int locatorPort, int jmxPort,
      String locators) {
    LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
        .setMemberName(LOCATOR_NAME)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, LOCATOR_NAME + ".log").getAbsolutePath())
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
