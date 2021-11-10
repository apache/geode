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
package org.apache.geode.logging.internal;

import static java.lang.System.lineSeparator;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper.getDistribution;
import static org.apache.geode.internal.logging.Banner.BannerHeader.displayValues;
import static org.apache.geode.logging.internal.Configuration.STARTUP_CONFIGURATION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.gms.GMSMembership;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for logging during reconnect.
 */
@Category(LoggingTest.class)
public class LoggingWithReconnectDistributedTest implements Serializable {

  private static final long TIMEOUT = getTimeout().toMillis();

  private static LocatorLauncher locatorLauncher;
  private static ServerLauncher serverLauncher;

  private static InternalDistributedSystem system;

  private VM locatorVM;
  private VM server1VM;
  private VM server2VM;

  private String locatorName;
  private String server1Name;
  private String server2Name;

  private File locatorDir;
  private File server1Dir;
  private File server2Dir;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    locatorName = "locator-" + testName.getMethodName();
    server1Name = "server-" + testName.getMethodName() + "-1";
    server2Name = "server-" + testName.getMethodName() + "-2";

    locatorVM = getVM(0);
    server1VM = getVM(1);
    server2VM = getController();

    locatorDir = temporaryFolder.newFolder(locatorName);
    server1Dir = temporaryFolder.newFolder(server1Name);
    server2Dir = temporaryFolder.newFolder(server2Name);

    int locatorPort = locatorVM.invoke(() -> createLocator());

    server1VM.invoke(() -> createServer(server1Name, server1Dir, locatorPort));
    server2VM.invoke(() -> createServer(server2Name, server2Dir, locatorPort));

    addIgnoredException(ForcedDisconnectException.class);
    addIgnoredException(MemberDisconnectedException.class);
    addIgnoredException("Possible loss of quorum");
  }

  @After
  public void tearDown() {
    locatorVM.invoke(() -> {
      locatorLauncher.stop();
      locatorLauncher = null;
      system = null;
    });

    for (VM vm : toArray(server1VM, server2VM)) {
      vm.invoke(() -> {
        serverLauncher.stop();
        serverLauncher = null;
        system = null;
      });
    }
  }

  @Test
  public void logFileContainsBannerOnlyOnce() {
    locatorVM.invoke(() -> {
      assertThat(system.getDistributionManager().getDistributionManagerIds()).hasSize(3);
    });

    server2VM.invoke(() -> {
      Distribution membershipManager = getDistribution(system);
      ((GMSMembership) membershipManager.getMembership())
          .forceDisconnect("Forcing disconnect in " + testName.getMethodName());

      await().until(() -> system.isReconnecting());
      system.waitUntilReconnected(TIMEOUT, MILLISECONDS);
      assertThat(system.getReconnectedSystem()).isNotSameAs(system);
    });

    locatorVM.invoke(() -> {
      assertThat(system.getDistributionManager().getDistributionManagerIds()).hasSize(3);
    });

    server2VM.invoke(() -> {
      File[] files = server2Dir.listFiles((dir, name) -> name.endsWith(".log"));
      assertThat(files).as(expectedOneLogFile(files)).hasSize(1);

      File logFile = files[0];
      assertThat(logFile).exists();

      // Banner must be logged only once
      LogFileAssert.assertThat(logFile).containsOnlyOnce(displayValues());

      // Startup Config must be logged only once
      String[] startupConfiguration = StringUtils
          .split(STARTUP_CONFIGURATION + lineSeparator() + system.getConfig().toLoggerString(),
              lineSeparator());

      LogFileAssert.assertThat(logFile).containsOnlyOnce(startupConfiguration);
    });
  }

  private String expectedOneLogFile(File[] files) {
    return "Expecting directory:" + lineSeparator() + " " + server2Dir.getAbsolutePath()
        + lineSeparator() + "to contain only one log file:" + lineSeparator() + " " + server2Name
        + ".log" + lineSeparator() + "but found multiple log files:" + lineSeparator() + " "
        + Arrays.asList(files);
  }

  private int createLocator() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();
    builder.setMemberName(locatorName);
    builder.setWorkingDirectory(locatorDir.getAbsolutePath());
    builder.setPort(0);
    builder.set(DISABLE_AUTO_RECONNECT, "false");
    builder.set(ENABLE_CLUSTER_CONFIGURATION, "false");
    builder.set(MAX_WAIT_TIME_RECONNECT, "1000");
    builder.set(MEMBER_TIMEOUT, "2000");

    locatorLauncher = builder.build();
    locatorLauncher.start();

    system = (InternalDistributedSystem) locatorLauncher.getCache().getDistributedSystem();

    return locatorLauncher.getPort();
  }

  private void createServer(String serverName, File serverDir, int locatorPort) {
    ServerLauncher.Builder builder = new ServerLauncher.Builder();
    builder.setMemberName(serverName);
    builder.setWorkingDirectory(serverDir.getAbsolutePath());
    builder.setServerPort(0);
    builder.set(LOCATORS, "localHost[" + locatorPort + "]");
    builder.set(DISABLE_AUTO_RECONNECT, "false");
    builder.set(ENABLE_CLUSTER_CONFIGURATION, "false");
    builder.set(MAX_WAIT_TIME_RECONNECT, "1000");
    builder.set(MEMBER_TIMEOUT, "2000");

    serverLauncher = builder.build();
    serverLauncher.start();

    system = (InternalDistributedSystem) serverLauncher.getCache().getDistributedSystem();
  }
}
