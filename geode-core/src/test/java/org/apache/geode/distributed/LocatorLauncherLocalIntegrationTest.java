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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.AbstractLauncher.Status.NOT_RESPONDING;
import static org.apache.geode.distributed.AbstractLauncher.Status.ONLINE;
import static org.apache.geode.distributed.AbstractLauncher.Status.STOPPED;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for using {@link LocatorLauncher} as an in-process API within an existing JVM.
 *
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
public class LocatorLauncherLocalIntegrationTest extends LocatorLauncherIntegrationTestCase {

  @Before
  public void setUpLocatorLauncherLocalIntegrationTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.PROPERTY_TEST_PREFIX, getUniqueName() + "-");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isTrue();
  }

  @After
  public void tearDownLocatorLauncherLocalIntegrationTest() throws Exception {
    disconnectFromDS();
  }

  @Test
  public void usesLocatorPortAsDefaultPort() throws Exception {
    launcher = givenLocatorLauncher();

    assertThat(launcher.getPort()).isEqualTo(defaultLocatorPort);
  }

  @Test
  public void startReturnsOnline() throws Exception {
    launcher = givenLocatorLauncher();

    assertThat(launcher.start().getStatus()).isEqualTo(ONLINE);
  }

  @Test
  public void startWithPortUsesPort() throws Exception {
    LocatorLauncher launcher = startLocator(newBuilder().setPort(defaultLocatorPort));

    assertThat(launcher.getLocator().getPort()).isEqualTo(defaultLocatorPort);
  }

  @Test
  public void startWithPortZeroUsesAnEphemeralPort() throws Exception {
    LocatorLauncher launcher = startLocator(newBuilder().setPort(0));

    assertThat(launcher.getLocator().getPort()).isGreaterThan(0);
    assertThat(launcher.getLocator().isPeerLocator()).isTrue();
  }

  @Test
  public void startUsesBuilderValues() throws Exception {
    LocatorLauncher launcher = startLocator(newBuilder().setPort(nonDefaultLocatorPort));

    InternalLocator locator = launcher.getLocator();
    assertThat(locator.getPort()).isEqualTo(nonDefaultLocatorPort);

    DistributedSystem system = locator.getDistributedSystem();
    assertThat(system.getProperties().getProperty(DISABLE_AUTO_RECONNECT)).isEqualTo("true");
    assertThat(system.getProperties().getProperty(LOG_LEVEL)).isEqualTo("config");
    assertThat(system.getProperties().getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(system.getProperties().getProperty(NAME)).isEqualTo(getUniqueName());
  }

  @Test
  public void startCreatesPidFile() throws Exception {
    startLocator();

    assertThat(getPidFile()).exists();
  }

  @Test
  public void pidFileContainsServerPid() throws Exception {
    startLocator();

    assertThat(getLocatorPid()).isEqualTo(localPid);
  }

  @Test
  public void startDeletesStaleControlFiles() throws Exception {
    File stopRequestFile = givenControlFile(getProcessType().getStopRequestFileName());
    File statusRequestFile = givenControlFile(getProcessType().getStatusRequestFileName());
    File statusFile = givenControlFile(getProcessType().getStatusFileName());

    startLocator();

    assertDeletionOf(stopRequestFile);
    assertDeletionOf(statusRequestFile);
    assertDeletionOf(statusFile);
  }

  @Test
  public void startOverwritesStalePidFile() throws Exception {
    givenPidFile(fakePid);

    startLocator();

    assertThat(getLocatorPid()).isNotEqualTo(fakePid);
  }

  @Test
  public void startWithDefaultPortInUseFailsWithBindException() throws Exception {
    givenLocatorPortInUse(defaultLocatorPort);

    launcher = new Builder().build();

    assertThatThrownBy(() -> launcher.start()).isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(BindException.class);
  }

  @Test
  public void startWithLocatorPortInUseFailsWithBindException() throws Exception {
    givenServerPortInUse(nonDefaultLocatorPort);

    launcher = new Builder().setPort(nonDefaultLocatorPort).build();

    assertThatThrownBy(() -> this.launcher.start()).isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(BindException.class);
  }

  @Test
  public void statusWithPidReturnsOnlineWithDetails() throws Exception {
    givenRunningLocator();

    LocatorState locatorState = new Builder().setPid(localPid).build().status();

    assertThat(locatorState.getStatus()).isEqualTo(ONLINE);
    assertThat(locatorState.getClasspath()).isEqualTo(getClassPath());
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(locatorState.getLogFile()).isEqualTo(getLogFilePath());
    assertThat(locatorState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(locatorState.getPid().intValue()).isEqualTo(localPid);
    assertThat(locatorState.getUptime()).isGreaterThan(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(new File(".").getCanonicalPath());
  }

  @Test
  public void statusWithWorkingDirectoryReturnsOnlineWithDetails() throws Exception {
    givenRunningLocator();

    LocatorState locatorState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(locatorState.getStatus()).isEqualTo(ONLINE);
    assertThat(locatorState.getClasspath()).isEqualTo(getClassPath());
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(locatorState.getLogFile()).isEqualTo(getLogFilePath());
    assertThat(locatorState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(locatorState.getPid().intValue()).isEqualTo(readPidFile());
    assertThat(locatorState.getUptime()).isGreaterThan(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(new File(".").getCanonicalPath());
  }

  @Test
  public void statusWithEmptyPidFileThrowsIllegalArgumentException() throws Exception {
    givenEmptyPidFile();

    LocatorLauncher launcher = new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build();

    assertThatThrownBy(launcher::status).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid pid 'null' found in");
  }

  @Test
  public void statusWithEmptyWorkingDirectoryReturnsNotRespondingWithDetails() throws Exception {
    givenEmptyWorkingDirectory();

    LocatorState locatorState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(locatorState.getStatus()).isEqualTo(NOT_RESPONDING);
    assertThat(locatorState.getClasspath()).isNull();
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments())
        .isEqualTo(ManagementFactory.getRuntimeMXBean().getInputArguments());
    assertThat(locatorState.getLogFile()).isNull();
    assertThat(locatorState.getMemberName()).isNull();
    assertThat(locatorState.getPid()).isNull();
    assertThat(locatorState.getUptime().intValue()).isEqualTo(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  /**
   * This test takes > 1 minute to run in {@link LocatorLauncherLocalFileIntegrationTest}.
   */
  @Test
  public void statusWithStalePidFileReturnsNotResponding() throws Exception {
    givenPidFile(fakePid);

    LocatorState locatorState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(locatorState.getStatus()).isEqualTo(NOT_RESPONDING);
  }

  @Test
  public void stopWithPidReturnsStopped() throws Exception {
    givenRunningLocator();

    LocatorState locatorState = new Builder().setPid(localPid).build().stop();

    assertThat(locatorState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithPidDeletesPidFile() throws Exception {
    givenRunningLocator(newBuilder().setDeletePidFileOnStop(true));

    new Builder().setPid(localPid).build().stop();

    assertDeletionOf(getPidFile());
  }

  @Test
  public void stopWithWorkingDirectoryReturnsStopped() throws Exception {
    givenRunningLocator();

    LocatorState locatorState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertThat(locatorState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithWorkingDirectoryDeletesPidFile() throws Exception {
    givenRunningLocator(newBuilder().setDeletePidFileOnStop(true));

    new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertDeletionOf(getPidFile());
  }
}
