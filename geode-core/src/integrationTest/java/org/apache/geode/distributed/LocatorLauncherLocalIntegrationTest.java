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

import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static org.apache.geode.distributed.AbstractLauncher.Status.NOT_RESPONDING;
import static org.apache.geode.distributed.AbstractLauncher.Status.ONLINE;
import static org.apache.geode.distributed.AbstractLauncher.Status.STOPPED;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.net.BindException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.internal.process.ProcessType;

/**
 * Integration tests for using {@link LocatorLauncher} as an in-process API within an existing JVM.
 *
 * @since GemFire 8.0
 */
public class LocatorLauncherLocalIntegrationTest extends LocatorLauncherIntegrationTestCase {

  @Before
  public void setUpLocatorLauncherLocalIntegrationTest() {
    disconnectFromDS();
    System.setProperty(ProcessType.PROPERTY_TEST_PREFIX, getUniqueName() + "-");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isTrue();
  }

  @After
  public void tearDownLocatorLauncherLocalIntegrationTest() {
    disconnectFromDS();
  }

  @Test
  public void usesLocatorPortAsDefaultPort() {
    launcher = givenLocatorLauncher();

    assertThat(launcher.getPort()).isEqualTo(defaultLocatorPort);
  }

  @Test
  public void startReturnsOnline() {
    launcher = givenLocatorLauncher();

    assertThat(launcher.start().getStatus()).isEqualTo(ONLINE);
  }

  @Test
  public void startWithPortUsesPort() {
    LocatorLauncher launcher = startLocator(newBuilder()
        .setPort(defaultLocatorPort));

    assertThat(launcher.getInternalLocator().getPort()).isEqualTo(defaultLocatorPort);
  }

  @Test
  public void startWithPortZeroUsesAnEphemeralPort() {
    LocatorLauncher launcher = startLocator(newBuilder()
        .setPort(0));

    assertThat(launcher.getInternalLocator().getPort()).isGreaterThan(0);
    assertThat(launcher.getInternalLocator().isPeerLocator()).isTrue();
  }

  @Test
  public void startUsesBuilderValues() {
    LocatorLauncher launcher = startLocator(newBuilder()
        .setPort(nonDefaultLocatorPort));

    InternalLocator locator = launcher.getInternalLocator();
    assertThat(locator.getPort()).isEqualTo(nonDefaultLocatorPort);

    DistributedSystem system = locator.getDistributedSystem();
    assertThat(system.getProperties().getProperty(DISABLE_AUTO_RECONNECT)).isEqualTo("true");
    assertThat(system.getProperties().getProperty(LOG_LEVEL)).isEqualTo("config");
    assertThat(system.getProperties().getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(system.getProperties().getProperty(NAME)).isEqualTo(getUniqueName());
  }

  @Test
  public void startCreatesPidFile() {
    startLocator();

    assertThat(getPidFile()).exists();
  }

  @Test
  public void pidFileContainsServerPid() {
    startLocator();

    assertThat(getLocatorPid()).isEqualTo(localPid);
  }

  @Test
  public void startDeletesStaleControlFiles() {
    File stopRequestFile = givenControlFile(getProcessType().getStopRequestFileName());
    File statusRequestFile = givenControlFile(getProcessType().getStatusRequestFileName());
    File statusFile = givenControlFile(getProcessType().getStatusFileName());

    startLocator();

    assertDeletionOf(stopRequestFile);
    assertDeletionOf(statusRequestFile);
    assertDeletionOf(statusFile);
  }

  @Test
  public void startOverwritesStalePidFile() {
    givenPidFile(fakePid);

    startLocator();

    assertThat(getLocatorPid()).isNotEqualTo(fakePid);
  }

  @Test
  public void startWithDefaultPortInUseFailsWithBindException() {
    givenLocatorPortInUse(defaultLocatorPort);
    launcher = newBuilder()
        .build();

    Throwable thrown = catchThrowable(() -> launcher.start());

    assertThat(thrown)
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(BindException.class);
  }

  @Test
  public void startWithLocatorPortInUseFailsWithBindException() {
    givenServerPortInUse(nonDefaultLocatorPort);
    launcher = newBuilder()
        .setPort(nonDefaultLocatorPort)
        .build();

    Throwable thrown = catchThrowable(() -> launcher.start());

    assertThat(thrown)
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(BindException.class);
  }

  @Test
  public void statusWithPidReturnsOnlineWithDetails() throws Exception {
    givenRunningLocator();

    LocatorState locatorState = new Builder()
        .setPid(localPid)
        .build()
        .status();

    assertThat(locatorState.getStatus()).isEqualTo(ONLINE);
    assertThat(locatorState.getClasspath()).isEqualTo(getClassPath());
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(getLocalHost().getCanonicalHostName());
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

    LocatorState locatorState = new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .status();

    assertThat(locatorState.getStatus()).isEqualTo(ONLINE);
    assertThat(locatorState.getClasspath()).isEqualTo(getClassPath());
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(locatorState.getLogFile()).isEqualTo(getLogFilePath());
    assertThat(locatorState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(locatorState.getPid().intValue()).isEqualTo(readPidFile());
    assertThat(locatorState.getUptime()).isGreaterThan(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(new File(".").getCanonicalPath());
  }

  @Test
  public void statusWithEmptyPidFileThrowsIllegalArgumentException() {
    givenEmptyPidFile();
    LocatorLauncher launcher = new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build();

    Throwable thrown = catchThrowable(launcher::status);

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid pid 'null' found in");
  }

  @Test
  public void statusWithEmptyWorkingDirectoryReturnsNotRespondingWithDetails() throws Exception {
    givenEmptyWorkingDirectory();

    LocatorState locatorState = new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .status();

    assertThat(locatorState.getStatus()).isEqualTo(NOT_RESPONDING);
    assertThat(locatorState.getClasspath()).isNull();
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments()).isEqualTo(getRuntimeMXBean().getInputArguments());
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
  public void statusWithStalePidFileReturnsNotResponding() {
    givenPidFile(fakePid);

    LocatorState locatorState = new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .status();

    assertThat(locatorState.getStatus()).isEqualTo(NOT_RESPONDING);
  }

  @Test
  public void stopWithPidReturnsStopped() {
    givenRunningLocator();

    LocatorState locatorState = new Builder()
        .setPid(localPid)
        .build()
        .stop();

    assertThat(locatorState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithPidDeletesPidFile() {
    givenRunningLocator(newBuilder().setDeletePidFileOnStop(true));

    new Builder()
        .setPid(localPid)
        .build()
        .stop();

    assertDeletionOf(getPidFile());
  }

  @Test
  public void stopWithWorkingDirectoryReturnsStopped() {
    givenRunningLocator();

    LocatorState locatorState = new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .stop();

    assertThat(locatorState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithWorkingDirectoryDeletesPidFile() {
    givenRunningLocator(newBuilder().setDeletePidFileOnStop(true));

    new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .stop();

    assertDeletionOf(getPidFile());
  }

  @Test
  public void getCacheReturnsTheCache() {
    givenRunningLocator();

    assertThat(launcher.getCache()).isInstanceOf(Cache.class);
  }

  @Test
  public void getLocatorReturnsTheLocator() {
    givenRunningLocator();

    assertThat(launcher.getLocator()).isInstanceOf(Locator.class);
  }
}
