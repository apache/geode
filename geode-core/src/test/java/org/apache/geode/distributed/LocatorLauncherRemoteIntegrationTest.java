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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.BindException;
import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for using {@code LocatorLauncher} as an application main in a forked JVM.
 *
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
public class LocatorLauncherRemoteIntegrationTest extends LocatorLauncherRemoteIntegrationTestCase {

  @Before
  public void setUpLocatorLauncherRemoteIntegrationTest() throws Exception {
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isTrue();
  }

  @Test
  public void startCreatesPidFile() throws Exception {
    startLocator();

    assertThat(getPidFile()).exists();
  }

  @Test
  public void pidFileContainsServerPid() throws Exception {
    startLocator();

    assertThatPidIsAlive(getLocatorPid());
  }

  @Test
  public void startCreatesLogFile() throws Exception {
    startLocator();

    assertThat(getLogFile()).exists();
  }

  @Test
  @Category(FlakyTest.class) // GEODE-3506
  public void startDeletesStaleControlFiles() throws Exception {
    File stopRequestFile = givenControlFile(getStopRequestFileName());
    File statusRequestFile = givenControlFile(getStatusRequestFileName());
    File statusFile = givenControlFile(getStatusFileName());

    startLocator();

    assertDeletionOf(stopRequestFile);
    assertDeletionOf(statusRequestFile);
    assertDeletionOf(statusFile);
  }

  /**
   * This test takes > 1 minute to run in {@link LocatorLauncherRemoteFileIntegrationTest}.
   */
  @Test
  public void startOverwritesStalePidFile() throws Exception {
    givenPidFile(fakePid);

    startLocator();

    assertThat(getLocatorPid()).isNotEqualTo(fakePid);
  }

  /**
   * This test takes > 1 minute to run in {@link LocatorLauncherRemoteFileIntegrationTest}.
   */
  @Test
  public void startWithForceOverwritesExistingPidFile() throws Exception {
    givenPidFile(localPid);

    startLocator(withForce());

    assertThatPidIsAlive(getLocatorPid());
    assertThat(getLocatorPid()).isNotEqualTo(localPid);
  }

  @Test
  public void startWithLocatorPortInUseFailsWithBindException() throws Exception {
    givenLocatorPortInUse(nonDefaultLocatorPort);

    startLocatorShouldFail(withPort(nonDefaultLocatorPort));

    assertThatProcessIsNotAlive(getLocatorProcess());
    assertThatLocatorThrew(BindException.class);
  }

  @Test
  public void startWithDefaultPortInUseFailsWithBindException() throws Exception {
    givenLocatorPortInUse(defaultLocatorPort);

    startLocatorShouldFail();

    assertThatProcessIsNotAlive(getLocatorProcess());
    assertThatLocatorThrew(BindException.class);
  }

  @Test
  public void statusWithPidReturnsOnlineWithDetails() throws Exception {
    givenRunningLocator();

    LocatorState locatorState = new Builder().setPid(getLocatorPid()).build().status();

    assertThat(locatorState.getStatus()).isEqualTo(ONLINE);
    assertThat(locatorState.getClasspath()).isEqualTo(getClassPath());
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(locatorState.getLogFile()).isEqualTo(getLogFile().getCanonicalPath());
    assertThat(locatorState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(locatorState.getPid().intValue()).isEqualTo(getLocatorPid());
    assertThat(locatorState.getUptime()).isGreaterThan(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
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
    assertThat(locatorState.getLogFile()).isEqualTo(getLogFile().getCanonicalPath());
    assertThat(locatorState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(locatorState.getPid().intValue()).isEqualTo(readPidFile());
    assertThat(locatorState.getUptime()).isGreaterThan(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void statusWithEmptyPidFileThrowsIllegalArgumentException() throws Exception {
    givenEmptyPidFile();

    LocatorLauncher launcher = new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build();

    assertThatThrownBy(() -> launcher.status()).isInstanceOf(IllegalArgumentException.class)
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
    assertThat(locatorState.getLogFile()).isNull();
    assertThat(locatorState.getMemberName()).isNull();
    assertThat(locatorState.getPid()).isNull();
    assertThat(locatorState.getUptime().intValue()).isEqualTo(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  /**
   * This test takes > 1 minute to run in {@link LocatorLauncherRemoteFileIntegrationTest}.
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

    LocatorState serverState = new Builder().setPid(getLocatorPid()).build().stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithPidStopsLocatorProcess() throws Exception {
    givenRunningLocator();

    new Builder().setPid(getLocatorPid()).build().stop();

    assertStopOf(getLocatorProcess());
  }

  @Test
  public void stopWithPidDeletesPidFile() throws Exception {
    givenRunningLocator();

    new Builder().setPid(getLocatorPid()).build().stop();

    assertDeletionOf(getPidFile());
  }

  @Test
  public void stopWithWorkingDirectoryReturnsStopped() throws Exception {
    givenRunningLocator();

    LocatorState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithWorkingDirectoryStopsLocatorProcess() throws Exception {
    givenRunningLocator();

    new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertStopOf(getLocatorProcess());
  }

  @Test
  public void stopWithWorkingDirectoryDeletesPidFile() throws Exception {
    givenRunningLocator();

    new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertDeletionOf(getPidFile());
  }
}
