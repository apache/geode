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
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.net.BindException;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessControllerFactory;

/**
 * Integration tests for using {@code LocatorLauncher} as an application main in a forked JVM.
 *
 * @since GemFire 8.0
 */
public class LocatorLauncherRemoteIntegrationTest extends LocatorLauncherRemoteIntegrationTestCase {

  @Before
  public void setUpLocatorLauncherRemoteIntegrationTest() {
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isTrue();
  }

  @Test
  public void startCreatesPidFile() {
    startLocator(withPort(0));

    assertThat(getPidFile()).exists();
  }

  @Test
  public void pidFileContainsServerPid() {
    startLocator(withPort(0));

    assertThatPidIsAlive(getLocatorPid());
  }

  @Test
  public void startCreatesLogFile() {
    startLocator(withPort(0));

    assertThat(getLogFile()).exists();
  }

  @Test
  public void startDeletesStaleControlFiles() {
    File stopRequestFile = givenControlFile(getStopRequestFileName());
    File statusRequestFile = givenControlFile(getStatusRequestFileName());
    File statusFile = givenControlFile(getStatusFileName());

    startLocator(withPort(0));

    assertDeletionOf(stopRequestFile);
    assertDeletionOf(statusRequestFile);
    assertDeletionOf(statusFile);
  }

  /**
   * This test takes > 1 minute to run in {@link LocatorLauncherRemoteFileIntegrationTest}.
   */
  @Test
  public void startOverwritesStalePidFile() {
    givenPidFile(fakePid);

    startLocator(withPort(0));

    assertThat(getLocatorPid()).isNotEqualTo(fakePid);
  }

  /**
   * This test takes > 1 minute to run in {@link LocatorLauncherRemoteFileIntegrationTest}.
   */
  @Test
  public void startWithForceOverwritesExistingPidFile() {
    givenPidFile(localPid);

    startLocator(withForce().withPort(0));

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
    givenRunningLocator(withPort(0));

    LocatorState locatorState = new Builder()
        .setPid(getLocatorPid())
        .build()
        .status();

    assertThat(locatorState.getClasspath()).isEqualTo(getClassPath());
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(locatorState.getLogFile()).isEqualTo(getLogFile().getCanonicalPath());
    assertThat(locatorState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(locatorState.getPid().intValue()).isEqualTo(getLocatorPid());
    assertThat(locatorState.getStatus()).isEqualTo(ONLINE);
    assertThat(locatorState.getUptime()).isGreaterThan(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualToIgnoringCase(getWorkingDirectoryPath());
  }

  @Test
  public void statusWithWorkingDirectoryReturnsOnlineWithDetails() throws Exception {
    givenRunningLocator(withPort(0));

    LocatorState locatorState = new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .status();

    assertThat(locatorState.getClasspath()).isEqualTo(getClassPath());
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(locatorState.getLogFile()).isEqualTo(getLogFile().getCanonicalPath());
    assertThat(locatorState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(locatorState.getPid().intValue()).isEqualTo(readPidFile());
    assertThat(locatorState.getStatus()).isEqualTo(ONLINE);
    assertThat(locatorState.getUptime()).isGreaterThan(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualToIgnoringCase(getWorkingDirectoryPath());
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

    assertThat(locatorState.getClasspath()).isNull();
    assertThat(locatorState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(locatorState.getHost()).isEqualTo(getLocalHost().getCanonicalHostName());
    assertThat(locatorState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(locatorState.getLogFile()).isNull();
    assertThat(locatorState.getMemberName()).isNull();
    assertThat(locatorState.getPid()).isNull();
    assertThat(locatorState.getStatus()).isEqualTo(NOT_RESPONDING);
    assertThat(locatorState.getUptime().intValue()).isEqualTo(0);
    assertThat(locatorState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  /**
   * This test takes > 1 minute to run in {@link LocatorLauncherRemoteFileIntegrationTest}.
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
    givenRunningLocator(withPort(0));

    LocatorState serverState = new Builder()
        .setPid(getLocatorPid())
        .build()
        .stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithPidStopsLocatorProcess() {
    givenRunningLocator(withPort(0));

    new Builder().setPid(getLocatorPid())
        .build()
        .stop();

    assertStopOf(getLocatorProcess());
  }

  @Test
  public void stopWithPidDeletesPidFile() {
    givenRunningLocator(withPort(0));

    new Builder()
        .setPid(getLocatorPid())
        .build()
        .stop();

    assertDeletionOf(getPidFile());
  }

  @Test
  public void stopWithWorkingDirectoryReturnsStopped() {
    givenRunningLocator(withPort(0));

    LocatorState serverState = new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithWorkingDirectoryStopsLocatorProcess() {
    givenRunningLocator(withPort(0));

    new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .stop();

    assertStopOf(getLocatorProcess());
  }

  @Test
  public void stopWithWorkingDirectoryDeletesPidFile() {
    givenRunningLocator(withPort(0));

    new Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .stop();

    assertDeletionOf(getPidFile());
  }
}
