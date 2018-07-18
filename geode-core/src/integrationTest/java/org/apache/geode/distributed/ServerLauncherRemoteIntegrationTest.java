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
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.BindException;
import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.ServerLauncher.ServerState;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessControllerFactory;

/**
 * Integration tests for using {@code ServerLauncher} as an application main in a forked JVM.
 *
 * @since GemFire 8.0
 */
public class ServerLauncherRemoteIntegrationTest extends ServerLauncherRemoteIntegrationTestCase {

  @Before
  public void setUpServerLauncherRemoteIntegrationTest() throws Exception {
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isTrue();
  }

  @Test
  public void startCreatesPidFile() throws Exception {
    startServer();

    assertThat(getPidFile()).exists();
  }

  @Test
  public void pidFileContainsServerPid() throws Exception {
    startServer();

    assertThatPidIsAlive(getServerPid());
  }

  @Test
  public void startCreatesLogFile() throws Exception {
    startServer();

    assertThat(getLogFile()).exists();
  }

  @Test
  public void startDeletesStaleControlFiles() throws Exception {
    File stopRequestFile = givenControlFile(getStopRequestFileName());
    File statusRequestFile = givenControlFile(getStatusRequestFileName());
    File statusFile = givenControlFile(getStatusFileName());

    startServer();

    assertDeletionOf(stopRequestFile);
    assertDeletionOf(statusRequestFile);
    assertDeletionOf(statusFile);
  }

  /**
   * This test takes > 1 minute to run in {@link ServerLauncherRemoteFileIntegrationTest}.
   */
  @Test
  public void startOverwritesStalePidFile() throws Exception {
    givenPidFile(fakePid);

    startServer();

    assertThat(getServerPid()).isNotEqualTo(fakePid);
  }

  @Test
  public void startWithDisableDefaultServerDoesNotUseDefaultPort() throws Exception {
    givenServerPortIsFree(defaultServerPort);

    startServer(withDisableDefaultServer());

    assertThatServerPortIsFree(defaultServerPort);
  }

  @Test
  public void startWithDisableDefaultServerSucceedsWhenDefaultPortInUse() throws Exception {
    givenServerPortInUse(defaultServerPort);

    startServer(withDisableDefaultServer());

    assertThatServerPortIsInUseBySocket(defaultServerPort);
  }

  /**
   * This test takes > 1 minute to run in {@link ServerLauncherRemoteFileIntegrationTest}.
   */
  @Test
  public void startWithForceOverwritesExistingPidFile() throws Exception {
    givenPidFile(localPid);

    startServer(withForce());

    assertThatPidIsAlive(getServerPid());
    assertThat(getServerPid()).isNotEqualTo(localPid);
  }

  @Test
  public void startWithServerPortInUseFailsWithBindException() throws Exception {
    givenServerPortInUse(nonDefaultServerPort);

    startServerShouldFail(withServerPort(nonDefaultServerPort));

    assertThatServerThrew(BindException.class);
  }

  @Test
  public void startWithServerPortOverridesPortInCacheXml() throws Exception {
    givenCacheXmlFileWithServerPort(unusedServerPort);

    ServerLauncher launcher = startServer(
        addJvmArgument("-D" + GEMFIRE_PREFIX + CACHE_XML_FILE + "=" + getCacheXmlFilePath())
            .withServerPort(nonDefaultServerPort));

    // server should use --server-port instead of port in cache.xml
    assertThatServerPortIsInUse(nonDefaultServerPort);
    assertThatServerPortIsFree(unusedServerPort);
    assertThat(Integer.valueOf(launcher.status().getPort())).isEqualTo(nonDefaultServerPort);
  }

  @Test
  public void startWithServerPortOverridesDefaultWithCacheXml() throws Exception {
    givenCacheXmlFile();

    ServerLauncher launcher = startServer(
        addJvmArgument("-D" + GEMFIRE_PREFIX + CACHE_XML_FILE + "=" + getCacheXmlFilePath())
            .withServerPort(nonDefaultServerPort));

    // verify server used --server-port instead of default
    assertThatServerPortIsInUse(nonDefaultServerPort);
    assertThatServerPortIsFree(defaultServerPort);
    assertThat(Integer.valueOf(launcher.status().getPort())).isEqualTo(nonDefaultServerPort);
  }

  @Test
  public void startWithDefaultPortInUseFailsWithBindException() throws Exception {
    givenServerPortInUse(defaultServerPort);

    startServerShouldFail();

    assertThatServerThrew(BindException.class);
  }

  @Test
  public void statusForDisableDefaultServerHasEmptyPort() throws Exception {
    givenRunningServer(withDisableDefaultServer());

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(serverState.getPort()).isEmpty();
  }

  @Test
  public void statusWithPidReturnsOnlineWithDetails() throws Exception {
    givenRunningServer();

    ServerState serverState = new Builder().setPid(getServerPid()).build().status();

    assertThat(serverState.getStatus()).isEqualTo(ONLINE);
    assertThat(serverState.getPid().intValue()).isEqualTo(getServerPid());
    assertThat(serverState.getUptime()).isGreaterThan(0);
    assertThat(serverState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
    assertThat(serverState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(serverState.getClasspath()).isEqualTo(getClassPath());
    assertThat(serverState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(serverState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(serverState.getLogFile()).isEqualTo(getLogFile().getCanonicalPath());
    assertThat(serverState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(serverState.getMemberName()).isEqualTo(getUniqueName());
  }

  @Test
  public void statusWithWorkingDirectoryReturnsOnlineWithDetails() throws Exception {
    givenRunningServer();

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(serverState.getStatus()).isEqualTo(ONLINE);
    assertThat(serverState.getPid().intValue()).isEqualTo(readPidFile());
    assertThat(serverState.getUptime()).isGreaterThan(0);
    assertThat(serverState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
    assertThat(serverState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(serverState.getClasspath()).isEqualTo(getClassPath());
    assertThat(serverState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(serverState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(serverState.getLogFile()).isEqualTo(getLogFile().getCanonicalPath());
    assertThat(serverState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(serverState.getMemberName()).isEqualTo(getUniqueName());
  }

  @Test
  public void statusWithEmptyPidFileThrowsIllegalArgumentException() throws Exception {
    givenEmptyPidFile();

    ServerLauncher launcher = new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build();

    assertThatThrownBy(() -> launcher.status()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid pid 'null' found in");
  }

  @Test
  public void statusWithEmptyWorkingDirectoryReturnsNotRespondingWithDetails() throws Exception {
    givenEmptyWorkingDirectory();

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(serverState.getStatus()).isEqualTo(NOT_RESPONDING);
    assertThat(serverState.getPid()).isNull();
    assertThat(serverState.getUptime().intValue()).isEqualTo(0);
    assertThat(serverState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
    assertThat(serverState.getClasspath()).isNull();
    assertThat(serverState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(serverState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(serverState.getLogFile()).isNull();
    assertThat(serverState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(serverState.getMemberName()).isNull();
  }

  @Test
  public void statusWithStalePidFileReturnsNotResponding() throws Exception {
    givenPidFile(fakePid);

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(serverState.getStatus()).isEqualTo(NOT_RESPONDING);
  }

  @Test
  public void stopWithPidReturnsStopped() throws Exception {
    givenRunningServer();

    ServerState serverState = new Builder().setPid(getServerPid()).build().stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithPidStopsServerProcess() throws Exception {
    givenRunningServer();

    new Builder().setPid(getServerPid()).build().stop();

    assertStopOf(getServerProcess());
  }

  @Test
  public void stopWithPidDeletesPidFile() throws Exception {
    givenRunningServer();

    new Builder().setPid(getServerPid()).build().stop();

    assertDeletionOf(getPidFile());
  }

  @Test
  public void stopWithWorkingDirectoryReturnsStopped() throws Exception {
    givenRunningServer();

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithWorkingDirectoryStopsServerProcess() throws Exception {
    givenRunningServer();

    new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertStopOf(getServerProcess());
  }

  @Test
  public void stopWithWorkingDirectoryDeletesPidFile() throws Exception {
    givenRunningServer();

    new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertDeletionOf(getPidFile());
  }
}
