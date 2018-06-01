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
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.ServerLauncher.ServerState;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.internal.process.ProcessType;

/**
 * Integration tests for using {@link ServerLauncher} as an in-process API within an existing JVM.
 *
 * @since GemFire 8.0
 */
public class ServerLauncherLocalIntegrationTest extends ServerLauncherLocalIntegrationTestCase {

  @Before
  public void setUpServerLauncherLocalIntegrationTest() {
    disconnectFromDS();
    System.setProperty(ProcessType.PROPERTY_TEST_PREFIX, getUniqueName() + "-");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isTrue();
  }

  @After
  public void tearDownServerLauncherLocalIntegrationTest() {
    disconnectFromDS();
  }

  @Test
  public void usesLocatorPortAsDefaultPort() {
    launcher = givenServerLauncher();

    assertThat(launcher.getServerPort()).isEqualTo(defaultServerPort);
  }

  @Test
  public void startReturnsOnline() {
    launcher = givenServerLauncher();

    assertThat(launcher.start().getStatus()).isEqualTo(ONLINE);
  }

  @Test
  public void startWithPortUsesPort() {
    ServerLauncher launcher =
        startServer(newBuilder().setDisableDefaultServer(false).setServerPort(defaultServerPort));

    assertThat(launcher.getCache().getCacheServers().get(0).getPort()).isEqualTo(defaultServerPort);
  }

  @Test
  public void startWithPortZeroUsesAnEphemeralPort() {
    ServerLauncher launcher =
        startServer(newBuilder().setDisableDefaultServer(false).setServerPort(0));

    assertThat(launcher.getCache().getCacheServers().get(0).getPort()).isGreaterThan(0);
  }

  @Test
  public void startUsesBuilderValues() {
    ServerLauncher launcher = startServer(newBuilder().set(DISABLE_AUTO_RECONNECT, "true"));

    Cache cache = launcher.getCache();
    assertThat(cache).isNotNull();

    DistributedSystem system = cache.getDistributedSystem();
    assertThat(system).isNotNull();
    assertThat(system.getProperties().getProperty(DISABLE_AUTO_RECONNECT)).isEqualTo("true");
    assertThat(system.getProperties().getProperty(LOG_LEVEL)).isEqualTo("config");
    assertThat(system.getProperties().getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(system.getProperties().getProperty(NAME)).isEqualTo(getUniqueName());
  }

  @Test
  public void startCreatesPidFile() {
    startServer();

    assertThat(getPidFile()).exists();
  }

  @Test
  public void pidFileContainsServerPid() {
    startServer();

    assertThat(getServerPid()).isEqualTo(localPid);
  }

  @Test
  public void startDeletesStaleControlFiles() {
    File stopRequestFile = givenControlFile(getProcessType().getStopRequestFileName());
    File statusRequestFile = givenControlFile(getProcessType().getStatusRequestFileName());
    File statusFile = givenControlFile(getProcessType().getStatusFileName());

    startServer();

    assertDeletionOf(stopRequestFile);
    assertDeletionOf(statusRequestFile);
    assertDeletionOf(statusFile);
  }

  @Test
  public void startOverwritesStalePidFile() {
    givenPidFile(fakePid);

    startServer();

    assertThat(getServerPid()).isNotEqualTo(fakePid);
  }

  @Test
  public void startWithDisableDefaultServerDoesNotUseDefaultPort() {
    givenServerPortIsFree(defaultServerPort);

    startServer(withDisableDefaultServer());

    assertThatServerPortIsFree(defaultServerPort);
  }

  @Test
  public void startWithDisableDefaultServerSucceedsWhenDefaultPortInUse() {
    givenServerPortInUse(defaultServerPort);

    startServer(withDisableDefaultServer());

    assertThatServerPortIsInUseBySocket(defaultServerPort);
  }

  @Test
  public void startWithServerPortOverridesPortInCacheXml() {
    int[] freePorts = getRandomAvailableTCPPorts(2);
    int cacheXmlPort = freePorts[0];
    int startPort = freePorts[1];
    givenCacheXmlFileWithServerPort(cacheXmlPort);

    launcher = startServer(new Builder().setServerPort(startPort));

    // server should use --server-port instead of port in cache.xml
    assertThatServerPortIsInUse(startPort);
    assertThatServerPortIsFree(cacheXmlPort);
    assertThat(Integer.valueOf(launcher.status().getPort())).isEqualTo(startPort);
  }

  @Test
  public void startWithServerPortOverridesDefaultWithCacheXml() {
    givenCacheXmlFile();

    launcher = awaitStart(new Builder().setMemberName(getUniqueName()).setRedirectOutput(true)
        .setServerPort(defaultServerPort).setWorkingDirectory(getWorkingDirectoryPath())
        .set(LOG_LEVEL, "config").set(MCAST_PORT, "0"));

    // verify server used --server-port instead of default
    assertThatServerPortIsInUse(defaultServerPort);
    assertThat(Integer.valueOf(launcher.status().getPort())).isEqualTo(defaultServerPort);
  }

  @Test
  public void startWithDefaultPortInUseFailsWithBindException() {
    givenServerPortInUse(defaultServerPort);

    launcher = new Builder().build();

    assertThatThrownBy(() -> launcher.start()).isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(BindException.class);
  }

  @Test
  public void startWithServerPortInUseFailsWithBindException() {
    givenServerPortInUse(nonDefaultServerPort);

    launcher = new Builder().setServerPort(nonDefaultServerPort).build();

    assertThatThrownBy(() -> launcher.start()).isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(BindException.class);
  }

  @Test
  public void startWithParametersOverridesCacheXmlConfiguration() throws IOException {
    int[] freePorts = getRandomAvailableTCPPorts(2);
    int xmlPort = freePorts[0];
    int serverPort = freePorts[1];
    Integer maxThreads = 100;
    Integer maxConnections = 1200;
    Integer maxMessageCount = 500000;
    Integer socketBufferSize = 342768;
    Integer messageTimeToLive = 120;
    String hostnameForClients = "hostName4Clients";
    String serverBindAddress = "127.0.0.1";

    ServerLauncher.Builder launcherBuilder = new Builder().setServerBindAddress(serverBindAddress)
        .setServerPort(serverPort).setMaxThreads(maxThreads).setMaxConnections(maxConnections)
        .setMaxMessageCount(maxMessageCount).setMessageTimeToLive(messageTimeToLive)
        .setSocketBufferSize(socketBufferSize).setHostNameForClients(hostnameForClients);
    givenCacheXmlFileWithServerProperties(xmlPort, CacheServer.DEFAULT_BIND_ADDRESS,
        CacheServer.DEFAULT_HOSTNAME_FOR_CLIENTS, CacheServer.DEFAULT_MAX_CONNECTIONS,
        CacheServer.DEFAULT_MAX_THREADS, CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
        CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE);

    launcher = startServer(launcherBuilder);

    assertThatServerPortIsInUse(serverPort);
    assertThatServerPortIsFree(xmlPort);
    assertThat(Integer.valueOf(launcher.status().getPort())).isEqualTo(serverPort);
    List<CacheServer> servers = launcher.getCache().getCacheServers();
    assertThat(servers.size()).isEqualTo(1);
    CacheServer server = servers.get(0);
    assertThat(server.getMaxThreads()).isEqualTo(maxThreads);
    assertThat(server.getMaxConnections()).isEqualTo(maxConnections);
    assertThat(server.getMaximumMessageCount()).isEqualTo(maxMessageCount);
    assertThat(server.getSocketBufferSize()).isEqualTo(socketBufferSize);
    assertThat(server.getMessageTimeToLive()).isEqualTo(messageTimeToLive);
    assertThat(server.getHostnameForClients()).isEqualTo(hostnameForClients);
    assertThat(server.getBindAddress()).isEqualTo(serverBindAddress);
  }

  @Test
  public void statusForDisableDefaultServerHasEmptyPort() {
    givenServerPortIsFree(defaultServerPort);

    ServerState serverState = startServer(newBuilder().setDisableDefaultServer(true)).status();

    assertThat(serverState.getPort()).isEqualTo("");
  }

  @Test
  public void statusWithPidReturnsOnlineWithDetails() throws UnknownHostException {
    givenRunningServer();

    ServerState serverState = new Builder().setPid(localPid).build().status();

    assertThat(serverState.getStatus()).isEqualTo(ONLINE);
    assertThat(serverState.getClasspath()).isEqualTo(getClassPath());
    assertThat(serverState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(serverState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(serverState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(serverState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(serverState.getLogFile()).isEqualTo(getLogFilePath());
    assertThat(serverState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(serverState.getPid().intValue()).isEqualTo(localPid);
    assertThat(serverState.getUptime()).isGreaterThan(0);
    assertThat(serverState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void statusWithWorkingDirectoryReturnsOnlineWithDetails() throws UnknownHostException {
    givenRunningServer();

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(serverState.getStatus()).isEqualTo(ONLINE);
    assertThat(serverState.getClasspath()).isEqualTo(getClassPath());
    assertThat(serverState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(serverState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(serverState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(serverState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(serverState.getLogFile()).isEqualTo(getLogFilePath());
    assertThat(serverState.getMemberName()).isEqualTo(getUniqueName());
    assertThat(serverState.getPid().intValue()).isEqualTo(readPidFile());
    assertThat(serverState.getUptime()).isGreaterThan(0);
    assertThat(serverState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void statusWithEmptyPidFileThrowsIllegalArgumentException() {
    givenEmptyPidFile();

    ServerLauncher launcher = new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build();

    assertThatThrownBy(() -> launcher.status()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid pid 'null' found in");
  }

  @Test
  public void statusWithEmptyWorkingDirectoryReturnsNotRespondingWithDetails()
      throws UnknownHostException {
    givenEmptyWorkingDirectory();

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(serverState.getStatus()).isEqualTo(NOT_RESPONDING);
    assertThat(serverState.getClasspath()).isNull();
    assertThat(serverState.getGemFireVersion()).isEqualTo(GemFireVersion.getGemFireVersion());
    assertThat(serverState.getHost()).isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
    assertThat(serverState.getJavaVersion()).isEqualTo(System.getProperty("java.version"));
    assertThat(serverState.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(serverState.getLogFile()).isNull();
    assertThat(serverState.getMemberName()).isNull();
    assertThat(serverState.getPid()).isNull();
    assertThat(serverState.getUptime().intValue()).isEqualTo(0);
    assertThat(serverState.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  /**
   * This test takes > 1 minute to run in {@link ServerLauncherLocalFileIntegrationTest}.
   */
  @Test
  public void statusWithStalePidFileReturnsNotResponding() {
    givenPidFile(fakePid);

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().status();

    assertThat(serverState.getStatus()).isEqualTo(NOT_RESPONDING);
  }

  @Test
  public void stopWithPidReturnsStopped() {
    givenRunningServer();

    ServerState serverState = new Builder().setPid(localPid).build().stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithPidDeletesPidFile() {
    givenRunningServer(newBuilder().setDeletePidFileOnStop(true));

    new Builder().setPid(localPid).build().stop();

    assertDeletionOf(getPidFile());
  }

  @Test
  public void stopWithWorkingDirectoryReturnsStopped() {
    givenRunningServer();

    ServerState serverState =
        new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertThat(serverState.getStatus()).isEqualTo(STOPPED);
  }

  @Test
  public void stopWithWorkingDirectoryDeletesPidFile() {
    givenRunningServer(newBuilder().setDeletePidFileOnStop(true));

    new Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertDeletionOf(getPidFile());
  }
}
