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

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.cache.AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.ServerLauncher.ServerState;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Abstract base class for integration tests of {@link ServerLauncher}.
 *
 * @since GemFire 8.0
 */
public abstract class ServerLauncherIntegrationTestCase extends LauncherIntegrationTestCase {

  protected volatile int defaultServerPort;
  protected volatile int nonDefaultServerPort;
  protected volatile int unusedServerPort;
  protected volatile ServerLauncher launcher;
  protected volatile String workingDirectory;
  protected volatile File cacheXmlFile;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Before
  public void setUpAbstractServerLauncherIntegrationTestCase() throws Exception {
    System.setProperty(GEMFIRE_PREFIX + MCAST_PORT, Integer.toString(0));

    workingDirectory = temporaryFolder.getRoot().getCanonicalPath();

    int[] ports = getRandomAvailableTCPPorts(3);
    defaultServerPort = ports[0];
    nonDefaultServerPort = ports[1];
    unusedServerPort = ports[2];
    System.setProperty(TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(defaultServerPort));
  }

  @After
  public void tearDownAbstractServerLauncherIntegrationTestCase() {
    if (launcher != null) {
      launcher.stop();
    }
  }

  @Override
  protected ProcessType getProcessType() {
    return ProcessType.SERVER;
  }

  protected ServerLauncher awaitStart(final ServerLauncher launcher) {
    GeodeAwaitility.await().untilAsserted(() -> assertThat(isLauncherOnline()).isTrue());
    return launcher;
  }

  protected ServerLauncher awaitStart(final Builder builder) {
    launcher = builder.build();
    assertThat(launcher.start().getStatus()).isEqualTo(Status.ONLINE);
    return awaitStart(launcher);
  }

  /**
   * Returns a new Builder with helpful defaults for safe testing. If you need a Builder in a test
   * without any of these defaults then simply use {@code new Builder()} instead.
   */
  protected Builder newBuilder() {
    return new Builder().setMemberName(getUniqueName()).setRedirectOutput(true)
        .setWorkingDirectory(getWorkingDirectoryPath()).set(LOG_LEVEL, "config")
        .set(MCAST_PORT, "0");
  }

  protected ServerLauncher givenServerLauncher() {
    return givenServerLauncher(newBuilder());
  }

  protected ServerLauncher givenServerLauncher(final Builder builder) {
    return builder.build();
  }

  protected ServerLauncher givenRunningServer() {
    return givenRunningServer(newBuilder());
  }

  protected ServerLauncher givenRunningServer(final Builder builder) {
    return awaitStart(builder);
  }

  protected ServerLauncher startServer() {
    return awaitStart(newBuilder());
  }

  protected ServerLauncher startServer(final Builder builder) {
    return awaitStart(builder);
  }

  protected File givenCacheXmlFileWithServerProperties(final int serverPort,
      final String bindAddress, final String hostnameForClients, final int maxConnections,
      final int maxThreads, final int maximumMessageCount, final int messageTimeToLive,
      final int socketBufferSize) {
    try {
      cacheXmlFile = writeCacheXml(serverPort, bindAddress, hostnameForClients, maxConnections,
          maxThreads, maximumMessageCount, messageTimeToLive, socketBufferSize);
      System.setProperty(CACHE_XML_FILE, cacheXmlFile.getCanonicalPath());
      return cacheXmlFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected File givenCacheXmlFileWithServerPort(final int cacheXmlPort) {
    try {
      cacheXmlFile = writeCacheXml(cacheXmlPort);
      System.setProperty(CACHE_XML_FILE, cacheXmlFile.getCanonicalPath());
      return cacheXmlFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected File givenCacheXmlFile() {
    try {
      cacheXmlFile = writeCacheXml();
      System.setProperty(CACHE_XML_FILE, cacheXmlFile.getCanonicalPath());
      return cacheXmlFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected File getCacheXmlFile() {
    return cacheXmlFile;
  }

  protected String getCacheXmlFilePath() {
    try {
      return cacheXmlFile.getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private File writeCacheXml(final int serverPort) throws IOException {
    CacheCreation creation = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer().setPort(serverPort);

    File cacheXmlFile = new File(getWorkingDirectory(), getUniqueName() + ".xml");
    PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();

    return cacheXmlFile;
  }

  private File writeCacheXml(final int serverPort, final String bindAddress,
      final String hostnameForClients, final int maxConnections, final int maxThreads,
      final int maximumMessageCount, final int messageTimeToLive, final int socketBufferSize)
      throws IOException {
    CacheCreation creation = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    CacheServer server = creation.addCacheServer();
    server.setPort(serverPort);
    server.setBindAddress(bindAddress);
    server.setHostnameForClients(hostnameForClients);
    server.setMaxConnections(maxConnections);
    server.setMaxThreads(maxThreads);
    server.setMaximumMessageCount(maximumMessageCount);
    server.setMessageTimeToLive(messageTimeToLive);
    server.setSocketBufferSize(socketBufferSize);

    File cacheXmlFile = new File(getWorkingDirectory(), getUniqueName() + ".xml");
    PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();

    return cacheXmlFile;
  }

  private File writeCacheXml() throws IOException {
    CacheCreation creation = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer();

    File cacheXmlFile = new File(getWorkingDirectory(), getUniqueName() + ".xml");
    PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();

    return cacheXmlFile;
  }

  private boolean isLauncherOnline() {
    ServerState serverState = launcher.status();
    assertNotNull(serverState);
    return Status.ONLINE.equals(serverState.getStatus());
  }
}
