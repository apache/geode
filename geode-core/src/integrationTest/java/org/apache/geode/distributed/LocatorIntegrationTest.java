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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.LOCATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class LocatorIntegrationTest {

  private Locator locator;
  private File logFile;
  private int port;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    logFile = temporaryFolder.newFile("locator.log");
  }

  @After
  public void tearDown() {
    if (locator != null) {
      locator.stop();
    }
    deleteLocatorViewFile(port);
  }

  /**
   * Fix: locator creates "locator0view.dat" file when started with port 0
   */
  @Test
  public void doesNotCreateZeroPortViewFileForEphemeralPort() throws Exception {
    deleteLocatorViewFile(0);

    Properties configProperties = new Properties();
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(HTTP_SERVICE_PORT, "0");
    configProperties.setProperty(JMX_MANAGER, "false");
    configProperties.setProperty(LOG_FILE, "");
    configProperties.setProperty(MCAST_PORT, "0");

    locator = Locator.startLocatorAndDS(port, null, configProperties);

    File viewFile = new File("locator0view.dat");

    assertThat(viewFile).doesNotExist();
  }

  @Test
  public void locatorStartsOnSpecifiedPort() throws IOException {
    Properties configProperties = new Properties();
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(HTTP_SERVICE_PORT, "0");
    configProperties.setProperty(JMX_MANAGER, "false");
    configProperties.setProperty(LOG_FILE, "");
    configProperties.setProperty(MCAST_PORT, "0");

    port = getRandomAvailableTCPPort();
    locator = Locator.startLocatorAndDS(port, null, configProperties);

    port = locator.getPort();
    assertThat(port).isEqualTo(port);
  }

  @Test
  public void locatorStartsOnEphemeralPort() throws IOException {
    Properties configProperties = new Properties();
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(HTTP_SERVICE_PORT, "0");
    configProperties.setProperty(JMX_MANAGER, "false");
    configProperties.setProperty(LOG_FILE, "");
    configProperties.setProperty(MCAST_PORT, "0");

    locator = Locator.startLocatorAndDS(0, null, configProperties);

    port = locator.getPort();
    assertThat(port).isNotZero();
  }

  /**
   * Fix: if jmx-manager-start is true in a locator then gfsh connect will fail
   */
  @Test
  public void gfshConnectsIfJmxManagerStartIsTrue() throws Exception {
    int jmxPort = getRandomAvailableTCPPort();

    Properties configProperties = new Properties();
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(HTTP_SERVICE_PORT, "0");
    configProperties.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    configProperties.setProperty(JMX_MANAGER_START, "true");
    configProperties.setProperty(LOG_FILE, "");
    configProperties.setProperty(MCAST_PORT, "0");

    locator = Locator.startLocatorAndDS(port, null, configProperties);
    InternalLocator internalLocator = (InternalLocator) locator;
    InternalCache cache = internalLocator.getCache();
    List<JmxManagerProfile> alreadyManaging = cache.getJmxManagerAdvisor().adviseAlreadyManaging();

    assertThat(alreadyManaging).hasSize(1);
    assertThat(alreadyManaging.get(0).getDistributedMember()).isEqualTo(cache.getMyId());
  }

  @Test
  public void hasHandlerForSharedConfigurationStatusRequest() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(HTTP_SERVICE_PORT, "0");
    configProperties.setProperty(JMX_MANAGER, "false");
    configProperties.setProperty(LOG_FILE, "");
    configProperties.setProperty(MCAST_PORT, "0");

    locator = Locator.startLocatorAndDS(port, null, configProperties);

    InternalLocator internalLocator = (InternalLocator) locator;

    // the locator should always install a SharedConfigurationStatusRequest handler
    boolean hasHandler = internalLocator.hasHandlerForClass(SharedConfigurationStatusRequest.class);
    assertThat(hasHandler).isTrue();
  }

  @Test
  public void infoRequestIncludesActualPortWhenSpecifiedPortIsZero() throws Exception {
    locator = Locator.startLocator(0, logFile);
    port = locator.getPort();
    TcpClient client = new TcpClient(
        SocketCreatorFactory.getSocketCreatorForComponent(LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);

    String[] info = client.getInfo(new HostAndPort("localhost", port));

    assertThat(info).isNotNull();
    assertThat(info.length).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void infoRequestIncludesActualPortWhenSpecifiedIsNonZero() throws Exception {
    locator = Locator.startLocator(getRandomAvailableTCPPort(), logFile);
    port = locator.getPort();
    TcpClient client = new TcpClient(
        SocketCreatorFactory.getSocketCreatorForComponent(LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);

    String[] info = client.getInfo(new HostAndPort("localhost", port));

    assertThat(info).isNotNull();
    assertThat(info.length).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void threadsAreCleanedUpWhenStartFails() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty(LOG_FILE, "");
    configProperties.setProperty(MCAST_PORT, "0");

    int threadCount = Thread.activeCount();

    Throwable thrown = catchThrowable(
        () -> locator = Locator.startLocatorAndDS(-2, null, configProperties));

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);

    GeodeAwaitility.await().until(() -> threadCount < Thread.activeCount());

    // TODO: since AssertJ supports withThreadDumpOnError, should we delete OSProcess.printStacks?
    OSProcess.printStacks(0);

    assertThat(threadCount)
        .as("Expected " + threadCount + " threads or fewer but found " + Thread.activeCount()
            + ". Check log file for a thread dump.")
        .withThreadDumpOnError()
        .isGreaterThanOrEqualTo(Thread.activeCount());
  }

  /**
   * Validates that Locator.startLocatorAndDS does not start a JMX Manager by default. Only the
   * LocatorLauncher starts one by default.
   */
  @Test
  public void doesNotStartJmxManagerByDefault() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(HTTP_SERVICE_PORT, "0");
    configProperties.setProperty(LOG_FILE, "");
    configProperties.setProperty(MCAST_PORT, "0");

    locator = Locator.startLocatorAndDS(port, null, configProperties);

    InternalLocator internalLocator = (InternalLocator) locator;
    InternalCache cache = internalLocator.getCache();
    SystemManagementService managementService =
        (SystemManagementService) ManagementService.getManagementService(cache);

    boolean isManager = managementService.isManager();
    assertThat(isManager).isFalse();
  }

  private void deleteLocatorViewFile(int portNumber) {
    File locatorFile = new File("locator" + portNumber + "view.dat");
    if (locatorFile.exists()) {
      assertThat(locatorFile.delete()).isTrue();
    }
  }
}
