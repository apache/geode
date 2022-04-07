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
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATOR_WAIT_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.IntSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({MembershipTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LocatorIntegrationTest {

  private Locator locator;
  private File tmpFile;
  private int port;

  @Parameters
  public static Collection<Object> data() {
    return Arrays.asList(new Object[] {
        (IntSupplier) () -> 0,
        (IntSupplier) AvailablePortHelper::getRandomAvailableTCPPort});
  }

  @Parameter
  public IntSupplier portSupplier;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    tmpFile = File.createTempFile("locator", ".log");
    port = portSupplier.getAsInt();
    deleteLocatorViewFile(port);
  }

  @After
  public void tearDown() {
    if (locator != null) {
      locator.stop();
    }
    assertThat(Locator.hasLocator()).isFalse();
  }

  /**
   * Fix: locator creates "locator0view.dat" file when started with port 0
   */
  @Test
  public void testThatLocatorDoesNotCreateFileWithZeroPort() throws Exception {
    deleteLocatorViewFile(0);

    Properties configProperties = new Properties();
    configProperties.setProperty(MCAST_PORT, "0");
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(LOCATOR_WAIT_TIME, "1"); // seconds
    configProperties.setProperty(LOG_FILE, "");

    locator = Locator.startLocatorAndDS(port, null, configProperties);

    File viewFile = new File("locator0view.dat");

    assertThat(viewFile).doesNotExist();
  }

  /**
   * Fix: if jmx-manager-start is true in a locator then gfsh connect will fail
   */
  @Test
  public void testGfshConnectShouldSucceedIfJmxManagerStartIsTrueInLocator() throws Exception {
    int jmxPort = getRandomAvailablePort(SOCKET);

    Properties configProperties = new Properties();
    configProperties.setProperty(MCAST_PORT, "0");
    configProperties.setProperty(JMX_MANAGER_PORT, "" + jmxPort);
    configProperties.setProperty(JMX_MANAGER_START, "true");
    configProperties.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(LOG_FILE, "");

    // not needed
    System.setProperty(GEMFIRE_PREFIX + "disableManagement", "false");

    locator = Locator.startLocatorAndDS(port, null, configProperties);
    List<JmxManagerProfile> alreadyManaging =
        GemFireCacheImpl.getInstance().getJmxManagerAdvisor().adviseAlreadyManaging();

    assertThat(alreadyManaging).hasSize(1);
    assertThat(alreadyManaging.get(0).getDistributedMember())
        .isEqualTo(GemFireCacheImpl.getInstance().getMyId());
  }

  @Test
  public void testHandlersAreWaitedOn() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty(MCAST_PORT, "0");
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(LOCATOR_WAIT_TIME, "1"); // seconds
    configProperties.setProperty(LOG_FILE, "");

    locator = Locator.startLocatorAndDS(port, null, configProperties);

    InternalLocator internalLocator = (InternalLocator) locator;

    // the locator should always install a SharedConfigurationStatusRequest handler
    assertThat(internalLocator.hasHandlerForClass(SharedConfigurationStatusRequest.class)).isTrue();
  }

  @Test
  public void testBasicInfo() throws Exception {
    locator = Locator.startLocator(port, tmpFile);
    int boundPort = port == 0 ? locator.getPort() : port;
    TcpClient client = new TcpClient(SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
    String[] info = client.getInfo(new HostAndPort("localhost", boundPort));

    assertThat(info).isNotNull();
    assertThat(info.length).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void testNoThreadLeftBehind() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty(MCAST_PORT, "0");
    configProperties.setProperty(JMX_MANAGER_START, "false");
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    int threadCount = Thread.activeCount();

    Throwable thrown = catchThrowable(
        () -> locator = Locator.startLocatorAndDS(-2, new File(""), configProperties));

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);

    for (int i = 0; i < 10; i++) {
      if (threadCount < Thread.activeCount()) {
        Thread.sleep(1000);
      }
    }

    OSProcess.printStacks(0);

    assertThat(threadCount)
        .as("Expected " + threadCount + " threads or fewer but found " + Thread.activeCount()
            + ". Check log file for a thread dump.")
        .isGreaterThanOrEqualTo(Thread.activeCount());
  }

  /**
   * Make sure two ServerLocation objects on different hosts but with the same port are not equal
   *
   * <p>
   * Fix: LoadBalancing directs all traffic to a single cache server if all servers are started on
   * the same port
   */
  @Test
  public void testServerLocationOnDifferentHostsShouldNotTestEqual() {
    ServerLocation serverLocation1 = new ServerLocation("host1", 777);
    ServerLocation serverLocation2 = new ServerLocation("host2", 777);

    assertThat(serverLocation1).isNotEqualTo(serverLocation2);
  }

  private void deleteLocatorViewFile(int portNumber) {
    File locatorFile = new File("locator" + portNumber + "view.dat");
    if (locatorFile.exists()) {
      assertThat(locatorFile.delete()).isTrue();
    }
  }
}
