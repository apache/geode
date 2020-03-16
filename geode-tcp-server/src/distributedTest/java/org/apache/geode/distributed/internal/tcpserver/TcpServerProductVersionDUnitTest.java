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
package org.apache.geode.distributed.internal.tcpserver;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

/**
 * In version 1.12 TcpServer changed: the three pairs of message types used to derive from
 * DataSerializable, but as of 1.12 they derive from BasicSerializable.
 *
 * This test verifies that the current version (1.12 or later) is compatible with the latest
 * version before 1.12
 */
@Category({MembershipTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class TcpServerProductVersionDUnitTest implements Serializable {

  private static final TestVersion FIRST_NEW_VERSION = TestVersion.valueOf("1.12.0");

  @Rule
  public DistributedRule distributedRule =
      DistributedRule.builder().withVMCount(0).build();

  @BeforeClass
  public static void beforeClass() {
    SocketCreatorFactory.close();
  }

  @AfterClass
  public static void afterClass() {
    SocketCreatorFactory.close();
  }


  private static final TestVersion oldProductVersion = getOldProductVersion();
  private static final TestVersion currentProductVersion = TestVersion.CURRENT_VERSION;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<VersionConfiguration> data() {
    return Arrays.asList(VersionConfiguration.values());
  }

  /*
   * We want the newest _available_ version older than FIRST_NEW_VERSION
   */
  private static TestVersion getOldProductVersion() {

    final Map<Boolean, List<TestVersion>> groups =
        VersionManager.getInstance().getVersionsWithoutCurrent().stream()
            .map(TestVersion::valueOf)
            .collect(Collectors.partitioningBy(v -> v.lessThan(FIRST_NEW_VERSION)));

    final List<TestVersion> olderVersions = groups.get(true);

    if (olderVersions.size() < 1) {
      throw new AssertionError("Time to decommission TcpServerProductVersionDUnitTest "
          + "because there are no supported versions older than " + FIRST_NEW_VERSION);
    }

    return olderVersions.get(olderVersions.size() - 1);
  }

  @SuppressWarnings("unused")
  private enum VersionConfiguration {
    OLD_CURRENT(oldProductVersion, currentProductVersion),
    CURRENT_OLD(currentProductVersion, oldProductVersion);

    final TestVersion clientProductVersion;
    final TestVersion locatorProductVersion;

    VersionConfiguration(final TestVersion clientProductVersion,
        final TestVersion locatorProductVersion) {
      this.clientProductVersion = clientProductVersion;
      this.locatorProductVersion = locatorProductVersion;
    }

  }

  private final VersionConfiguration versions;

  public TcpServerProductVersionDUnitTest(final VersionConfiguration versions) {
    this.versions = versions;
  }

  @Test
  public void testAllMessageTypes() {
    int clientVMNumber = versions.clientProductVersion.isSameAs(Version.CURRENT)
        ? DUnitLauncher.DEBUGGING_VM_NUM : 0;
    int locatorVMNumber = versions.locatorProductVersion.isSameAs(Version.CURRENT)
        ? DUnitLauncher.DEBUGGING_VM_NUM : 0;
    VM clientVM = Host.getHost(0).getVM(versions.clientProductVersion.toString(), clientVMNumber);
    VM locatorVM =
        Host.getHost(0).getVM(versions.locatorProductVersion.toString(), locatorVMNumber);
    int locatorPort = createLocator(locatorVM, true);

    clientVM.invoke("issue version request",
        createRequestResponseFunction(locatorPort, VersionRequest.class.getName(),
            VersionResponse.class.getName()));
    testDeprecatedMessageTypes(clientVM, locatorPort);
    clientVM.invoke("issue shutdown request",
        createRequestResponseFunction(locatorPort, ShutdownRequest.class.getName(),
            ShutdownResponse.class.getName()));
    locatorVM.invoke("wait for locator to stop", () -> {
      Locator locator = Locator.getLocator();
      if (locator != null) {
        ((InternalLocator) locator).stop(false, false, false);
        GeodeAwaitility.await().until(((InternalLocator) locator)::isStopped);
      }
    });
  }

  @SuppressWarnings("deprecation")
  private void testDeprecatedMessageTypes(VM clientVM, int locatorPort) {
    clientVM.invoke("issue info request",
        createRequestResponseFunction(locatorPort, InfoRequest.class.getName(),
            InfoResponse.class.getName()));
  }

  private SerializableRunnableIF createRequestResponseFunction(
      final int locatorPort,
      final String requestClassName,
      final String responseClassName) {

    return () -> {

      final Class<?> requestClass = Class.forName(requestClassName);
      final Object requestMessage = requestClass.newInstance();

      final TcpClient tcpClient;
      if (versions.clientProductVersion.greaterThanOrEqualTo(FIRST_NEW_VERSION)) {
        tcpClient = getTcpClient();
      } else {
        tcpClient = getLegacyTcpClient();
      }

      @SuppressWarnings("deprecation")
      final InetAddress localHost = SocketCreator.getLocalHost();
      Object response;
      try {
        Method requestToServer =
            TcpClient.class.getMethod("requestToServer", InetAddress.class, int.class, Object.class,
                int.class);
        response = requestToServer.invoke(tcpClient, localHost, locatorPort,
            requestMessage, 1000);
      } catch (NoSuchMethodException e) {
        response = tcpClient
            .requestToServer(
                new HostAndPort(localHost.getHostAddress(), locatorPort),
                requestMessage, 1000);
      }

      final Class<?> responseClass = Class.forName(responseClassName);

      assertThat(response).isInstanceOf(responseClass);
    };

  }

  /*
   * The TcpClient class changed in version FIRST_NEW_VERSION. That version (and later)
   * no longer has the old constructor TcpClient(final Properties), so we have to access
   * that constructor via reflection.
   */
  private TcpClient getLegacyTcpClient()
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      InstantiationException {

    final Constructor<TcpClient> constructor = TcpClient.class.getConstructor(Properties.class);
    return constructor.newInstance(getDistributedSystemProperties());
  }

  private TcpClient getTcpClient() {

    SocketCreatorFactory
        .setDistributionConfig(new DistributionConfigImpl(getDistributedSystemProperties()));

    return new TcpClient(
        SocketCreatorFactory
            .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
  }

  private int createLocator(VM memberVM, boolean usingOldVersion) {
    return memberVM.invoke("create locator", () -> {
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      try {
        int port = 0;
        // for stress-tests make sure that an older-version locator doesn't try
        // to read state persisted by another run's newer-version locator
        if (usingOldVersion) {
          port = AvailablePortHelper.getRandomAvailableTCPPort();
          DistributedTestUtils.deleteLocatorStateFile(port);
        }
        return Locator.startLocatorAndDS(port, new File(""), getDistributedSystemProperties())
            .getPort();
      } finally {
        System.clearProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
      }
    });
  }

  public Properties getDistributedSystemProperties() {
    Properties properties = new Properties();
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    @SuppressWarnings("deprecation")
    final int currentVMNum = VM.getCurrentVMNum();
    properties.setProperty(NAME, "vm" + currentVMNum);
    return properties;
  }

}
