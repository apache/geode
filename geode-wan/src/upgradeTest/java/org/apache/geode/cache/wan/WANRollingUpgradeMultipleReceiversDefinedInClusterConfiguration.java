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

package org.apache.geode.cache.wan;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.w3c.dom.Document;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

public class WANRollingUpgradeMultipleReceiversDefinedInClusterConfiguration
    extends WANRollingUpgradeDUnitTest {

  @Parameterized.Parameter(1)
  public String xmlFile;

  @Parameterized.Parameter(2)
  public int expectedReceiverElements;

  @Parameterized.Parameter(3)
  public int expectedReceivers;

  @Parameterized.Parameters(name = "from_v{0}; xmlFile={1}; expectedReceiverCount={2}")
  public static Collection data() {
    // Get initial versions to test against
    List<String> versions = getVersionsToTest();

    // Build up a list of version->attributes->expectedReceivers
    List<Object[]> result = new ArrayList<>();
    versions.forEach(version -> {
      // Add a case for hostname-for-senders
      result.add(addReceiversWithHostNameForSenders(version));

      // Add a case for bind-address
      result.add(addReceiversWithBindAddresses(version));

      // Add a case for multiple receivers with default attributes
      result.add(addMultipleReceiversWithDefaultAttributes(version));

      // Add a case for single receiver with default bind-address
      result.add(addSingleReceiverWithDefaultBindAddress(version));

      // Add a case for single receiver with default attributes
      result.add(addSingleReceiverWithDefaultAttributes(version));
    });

    System.out.println("running against these versions and attributes: "
        + result.stream().map(Arrays::toString).collect(Collectors.joining(", ")));
    return result;
  }

  private static List<String> getVersionsToTest() {
    // There is no need to test old versions beyond 130. Individual member configuration is not
    // saved in cluster configuration and multiple receivers are not supported starting in 140.
    // Note: This comparison works because '130' < '140'.
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(version -> (TestVersion.compare(version, "1.4.0") >= 0));
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    }
    return result;
  }

  private static Object[] addReceiversWithHostNameForSenders(String version) {
    return new Object[] {version,
        "WANRollingUpgradeMultipleReceiversDefinedInClusterConfiguration_ReceiverWithHostNameForSenders.xml",
        2, 0};
  }

  private static Object[] addReceiversWithBindAddresses(String version) {
    return new Object[] {version,
        "WANRollingUpgradeMultipleReceiversDefinedInClusterConfiguration_ReceiverWithBindAddresses.xml",
        2, 0};
  }

  private static Object[] addMultipleReceiversWithDefaultAttributes(String version) {
    return new Object[] {version,
        "WANRollingUpgradeMultipleReceiversDefinedInClusterConfiguration_MultipleReceiversWithDefaultAttributes.xml",
        2, 1};
  }

  private static Object[] addSingleReceiverWithDefaultAttributes(String version) {
    return new Object[] {version,
        "WANRollingUpgradeMultipleReceiversDefinedInClusterConfiguration_SingleReceiverWithDefaultAttributes.xml",
        1, 1};
  }

  private static Object[] addSingleReceiverWithDefaultBindAddress(String version) {
    return new Object[] {version,
        "WANRollingUpgradeMultipleReceiversDefinedInClusterConfiguration_SingleReceiverWithBindAddress.xml",
        1, 1};
  }

  @Test
  public void testMultipleReceiversRemovedDuringRoll() {
    // Get old locator properties
    VM locator = Host.getHost(0).getVM(oldVersion, 0);
    String hostName = NetworkUtils.getServerHostName();
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = hostName + "[" + locatorPort + "]";
    // Start old locator
    locator.invoke(() -> {
      DistributedTestUtils.deleteLocatorStateFile(locatorPort);
      startLocator(locatorPort, 0,
          locators, null, true);
    });

    // Wait for configuration configuration to be ready.
    locator.invoke(
        () -> await()
            .untilAsserted(() -> assertThat(
                InternalLocator.getLocator().isSharedConfigurationRunning()).isTrue()));

    // Add cluster configuration elements containing multiple receivers
    locator.invoke(this::addMultipleGatewayReceiverElementsToClusterConfiguration);

    // Roll old locator to current
    rollLocatorToCurrent(locator, locatorPort, 0, locators,
        null, true);

    // Verify cluster configuration contains expected number of receivers
    locator.invoke(this::verifyGatewayReceiverClusterConfigurationElements);

    // Start member in current version with cluster configuration enabled
    VM server = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 1);
    server.invoke(() -> createCache(locators, true, true));

    // Verify member has expected number of receivers
    server.invoke(this::verifyGatewayReceivers);
  }

  private void addMultipleGatewayReceiverElementsToClusterConfiguration()
      throws Exception {
    // Load cacheXML file into String
    URL url = this.getClass().getClassLoader().getResource("org/apache/geode/cache/wan/" + xmlFile);
    String cacheXmlString = readFile(url);

    assertThat(url).isNotNull();

    // Get configuration region
    Region<String, Configuration> configurationRegion = CacheFactory.getAnyInstance().getRegion(
        InternalConfigurationPersistenceService.CONFIG_REGION_NAME);

    // Create a configuration and put into the configuration region
    Configuration configuration = new Configuration(ConfigurationPersistenceService.CLUSTER_CONFIG);
    configuration.setCacheXmlContent(cacheXmlString);
    configurationRegion.put(ConfigurationPersistenceService.CLUSTER_CONFIG, configuration);
  }

  private static String readFile(URL url) throws IOException {
    return new String(Files.readAllBytes(Paths.get(url.getPath())), StandardCharsets.UTF_8);
  }

  private void verifyGatewayReceiverClusterConfigurationElements() throws Exception {
    // Get configuration region
    Region<String, Configuration> configurationRegion = CacheFactory.getAnyInstance().getRegion(
        InternalConfigurationPersistenceService.CONFIG_REGION_NAME);

    // Get the configuration from the region
    Configuration configuration =
        configurationRegion.get(ConfigurationPersistenceService.CLUSTER_CONFIG);

    // Verify the configuration contains no gateway-receiver elements
    Document document =
        XmlUtils.createDocumentFromXml(configuration.getCacheXmlContent(),
            new ServiceLoaderModuleService(LogService.getLogger()));
    assertThat(document.getElementsByTagName("gateway-receiver").getLength())
        .isEqualTo(expectedReceivers);
  }

  private void verifyGatewayReceivers() {
    assertThat(CacheFactory.getAnyInstance().getGatewayReceivers().size())
        .isEqualTo(expectedReceivers);
  }

  private static class Attribute implements Serializable {

    private String name;

    private String value;

    private static final Attribute DEFAULT = new Attribute("default", "");

    Attribute(String name, String value) {
      this.name = name;
      this.value = value;
    }

    public String toString() {
      return name + "=" + value;
    }
  }
}
