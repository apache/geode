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
package org.apache.geode.management;

import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startLocatorCommand;
import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startServerCommand;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

/**
 * This test iterates through the versions of Geode and executes client compatibility with
 * the current version of Geode.
 */
@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradeWithSslDUnitTest {
  private final UniquePortSupplier portSupplier = new UniquePortSupplier();
  private final String hostName;
  private File securityPropertiesFile;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    final List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.10.0") < 0);
    return result;
  }

  @Rule
  public GfshRule oldGfsh;

  @Rule
  public GfshRule currentGfsh;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  public RollingUpgradeWithSslDUnitTest(String version) throws UnknownHostException {
    oldGfsh = new GfshRule(version);
    currentGfsh = new GfshRule();
    hostName = InetAddress.getLocalHost().getCanonicalHostName();
  }

  @Before
  public void before() throws IOException, GeneralSecurityException {
    final Properties properties = generateSslProperties();

    // Since GfshRule provides no way to pass along Properties object to start server etc,
    // we must write the properties to an actual file.
    securityPropertiesFile = tempFolder.newFile("gfsecurity.properties");
    try (FileOutputStream fileOutputStream =
        new FileOutputStream(securityPropertiesFile.getAbsolutePath())) {
      properties.store(fileOutputStream, "");
    }
  }

  @Test
  public void testRollingUpgradeWithEndpointIdentificationEnabled() throws Exception {
    final int locatorPort = portSupplier.getAvailablePort();
    final int locatorJmxPort = portSupplier.getAvailablePort();
    final int locator2Port = portSupplier.getAvailablePort();
    final int locator2JmxPort = portSupplier.getAvailablePort();
    final int server1Port = portSupplier.getAvailablePort();
    final int server2Port = portSupplier.getAvailablePort();

    final GfshExecution startupExecution =
        GfshScript.of(
            startLocatorCommandWithConfig("locator1", locatorPort, locatorJmxPort, -1))
            .and(startLocatorCommandWithConfig("locator2", locator2Port, locator2JmxPort,
                locatorPort))
            .and(startServerCommandWithConfig("server1", server1Port, locatorPort))
            .and(startServerCommandWithConfig("server2", server2Port, locatorPort))
            .execute(oldGfsh, tempFolder.getRoot());

    initializeRegion(locatorPort);
    causeP2PTraffic(locatorPort);

    // doing rolling upgrades
    upgradeLocator("locator1", locatorPort, locatorJmxPort, locator2Port, startupExecution);
    verifyListMembers(locatorPort);
    causeP2PTraffic(locatorPort);

    upgradeLocator("locator2", locator2Port, locator2JmxPort, locatorPort, startupExecution);
    verifyListMembers(locatorPort);
    causeP2PTraffic(locatorPort);

    // make sure servers can do rolling upgrade too
    upgradeServer("server1", server1Port, locatorPort, startupExecution);
    causeP2PTraffic(locatorPort);

    upgradeServer("server2", server2Port, locatorPort, startupExecution);
    causeP2PTraffic(locatorPort);
  }

  private void upgradeLocator(String name, int locatorPort, int locatorJmxPort,
      int connectedLocatorPort,
      GfshExecution startupExecution) {
    oldGfsh.stopLocator(startupExecution, name);
    GfshScript
        .of(startLocatorCommandWithConfig(name, locatorPort, locatorJmxPort, connectedLocatorPort))
        .execute(currentGfsh, tempFolder.getRoot());
  }

  private void upgradeServer(String name, int serverPort, int locatorPort,
      GfshExecution startupExecution) {
    oldGfsh.stopServer(startupExecution, name);
    GfshScript.of(startServerCommandWithConfig(name, serverPort, locatorPort))
        .execute(currentGfsh, tempFolder.getRoot());
  }

  private void verifyListMembers(int locatorPort) {
    final GfshExecution members =
        GfshScript.of("connect --locator=" + hostName + "[" + locatorPort + "]")
            .and("list members")
            .execute(currentGfsh, tempFolder.getRoot());

    assertThat(members.getOutputText())
        .contains("locator1")
        .contains("locator2")
        .contains("server1")
        .contains("server2");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder.getRoot());
  }

  private String startServerCommandWithConfig(String server, int serverPort, int locatorPort) {
    return startServerCommand(server, hostName, serverPort, locatorPort) + additionalParameters();
  }

  private String startLocatorCommandWithConfig(String name, final int locatorPort,
      final int locatorJmxPort,
      final int connectedLocatorPort) {
    return startLocatorCommand(name, hostName, locatorPort, locatorJmxPort, 0, connectedLocatorPort)
        + additionalParameters();
  }

  private String additionalParameters() {
    final String propertiesFile =
        RollingUpgradeWithSslDUnitTest.class.getResource("gemfire.properties").getFile();

    return " --properties-file=" + propertiesFile +
        " --security-properties-file=" + securityPropertiesFile.getAbsolutePath() +
        " --J=-Dgemfire.forceDnsUse=true" +
        " --J=-Djdk.tls.trustNameService=true" +
        " --J=-Dgemfire.bind-address=" + hostName;
  }

  private void initializeRegion(int locatorPort) {
    final GfshExecution getResponse =
        GfshScript.of("connect --locator=" + hostName + "[" + locatorPort + "]")
            .and("create region --name=region1 --type=REPLICATE")
            .and("list regions")
            .execute(currentGfsh, tempFolder.getRoot());

    assertThat(getResponse.getOutputText()).contains("region1");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder.getRoot());
  }

  private void causeP2PTraffic(int locatorPort) {
    final GfshExecution getResponse =
        GfshScript.of("connect --locator=" + hostName + "[" + locatorPort + "]")
            .and("put --key='key1' --value='value1' --region=region1")
            .and("get --key='key1' --region=region1")
            .execute(currentGfsh, tempFolder.getRoot());

    assertThat(getResponse.getOutputText()).contains("value1");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder.getRoot());
  }

  public Properties generateSslProperties() throws IOException, GeneralSecurityException {
    final CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    final CertificateMaterial certificate = new CertificateBuilder()
        .commonName(hostName)
        .issuedBy(ca)
        .sanDnsName(hostName)
        .generate();

    final CertStores store = new CertStores(hostName);
    store.withCertificate("geode", certificate);
    store.trust("ca", ca);

    return store.propertiesWith("cluster", true, true);
  }
}
