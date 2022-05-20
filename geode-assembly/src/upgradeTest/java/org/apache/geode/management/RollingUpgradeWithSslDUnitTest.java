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

import static java.util.stream.Collectors.toList;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.apache.geode.test.version.TestVersions.atLeast;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshExecutor;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

/**
 * This test iterates through the versions of Geode and executes client compatibility with
 * the current version of Geode.
 */
@Category(BackwardCompatibilityTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradeWithSslDUnitTest {

  @Parameters(name = "{0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(atLeast(TestVersion.valueOf("1.10.0"))))
        .collect(toList());
  }

  private final UniquePortSupplier portSupplier = new UniquePortSupplier();

  private final VmConfiguration sourceVmConfiguration;

  private String hostName;
  private String keyStoreFileName;
  private String trustStoreFileName;
  private File securityPropertiesFile;
  private GfshExecutor oldGfsh;
  private GfshExecutor currentGfsh;
  private Path tempFolder;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  public RollingUpgradeWithSslDUnitTest(VmConfiguration vmConfiguration) {
    sourceVmConfiguration = vmConfiguration;
  }

  @Before
  public void setUp() throws IOException, GeneralSecurityException {
    oldGfsh = gfshRule.executor().withVmConfiguration(sourceVmConfiguration).build();
    currentGfsh = gfshRule.executor().build();
    hostName = InetAddress.getLocalHost().getCanonicalHostName();
    keyStoreFileName = hostName + "-keystore.jks";
    trustStoreFileName = hostName + "-truststore.jks";

    tempFolder = folderRule.getFolder().toPath();

    generateStores();
    /*
     * We must use absolute paths for truststore and keystore in properties file and
     * since we don't know those at coding-time, we must generate the file.
     * Since GfshRule provides no way to pass along Properties object to start server etc,
     * we must write the properties to an actual file.
     */
    final Properties properties = generateSslProperties();

    securityPropertiesFile = tempFolder.resolve("gfsecurity.properties").toFile();
    Files.createFile(securityPropertiesFile.toPath());
    final FileOutputStream fileOutputStream =
        new FileOutputStream(securityPropertiesFile.getAbsolutePath());
    properties.store(fileOutputStream, "");
  }

  @Test
  public void testRollingUpgradeWithDeployment() {
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
            .execute(oldGfsh, tempFolder);

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
      int connectedLocatorPort, GfshExecution startupExecution) {
    startupExecution.locatorStopper().stop(name);
    GfshScript
        .of(startLocatorCommandWithConfig(name, locatorPort, locatorJmxPort, connectedLocatorPort))
        .execute(currentGfsh, tempFolder);
  }

  private void upgradeServer(String name, int serverPort, int locatorPort,
      GfshExecution startupExecution) {
    startupExecution.serverStopper().stop(name);
    GfshScript.of(startServerCommandWithConfig(name, serverPort, locatorPort))
        .execute(currentGfsh, tempFolder);
  }

  private Properties generateSslProperties() {
    final Properties properties = new Properties();

    properties.setProperty(BIND_ADDRESS, hostName);
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_ENABLED_COMPONENTS, "cluster,server");
    properties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    properties.setProperty(SSL_KEYSTORE, tempFolder + "/" + keyStoreFileName);
    properties.setProperty(SSL_KEYSTORE_TYPE, "jks");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "geode");

    properties.setProperty(SSL_TRUSTSTORE, tempFolder + "/" + trustStoreFileName);
    properties.setProperty(SSL_TRUSTSTORE_TYPE, "jks");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");

    return properties;
  }

  private void verifyListMembers(int locatorPort) {
    final GfshExecution members =
        GfshScript.of("connect --locator=" + hostName + "[" + locatorPort + "]")
            .and("list members")
            .execute(currentGfsh, tempFolder);

    assertThat(members.getOutputText())
        .contains("locator1")
        .contains("locator2")
        .contains("server1")
        .contains("server2");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder);
  }

  private String startServerCommandWithConfig(String server, int serverPort, int locatorPort) {
    return startServerCommand(server, hostName, serverPort, locatorPort) + additionalParameters();
  }

  private String startLocatorCommandWithConfig(String name, final int locatorPort,
      final int locatorJmxPort, final int connectedLocatorPort) {
    return startLocatorCommand(name, hostName, locatorPort, locatorJmxPort, 0, connectedLocatorPort)
        + additionalParameters();
  }

  private String additionalParameters() {
    final String propertiesFile =
        RollingUpgradeWithSslDUnitTest.class.getResource("gemfire.properties").getFile();

    return " --properties-file=" + propertiesFile +
        " --security-properties-file=" + securityPropertiesFile.getAbsolutePath() +
        " --J=-Dgemfire.forceDnsUse=true --J=-Djdk.tls.trustNameService=true";
  }

  private void initializeRegion(int locatorPort) {
    final GfshExecution getResponse =
        GfshScript.of("connect --locator=" + hostName + "[" + locatorPort + "]")
            .and("create region --name=region1 --type=REPLICATE")
            .and("list regions")
            .execute(currentGfsh, tempFolder);

    assertThat(getResponse.getOutputText()).contains("region1");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder);
  }

  private void causeP2PTraffic(int locatorPort) {
    final GfshExecution getResponse =
        GfshScript.of("connect --locator=" + hostName + "[" + locatorPort + "]")
            .and("put --key='123abc' --value='Hello World!!' --region=region1")
            .and("get --key='123abc' --region=region1")
            .execute(currentGfsh, tempFolder);

    assertThat(getResponse.getOutputText()).contains("Hello World!!");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder);
  }

  private void generateStores() throws IOException, GeneralSecurityException {
    final String algorithm = "SHA256withRSA";
    final CertificateMaterial ca = new CertificateBuilder(365, algorithm)
        .commonName("Test CA")
        .isCA()
        .generate();

    final CertificateMaterial certificate = new CertificateBuilder(365, algorithm)
        .commonName(hostName)
        .issuedBy(ca)
        .sanDnsName(hostName)
        .generate();

    final CertStores store = new CertStores(hostName);
    store.withCertificate("geode", certificate);
    store.trust("ca", ca);

    final File keyStoreFile = new File(tempFolder.toFile(), keyStoreFileName);
    keyStoreFile.createNewFile();
    store.createKeyStore(keyStoreFile.getAbsolutePath(), "geode");
    System.out.println("Keystore created: " + keyStoreFile.getAbsolutePath());

    final File trustStoreFile = new File(tempFolder.toFile(), trustStoreFileName);
    trustStoreFile.createNewFile();
    store.createTrustStore(trustStoreFile.getPath(), "geode");
    System.out.println("Truststore created: " + trustStoreFile.getAbsolutePath());
  }

  private static String startServerCommand(String name, String hostname, int port,
      int connectedLocatorPort) {
    return "start server --name=" + name
        + " --server-port=" + port
        + " --locators=" + hostname + "[" + connectedLocatorPort + "]";
  }

  private static String startLocatorCommand(String name, String hostname, int port, int jmxPort,
      int httpPort, int connectedLocatorPort) {
    String command = "start locator --name=" + name
        + " --port=" + port
        + " --http-service-port=" + httpPort;
    if (connectedLocatorPort > 0) {
      command += " --locators=" + hostname + "[" + connectedLocatorPort + "]";
    }
    command += " --J=-Dgemfire.jmx-manager-port=" + jmxPort;
    return command;
  }
}
