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

package org.apache.geode.internal.net;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.nio.charset.Charset.defaultCharset;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CLIENT_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_SERVER_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.internal.shared.NativeCalls;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@RunWith(Parameterized.class)
public class SocketCreatorUpgradeTest {
  private static final TestVersion MINIMUM_VERSION = TestVersion.valueOf("1.12.0");

  private static final String ALGORITHM = "SHA256withRSA";
  private static final int EXPIRATION = 1;
  private static final String STORE_PASSWORD = "geode";
  private static final String STORE_TYPE = "jks";
  private static final String PROTOCOL_TLSv1_2 = "TLSv1.2";
  private static final String PROTOCOL_TLSv1_2_SSLv2Hello = "TLSv1.2,SSLv2Hello";
  private static final String PROTOCOL_ANY = "any";
  private static final String LOCATOR_1 = "locator1";
  private static final String LOCATOR_2 = "locator2";

  private final TestVersion version;

  private final String startLocator1;
  private final String startLocator2;
  private final String startLocator1New;
  private final String startLocator2New;
  private final String stopLocator1;
  private final String stopLocator2;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final GfshRule gfshOldGeodeOldJava;

  @Rule
  public final GfshRule gfshOldGeodeNewJava;

  @Rule
  public final GfshRule gfshNewGeodeOldJava;

  @Rule
  public final GfshRule gfshNewGeodeNewJava;

  private final File root;
  private final File keyStoreFile;
  private final File trustStoreFile;
  private final File securityPropertiesFile;
  private final File newSecurityPropertiesFile;

  @Parameters(name = "{0}")
  public static Collection<String> data() {
    final List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(v -> TestVersion.valueOf(v).greaterThanOrEqualTo(MINIMUM_VERSION));
    return result;
  }

  public SocketCreatorUpgradeTest(final String version) throws IOException,
      GeneralSecurityException {

    this.version = TestVersion.valueOf(version);

    final Path oldJavaHome = Paths.get(getenv("JAVA_HOME_8u265"));
    final Path newJavaHome = Paths.get(getenv("JAVA_HOME_8u272"));

    gfshOldGeodeOldJava = new GfshRule(version, oldJavaHome);
    gfshOldGeodeNewJava = new GfshRule(version, newJavaHome);
    gfshNewGeodeOldJava = new GfshRule(oldJavaHome);
    gfshNewGeodeNewJava = new GfshRule(newJavaHome);

    final UniquePortSupplier portSupplier = new UniquePortSupplier();
    final int locator1Port = portSupplier.getAvailablePort();
    final int locator1JmxPort = portSupplier.getAvailablePort();
    final int locator2Port = portSupplier.getAvailablePort();
    final int locator2JmxPort = portSupplier.getAvailablePort();

    tempFolder.create();
    root = tempFolder.getRoot();
    keyStoreFile = tempFolder.newFile();
    trustStoreFile = tempFolder.newFile();
    securityPropertiesFile = tempFolder.newFile();
    newSecurityPropertiesFile = tempFolder.newFile();

    final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
    generateKeyAndTrustStore(hostName, keyStoreFile, trustStoreFile);

    startLocator1 = startLocator(LOCATOR_1, hostName, locator1Port, locator1JmxPort,
        securityPropertiesFile, locator2Port);
    startLocator2 = startLocator(LOCATOR_2, hostName, locator2Port, locator2JmxPort,
        securityPropertiesFile, locator1Port);

    startLocator1New = startLocator(LOCATOR_1, hostName, locator1Port, locator1JmxPort,
        newSecurityPropertiesFile, locator2Port);
    startLocator2New = startLocator(LOCATOR_2, hostName, locator2Port, locator2JmxPort,
        newSecurityPropertiesFile, locator1Port);

    stopLocator1 = stopLocator(LOCATOR_1);
    stopLocator2 = stopLocator(LOCATOR_2);
  }

  @Test
  public void upgradingToNewGeodeOnOldJavaWithProtocolsAny() throws IOException {
    generateSecurityProperties(PROTOCOL_ANY, securityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade to new version, no SSL configuration changes
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeOldJava.execute(root, startLocator2);
  }

  @Test
  public void startingOldGeodeWithProtocolsTLSv1_2() throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);
  }

  @Test
  public void startingOldGeode1_12WithProtocolsTLSv1_2Hangs() throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(MINIMUM_VERSION)
        .isLessThan(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    assertThatThrownBy(() -> gfshOldGeodeOldJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES), root))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(root, LOCATOR_2);
  }

  @Test
  public void upgradingToNewGeodeOnOldJavaWithProtocolsTLSv1_2Hangs() throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    assertThatThrownBy(() -> gfshNewGeodeOldJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES), root))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(root, LOCATOR_2);
  }

  @Test
  public void upgradingToNewGeodeOnOldJavaWithProtocolsTLSv1_2WithNewProperties()
      throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2,
        securityPropertiesFile, keyStoreFile, trustStoreFile);

    generateSecurityProperties(PROTOCOL_TLSv1_2, PROTOCOL_TLSv1_2_SSLv2Hello,
        newSecurityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade with new security properties
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeOldJava.execute(root, startLocator1New);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeOldJava.execute(root, startLocator2New);
  }

  @Test
  public void upgradingToNewGeodeOnOldJavaWithProtocolsTLSv1_2ThroughNewProperties()
      throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2,
        securityPropertiesFile, keyStoreFile, trustStoreFile);

    generateSecurityProperties(PROTOCOL_TLSv1_2, PROTOCOL_TLSv1_2_SSLv2Hello,
        newSecurityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade with new security properties
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeOldJava.execute(root, startLocator1New);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeOldJava.execute(root, startLocator2New);

    // Go back to original security properties
    gfshNewGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeOldJava.execute(root, startLocator1);
    gfshNewGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeOldJava.execute(root, startLocator2);
  }

  @Test
  public void startingOldGeodeWithProtocolsTLSv1_2_SSLv2Hello() throws IOException {
    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);
  }

  @Test
  public void upgradingToNewGeodeOnOldJavaWithProtocolsTLSv1_2_SSLv2Hello() throws IOException {
    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade to new version, no SSL configuration changes
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeOldJava.execute(root, startLocator2);
  }

  @Test
  public void upgradingToNewJavaOnOldGeodeWithProtocolsAny() throws IOException {
    assumeThat(version).as("Only Geode 1.12 can directly upgrade Java version.")
        .isGreaterThanOrEqualTo(MINIMUM_VERSION)
        .isLessThan(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_ANY, securityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade to new java, no SSL configuration changes
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshOldGeodeNewJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshOldGeodeNewJava.execute(root, startLocator2);
  }

  @Test
  public void upgradingToNewJavaOnOldGeodeWithProtocolsAnyHangs() throws IOException {
    assumeThat(version).as("Geode 1.12 can directly upgrade Java version.")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_ANY, securityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    assertThatThrownBy(() -> gfshOldGeodeNewJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES), root))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(root, LOCATOR_2);
  }

  @Test
  public void upgradingToNewJavaOnOldGeodeWithProtocolsTLSv1_2Hangs() throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    assertThatThrownBy(() -> gfshOldGeodeNewJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES), root))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(root, LOCATOR_2);
  }

  @Test
  public void upgradingToNewJavaOnOldGeodeWithProtocolsTLSv1_2_SSLv2Hello() throws IOException {
    assumeThat(version).as("Only Geode 1.12 can directly upgrade Java version.")
        .isGreaterThanOrEqualTo(MINIMUM_VERSION)
        .isLessThan(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade to new java, no SSL configuration changes
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshOldGeodeNewJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshOldGeodeNewJava.execute(root, startLocator2);
  }

  @Test
  public void upgradingToNewJavaOnOldGeodeWithProtocolsTLSv1_2_SSLv2HelloHangs()
      throws IOException {
    assumeThat(version).as("Geode 1.12 can directly upgrade Java version.")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    assertThatThrownBy(() -> gfshOldGeodeNewJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES), root))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(root, LOCATOR_2);
  }

  @Test
  public void upgradingToNewGeodeAndNewJavaWithProtocolsAny() throws IOException {
    generateSecurityProperties(PROTOCOL_ANY, securityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade to new java and new geode, no SSL configuration changes
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeNewJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeNewJava.execute(root, startLocator2);
  }

  @Test
  public void upgradingToNewGeodeAndNewJavaWithProtocolsTLSv1_2Hangs() throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    assertThatThrownBy(() -> gfshNewGeodeNewJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES), root))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(root, LOCATOR_2);
  }

  @Test
  public void upgradingToNewGeodeAndNewJavaWithProtocolsTLSv1_2WithNewProperties()
      throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2,
        securityPropertiesFile, keyStoreFile, trustStoreFile);

    generateSecurityProperties(PROTOCOL_TLSv1_2, PROTOCOL_TLSv1_2_SSLv2Hello,
        newSecurityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade with new security properties
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeNewJava.execute(root, startLocator1New);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeNewJava.execute(root, startLocator2New);
  }

  @Test
  public void upgradingToNewGeodeAndNewJavaWithProtocolsTLSv1_2ThroughNewProperties()
      throws IOException {
    assumeThat(version).as("Geode 1.12 can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2,
        securityPropertiesFile, keyStoreFile, trustStoreFile);

    generateSecurityProperties(PROTOCOL_TLSv1_2, PROTOCOL_TLSv1_2_SSLv2Hello,
        newSecurityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade with new security properties
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeNewJava.execute(root, startLocator1New);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeNewJava.execute(root, startLocator2New);

    // Go back to original security properties
    gfshNewGeodeNewJava.execute(root, stopLocator1);
    gfshNewGeodeNewJava.execute(root, startLocator1);
    gfshNewGeodeNewJava.execute(root, stopLocator2);
    gfshNewGeodeNewJava.execute(root, startLocator2);
  }

  @Test
  public void upgradingToNewGeodeAndNewJavaWithProtocolsTLSv1_2_SSLv2Hello() throws IOException {
    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);

    // upgrade to new java and new geode, no SSL configuration changes
    gfshOldGeodeOldJava.execute(root, stopLocator1);
    gfshNewGeodeNewJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, stopLocator2);
    gfshNewGeodeNewJava.execute(root, startLocator2);
  }

  private static String startLocator(final String name, final String bindAddress,
      final int port, final int jmxPort, final File securityPropertiesFile,
      final int otherLocatorPort) {
    return format(
        "start locator --connect=false --http-service-port=0 --name=%s --bind-address=%s --port=%d --J=-Dgemfire.jmx-manager-port=%d --security-properties-file=%s",
        name, bindAddress, port, jmxPort, securityPropertiesFile.getAbsolutePath()) +
        ((otherLocatorPort > 0) ? format(" --locators=%s[%d]", bindAddress, otherLocatorPort) : "");
  }

  private static String stopLocator(final String name) {
    return format("stop locator --dir=%s", name);
  }

  public static void generateKeyAndTrustStore(final String hostName, final File keyStoreFile,
      final File trustStoreFile) throws IOException, GeneralSecurityException {
    final CertificateMaterial ca = new CertificateBuilder(EXPIRATION, ALGORITHM)
        .commonName("Test CA")
        .isCA()
        .generate();

    final CertificateMaterial certificate = new CertificateBuilder(EXPIRATION, ALGORITHM)
        .commonName(hostName)
        .issuedBy(ca)
        .sanDnsName(hostName)
        .generate();

    final CertStores store = new CertStores(hostName);
    store.withCertificate("geode", certificate);
    store.trust("ca", ca);

    store.createKeyStore(keyStoreFile.getAbsolutePath(), STORE_PASSWORD);
    store.createTrustStore(trustStoreFile.getPath(), STORE_PASSWORD);
  }

  private static void generateSecurityProperties(final String protocols,
      final File securityPropertiesFile, final File keyStoreFile, final File trustStoreFile)
      throws IOException {
    generateSecurityProperties(protocols, null, null, securityPropertiesFile, keyStoreFile,
        trustStoreFile);
  }

  private static void generateSecurityProperties(final String clientProtocols,
      final String serverProtocols, final File securityPropertiesFile, final File keyStoreFile,
      final File trustStoreFile) throws IOException {
    generateSecurityProperties(null, clientProtocols, serverProtocols, securityPropertiesFile,
        keyStoreFile,
        trustStoreFile);
  }

  private static void generateSecurityProperties(final String protocols,
      final String clientProtocols, final String serverProtocols, final File securityPropertiesFile,
      final File keyStoreFile, final File trustStoreFile) throws IOException {
    final Properties properties = new Properties();

    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    if (null != protocols) {
      properties.setProperty(SSL_PROTOCOLS, protocols);
    }
    if (null != clientProtocols) {
      properties.setProperty(SSL_CLIENT_PROTOCOLS, clientProtocols);
    }
    if (null != serverProtocols) {
      properties.setProperty(SSL_SERVER_PROTOCOLS, serverProtocols);
    }

    properties.setProperty(SSL_KEYSTORE, keyStoreFile.getAbsolutePath());
    properties.setProperty(SSL_KEYSTORE_TYPE, STORE_TYPE);
    properties.setProperty(SSL_KEYSTORE_PASSWORD, STORE_PASSWORD);

    properties.setProperty(SSL_TRUSTSTORE, trustStoreFile.getAbsolutePath());
    properties.setProperty(SSL_TRUSTSTORE_TYPE, STORE_TYPE);
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, STORE_PASSWORD);

    properties.store(new FileWriter(securityPropertiesFile), null);
  }

  private static void killByPidFile(final Path pidFile) {
    try {
      final int pid = parseInt(readFileToString(pidFile.toFile(), defaultCharset()));
      NativeCalls.getInstance().killProcess(pid);
      Files.delete(pidFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void killLocator(final File root, final String name) {
    killByPidFile(root.toPath().resolve(name).resolve("vf.gf.locator.pid"));
  }

}
