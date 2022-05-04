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

import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.concurrent.CompletableFuture.runAsync;
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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecutor;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@RunWith(Parameterized.class)
public class SocketCreatorUpgradeTest {

  private static final String ALGORITHM = "SHA256withRSA";
  private static final int EXPIRATION = 1;
  private static final String STORE_PASSWORD = "geode";
  private static final String STORE_TYPE = "jks";
  private static final String PROTOCOL_TLSv1_2 = "TLSv1.2";
  private static final String PROTOCOL_TLSv1_2_SSLv2Hello = "TLSv1.2,SSLv2Hello";
  private static final String PROTOCOL_ANY = "any";
  private static final String LOCATOR_1 = "locator1";
  private static final String LOCATOR_2 = "locator2";

  private final String version;
  private final TestVersion testVersion;

  private String startLocator1;
  private String startLocator2;
  private String startLocator1New;
  private String startLocator2New;
  private String stopLocator1;
  private String stopLocator2;

  @Rule(order = 0)
  public final FolderRule tempFolder = new FolderRule();
  @Rule(order = 1)
  public final GfshRule gfshRule = new GfshRule();

  private GfshExecutor gfshOldGeodeOldJava;
  private GfshExecutor gfshOldGeodeNewJava;
  private GfshExecutor gfshNewGeodeOldJava;
  private GfshExecutor gfshNewGeodeNewJava;

  private Path root;
  private File keyStoreFile;
  private File trustStoreFile;
  private File securityPropertiesFile;
  private File newSecurityPropertiesFile;

  private File locator1LogFile;
  private File locator2LogFile;

  @Parameters(name = "{0}")
  public static Collection<String> data() {
    final List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(v -> TestVersion.valueOf(v).lessThan(TestVersion.valueOf("1.8.0")));
    Collections.reverse(result);
    return result;
  }

  public SocketCreatorUpgradeTest(final String version) {
    this.version = version;
    testVersion = TestVersion.valueOf(version);
  }

  @Before
  public void setUp() throws IOException, GeneralSecurityException {
    final Path oldJavaHome = Paths.get(getenv("JAVA_HOME_8u265"));
    final Path newJavaHome = Paths.get(getenv("JAVA_HOME_8u272"));

    root = tempFolder.getFolder().toPath();

    gfshOldGeodeOldJava = gfshRule.executor()
        .withGeodeVersion(version)
        .withJavaHome(oldJavaHome)
        .build(root);
    gfshOldGeodeNewJava = gfshRule.executor()
        .withGeodeVersion(version)
        .withJavaHome(newJavaHome)
        .build(root);
    gfshNewGeodeOldJava = gfshRule.executor()
        .withJavaHome(oldJavaHome)
        .build(root);
    gfshNewGeodeNewJava = gfshRule.executor()
        .withJavaHome(newJavaHome)
        .build(root);

    final UniquePortSupplier portSupplier = new UniquePortSupplier();
    final int locator1Port = portSupplier.getAvailablePort();
    final int locator1JmxPort = portSupplier.getAvailablePort();
    final int locator2Port = portSupplier.getAvailablePort();
    final int locator2JmxPort = portSupplier.getAvailablePort();

    keyStoreFile = Files.createFile(root.resolve("keyStoreFile")).toFile();
    trustStoreFile = Files.createFile(root.resolve("trustStoreFile")).toFile();
    securityPropertiesFile = Files.createFile(root.resolve("security.properties")).toFile();
    newSecurityPropertiesFile = Files.createFile(root.resolve("newSecurity.properties")).toFile();

    final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
    generateKeyAndTrustStore(hostName, keyStoreFile, trustStoreFile);

    startLocator1 = startLocator(LOCATOR_1, hostName, locator1Port, locator1JmxPort,
        securityPropertiesFile, locator2Port, testVersion);
    startLocator2 = startLocator(LOCATOR_2, hostName, locator2Port, locator2JmxPort,
        securityPropertiesFile, locator1Port, testVersion);

    startLocator1New = startLocator(LOCATOR_1, hostName, locator1Port, locator1JmxPort,
        newSecurityPropertiesFile, locator2Port, testVersion);
    startLocator2New = startLocator(LOCATOR_2, hostName, locator2Port, locator2JmxPort,
        newSecurityPropertiesFile, locator1Port, testVersion);

    stopLocator1 = stopLocator(LOCATOR_1);
    stopLocator2 = stopLocator(LOCATOR_2);

    locator1LogFile = root.resolve(LOCATOR_1).resolve(LOCATOR_1 + ".log")
        .toAbsolutePath().toFile();
    locator2LogFile = root.resolve(LOCATOR_2).resolve(LOCATOR_2 + ".log")
        .toAbsolutePath().toFile();
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
    assumeThat(testVersion)
        .as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .satisfiesAnyOf(
            v -> assertThat(v).isLessThanOrEqualTo(TestVersion.valueOf("1.12.0")),
            v -> assertThat(v).isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0")));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    gfshOldGeodeOldJava.execute(root, startLocator2);
  }

  @Test
  public void startingOldGeode1_12_1_UpTo1_13_0WithProtocolsTLSv1_2Hangs() throws IOException {
    assumeThat(testVersion).as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.12.1"))
        .isLessThan(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    runAsync(() -> gfshOldGeodeOldJava.execute(root, startLocator2));

    await().untilAsserted(
        () -> assertThat(locator1LogFile).content().contains("SSLv2Hello is disabled"));
  }

  @Test
  public void upgradingToNewGeodeOnOldJavaWithProtocolsTLSv1_2() throws IOException {
    assumeThat(testVersion).as("TODO")
        .isLessThanOrEqualTo(TestVersion.valueOf("1.12.0"));
    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
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
  public void upgradingToNewGeodeOnOldJavaWithProtocolsTLSv1_2Hangs() throws IOException {
    assumeThat(testVersion).as("Geode 1.12.0 and older can upgrade.")
        .isGreaterThan(TestVersion.valueOf("1.12.0"));
    assumeThat(testVersion).as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    runAsync(() -> gfshNewGeodeOldJava.execute(root, startLocator2));

    await().untilAsserted(
        () -> assertThat(locator2LogFile).content().contains("SSLv2Hello is disabled"));
  }

  @Test
  public void upgradingToNewGeodeOnOldJavaWithProtocolsTLSv1_2WithNewProperties()
      throws IOException {
    assumeThat(testVersion)
        .as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .satisfiesAnyOf(
            v -> assertThat(v).isLessThanOrEqualTo(TestVersion.valueOf("1.12.0")),
            v -> assertThat(v).isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0")));

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
    assumeThat(testVersion)
        .as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .satisfiesAnyOf(
            v -> assertThat(v).isLessThanOrEqualTo(TestVersion.valueOf("1.12.0")),
            v -> assertThat(v).isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0")));

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
    assumeThat(testVersion).as("Only Geode older than 1.13.0 can directly upgrade Java version.")
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
    assumeThat(testVersion).as("Geode older than 1.13.0 can directly upgrade Java version.")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_ANY, securityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    runAsync(() -> gfshOldGeodeNewJava.execute(root, startLocator2));

    await().atMost(1, TimeUnit.MINUTES).untilAsserted(
        () -> assertThat(locator2LogFile).content().contains("SSLv2Hello is not enabled"));
  }

  @Test
  public void upgradingToNewJavaOnOldGeodeWithProtocolsTLSv1_2() throws IOException {
    assumeThat(testVersion).as("Only Geode older than 1.12.1 can directly upgrade Java version.")
        .isLessThan(TestVersion.valueOf("1.12.1"));

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
  public void upgradingToNewJavaOnOldGeodeWithProtocolsTLSv1_2Hangs() throws IOException {
    assumeThat(testVersion).as("Geode 1.12.0 and older can upgrade.")
        .isGreaterThan(TestVersion.valueOf("1.12.0"));
    assumeThat(testVersion)
        .as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .satisfiesAnyOf(
            v -> assertThat(v).isLessThanOrEqualTo(TestVersion.valueOf("1.12.0")),
            v -> assertThat(v).isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0")));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    runAsync(() -> gfshOldGeodeNewJava.execute(root, startLocator2));

    await().atMost(1, TimeUnit.MINUTES).untilAsserted(
        () -> assertThat(locator2LogFile).content().contains("SSLv2Hello is not enabled"));
  }

  @Test
  public void upgradingToNewJavaOnOldGeodeWithProtocolsTLSv1_2_SSLv2Hello() throws IOException {
    assumeThat(testVersion).as("Only Geode older than 1.13.0 can directly upgrade Java version.")
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
    assumeThat(testVersion).as("Geode older than 1.13.0 can directly upgrade Java version.")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    runAsync(() -> gfshOldGeodeNewJava.execute(root, startLocator2));

    await().atMost(1, TimeUnit.MINUTES).untilAsserted(
        () -> assertThat(locator2LogFile).content().contains("SSLv2Hello is not enabled"));
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
    assumeThat(testVersion).as("Geode 1.12.0 and older can upgrade.")
        .isGreaterThan(TestVersion.valueOf("1.12.0"));
    assumeThat(testVersion).as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0"));

    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(root, startLocator1);
    runAsync(() -> gfshNewGeodeNewJava.execute(root, startLocator2));

    await().atMost(1, TimeUnit.MINUTES).untilAsserted(
        () -> assertThat(locator2LogFile).content().contains("SSLv2Hello is not enabled"));
  }

  @Test
  public void upgradingToNewGeodeAndNewJavaWithProtocolsTLSv1_2() throws IOException {
    assumeThat(testVersion)
        .as("Only Geode older than 1.12.1 can directly upgrade Geode and Java version.")
        .isLessThan(TestVersion.valueOf("1.12.1"));

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
  public void upgradingToNewGeodeAndNewJavaWithProtocolsTLSv1_2WithNewProperties()
      throws IOException {
    assumeThat(testVersion)
        .as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .satisfiesAnyOf(
            v -> assertThat(v).isLessThanOrEqualTo(TestVersion.valueOf("1.12.0")),
            v -> assertThat(v).isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0")));

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
    assumeThat(testVersion)
        .as("Geode between [1.12.1, 1.3.0) can't connect p2p with just TLSv1.2")
        .satisfiesAnyOf(
            v -> assertThat(v).isLessThanOrEqualTo(TestVersion.valueOf("1.12.0")),
            v -> assertThat(v).isGreaterThanOrEqualTo(TestVersion.valueOf("1.13.0")));

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
      final int otherLocatorPort, final TestVersion version) {
    String startLocator = format(
        "start locator --connect=false --http-service-port=0 --name=%s --bind-address=%s --port=%d --J=-Dgemfire.jmx-manager-port=%d --security-properties-file=%s --locators=%s[%d]",
        name, bindAddress, port, jmxPort, securityPropertiesFile.getAbsolutePath(), bindAddress,
        otherLocatorPort);
    if (version.lessThan(TestVersion.valueOf("1.12.1"))) {
      // Older versions don't honor the bind-address hostname and require reverse lookups on IP
      startLocator += " --J=-Djdk.tls.trustNameService=true";
    }
    if (version.lessThan(TestVersion.valueOf("1.12.0"))) {
      // Older versions don't honor the bind-address hostname and require reverse lookups on IP
      startLocator += " --J=-Dgemfire.forceDnsUse=true";
    }
    return startLocator;
  }

  private static String stopLocator(final String name) {
    return format("stop locator --dir=%s", name);
  }

  private static void generateKeyAndTrustStore(final String hostName, final File keyStoreFile,
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
        keyStoreFile, trustStoreFile);
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
}
