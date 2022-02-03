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
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.apache.geode.internal.process.ProcessUtils.killProcess;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@RunWith(Parameterized.class)
public class SocketCreatorUpgradeTest {
  public static final String ALGORITHM = "SHA256withRSA";
  public static final int EXPIRATION = 1;
  public static final String STORE_PASSWORD = "geode";
  public static final String STORE_TYPE = "jks";
  public static final String PROTOCOL_TLSv1_2 = "TLSv1.2";
  public static final String PROTOCOL_TLSv1_2_SSLv2Hello = "TLSv1.2,SSLv2Hello";
  public static final String PROTOCOL_ANY = "any";
  public static final String VERSION_FULL = "version --full";

  private final String startLocator1;
  private final String startLocator2;
  // private final String connect;

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

  private final File keyStoreFile;
  private final File trustStoreFile;
  private final File securityPropertiesFile;

  @Parameters(name = "{0}")
  public static Collection<String> data() {
    final List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.14.0") < 0);
    return result;
  }

  public SocketCreatorUpgradeTest(final String version) throws IOException,
      GeneralSecurityException {
    final Path oldJavaHome = Paths.get(getenv("JAVA_HOME_8u265"));
    final Path newJavaHome = Paths.get(getenv("JAVA_HOME_8u272"));
    gfshOldGeodeOldJava = new GfshRule(version, oldJavaHome);
    gfshOldGeodeNewJava = new GfshRule(version, newJavaHome);
    gfshNewGeodeOldJava = new GfshRule(oldJavaHome);
    gfshNewGeodeNewJava = new GfshRule(newJavaHome);

    final UniquePortSupplier portSupplier = new UniquePortSupplier();
    int locator1Port = portSupplier.getAvailablePort();
    int locator2Port = portSupplier.getAvailablePort();

    tempFolder.create();
    keyStoreFile = tempFolder.newFile();
    trustStoreFile = tempFolder.newFile();
    securityPropertiesFile = tempFolder.newFile();

    final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
    generateKeyAndTrustStore(hostName, keyStoreFile, trustStoreFile);

    startLocator1 = startLocator("locator1", hostName, locator1Port,
        portSupplier.getAvailablePort(), securityPropertiesFile, locator2Port);
    startLocator2 = startLocator("locator2", hostName, locator2Port,
        portSupplier.getAvailablePort(), securityPropertiesFile, locator1Port);
    // connect =
    // format("connect --locator=%s[%d],%s[%d] --security-properties-file=%s", hostName,
    // locator1Port, hostName, locator2Port, securityPropertiesFile);
  }

  // @Before
  // public void before() {
  // System.out.println(gfshOldGeodeOldJava.getTemporaryFolder().getRoot());
  // System.out.println(gfshOldGeodeNewJava.getTemporaryFolder().getRoot());
  // }

  @Test
  public void upgradingGeodeWithProtocolsAny() throws IOException {
    generateSecurityProperties(PROTOCOL_ANY, securityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(VERSION_FULL, startLocator1);
    gfshNewGeodeOldJava.execute(VERSION_FULL, startLocator2);
  }

  @Test
  public void upgradingGeodeWithProtocolsTLSv1_2Hangs() throws IOException {
    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(startLocator1);
    assertThatThrownBy(() -> gfshNewGeodeOldJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES)))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(gfshNewGeodeOldJava, "locator2");
  }

  @Test
  public void upgradingGeodeWithProtocolsTLSv1_2_SSLv2Hello() throws IOException {
    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(startLocator1);
    gfshNewGeodeOldJava.execute(startLocator2);
  }

  @Test
  public void upgradingJavaWithProtocolsAnyHangs() throws IOException {
    generateSecurityProperties(PROTOCOL_ANY, securityPropertiesFile, keyStoreFile, trustStoreFile);

    gfshOldGeodeOldJava.execute(startLocator1);
    assertThatThrownBy(() -> gfshOldGeodeNewJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES)))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(gfshOldGeodeNewJava, "locator2");
  }

  @Test
  public void upgradingJavaWithProtocolsTLSv1_2Hangs() throws IOException {
    generateSecurityProperties(PROTOCOL_TLSv1_2, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(startLocator1);
    assertThatThrownBy(() -> gfshOldGeodeNewJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES)))
            .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(gfshOldGeodeNewJava, "locator2");
  }

  @Test
  public void upgradingJavaWithProtocolsTLSv1_2_SSLv2HelloHangs() throws IOException {
    generateSecurityProperties(PROTOCOL_TLSv1_2_SSLv2Hello, securityPropertiesFile, keyStoreFile,
        trustStoreFile);

    gfshOldGeodeOldJava.execute(startLocator1);
    assertThatThrownBy(() -> gfshOldGeodeNewJava.execute(
        GfshScript.of(startLocator2).awaitAtMost(1, TimeUnit.MINUTES)))
        .hasRootCauseInstanceOf(TimeoutException.class);

    killLocator(gfshOldGeodeNewJava, "locator2");
  }

  private static String startLocator(final String name, final String bindAddress,
      final int port, final int jmxPort, final File securityPropertiesFile,
      final int otherLocatorPort) {
    return format(
        "start locator --name=%s --bind-address=%s --port=%d --J=-Dgemfire.jmx-manager-port=%d --security-properties-file=%s --http-service-port=0 --locators=%s[%d]",
        name, bindAddress, port, jmxPort, securityPropertiesFile.getAbsolutePath(), bindAddress,
        otherLocatorPort);
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
      final File securityPropertiesFile,
      final File keyStoreFile, final File trustStoreFile) throws IOException {
    final Properties properties = new Properties();

    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    properties.setProperty(SSL_PROTOCOLS, protocols);

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
      killProcess(pid);
      Files.delete(pidFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void killLocator(final GfshRule gfsh, final String name) {
    killByPidFile(
        gfsh.getTemporaryFolder().getRoot().toPath().resolve(name).resolve("vf.gf.locator.pid"));
  }

}
