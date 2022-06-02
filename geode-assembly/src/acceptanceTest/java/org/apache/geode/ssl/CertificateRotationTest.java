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
package org.apache.geode.ssl;

import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;
import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

/**
 * This test creates a cluster and a client with SSL enabled for all components and client
 * authentication enabled.
 *
 * <p>
 * It verifies that the cluster certificate, the client certificate, and the CA certificate can be
 * rotated without having to restart the client or the members.
 */
public class CertificateRotationTest {

  private static final String regionName = "region";
  private static final String dummyStorePass = "geode";
  private static final Pattern updatedKeyManager = compile("Updated KeyManager");
  private static final Pattern updatedTrustManager = compile("Updated TrustManager");

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  private CertificateMaterial caCert;

  private String[] memberNames;
  private int locatorPort;
  private int locatorHttpPort;
  private Path clusterKeyStore;
  private Path clusterTrustStore;
  private Path clusterSecurityProperties;

  private ClientCache client;
  private Region<String, String> region;
  private Path clientKeyStore;
  private Path clientTrustStore;
  private Path clientLogFile;

  private Path rootFolder;

  /**
   * The test setup creates a cluster with 1 locator and 2 servers, a client cache, and a CA
   * certificate. The cluster has SSL enabled for all components and uses client authentication. The
   * cluster members share a certificate which is signed by a CA and trust the CA certificate. The
   * client has a certificate signed by the same CA and also trusts the CA certificate.
   */
  @Before
  public void setUp() throws IOException, GeneralSecurityException, InterruptedException {
    rootFolder = folderRule.getFolder().toPath().toAbsolutePath();

    caCert = new CertificateBuilder()
        .commonName("ca")
        .isCA()
        .generate();

    startCluster();
    startClient();
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }

    shutdownCluster();
  }

  /**
   * This test rotates the cluster's certificate and verifies that the client can form a new secure
   * connection.
   */
  @Test
  public void rotateClusterCertificate()
      throws GeneralSecurityException, IOException {
    CertificateMaterial newClusterCert = new CertificateBuilder()
        .commonName("cluster")
        .issuedBy(caCert)
        .sanDnsName("localhost")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate();

    writeCertsToKeyStore(clusterKeyStore, newClusterCert);
    waitForMembersToLogMessage(updatedKeyManager);

    assertThatCode(() -> region.put("foo", "bar"))
        .as("The client performs an operation which requires a new secure connection")
        .doesNotThrowAnyException();
  }

  /**
   * This test rotates the client's certificate and verifies that the client can form a new secure
   * connection.
   */
  @Test
  public void rotateClientCertificate()
      throws GeneralSecurityException, IOException {
    CertificateMaterial newClientCert = new CertificateBuilder()
        .commonName("client")
        .issuedBy(caCert)
        .sanDnsName("localhost")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate();

    writeCertsToKeyStore(clientKeyStore, newClientCert);
    waitForClientToLogMessage(updatedKeyManager);

    assertThatCode(() -> region.put("foo", "bar"))
        .as("The client performs an operation which requires a new secure connection")
        .doesNotThrowAnyException();
  }

  /**
   * This test rotates the CA certificate in both the cluster and the client. It verifies that the
   * client can form a new secure connection after the new CA certificate has been added and the old
   * CA certificate removed.
   */
  @Test
  public void rotateCaCertificate()
      throws GeneralSecurityException, IOException {
    /*
     * First, create a new CA certificate and add it to both the cluster's and the client's trust
     * stores. The trust stores will contain both the old and the new CA certificates.
     */

    CertificateMaterial newCaCert = new CertificateBuilder()
        .commonName("ca")
        .isCA()
        .generate();

    writeCertsToTrustStore(clusterTrustStore, caCert, newCaCert);
    writeCertsToTrustStore(clientTrustStore, caCert, newCaCert);

    waitForMembersToLogMessage(updatedTrustManager);
    waitForClientToLogMessage(updatedTrustManager);

    /*
     * Next, create new certificates for the cluster and the client which are signed by the new CA,
     * and replace the certificates in the cluster's and the client's key stores.
     */

    CertificateMaterial newClusterCert = new CertificateBuilder()
        .commonName("cluster")
        .issuedBy(newCaCert)
        .sanDnsName("localhost")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate();

    CertificateMaterial newClientCert = new CertificateBuilder()
        .commonName("client")
        .issuedBy(newCaCert)
        .sanDnsName("localhost")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate();

    writeCertsToKeyStore(clusterKeyStore, newClusterCert);
    writeCertsToKeyStore(clientKeyStore, newClientCert);

    waitForMembersToLogMessage(updatedKeyManager);
    waitForClientToLogMessage(updatedKeyManager);

    /*
     * Finally, remove the old CA certificate from both the cluster's and the client's trust stores.
     */

    writeCertsToTrustStore(clusterTrustStore, newCaCert);
    writeCertsToTrustStore(clientTrustStore, newCaCert);

    for (String name : memberNames) {
      await().untilAsserted(() -> assertThat(logsForMember(name))
          .as("The cluster's trust manager has been updated twice")
          .haveExactly(2, linesMatching(updatedTrustManager)));
    }

    await().untilAsserted(() -> assertThat(logsForClient())
        .as("The client's trust manager has been updated twice")
        .haveExactly(2, linesMatching(updatedTrustManager)));

    assertThatCode(() -> region.put("foo", "bar"))
        .as("The client performs an operation which requires a new secure connection")
        .doesNotThrowAnyException();
  }

  /**
   * This test verifies that rotating to an untrusted certificate causes an exception when the
   * client tries to form a new secure connection. This is a sanity check that certificates are
   * being dynamically updated.
   */
  @Test
  public void untrustedCertificateThrows()
      throws GeneralSecurityException, IOException {
    CertificateMaterial selfSignedCert = new CertificateBuilder()
        .commonName("client")
        .sanDnsName("localhost")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate();

    writeCertsToKeyStore(clientKeyStore, selfSignedCert);
    waitForClientToLogMessage(updatedKeyManager);

    assertThatThrownBy(() -> region.put("foo", "bar"))
        .as("The client performs an operation which requires a new connection")
        .isNotNull();
  }

  private void writeCertsToKeyStore(Path keyStoreFile, CertificateMaterial... certs)
      throws GeneralSecurityException, IOException {
    CertStores store = new CertStores("");
    for (int i = 0; i < certs.length; i++) {
      store.withCertificate(String.valueOf(i), certs[i]);
    }
    store.createKeyStore(keyStoreFile.toAbsolutePath().toString(), dummyStorePass);
  }

  private void writeCertsToTrustStore(Path trustStoreFile, CertificateMaterial... certs)
      throws GeneralSecurityException, IOException {
    CertStores store = new CertStores("");
    for (int i = 0; i < certs.length; i++) {
      store.trust(String.valueOf(i), certs[i]);
    }
    store.createTrustStore(trustStoreFile.toAbsolutePath().toString(), dummyStorePass);
  }

  private void waitForMembersToLogMessage(Pattern pattern) {
    for (String name : memberNames) {
      await().untilAsserted(() -> assertThat(logsForMember(name))
          .as("The logs for member " + name + " include a line matching \"" + pattern + "\"")
          .haveAtLeast(1, linesMatching(pattern)));
    }
  }

  private void waitForClientToLogMessage(Pattern pattern) {
    await().untilAsserted(() -> assertThat(logsForClient())
        .as("The logs for the client include a line matching \"" + pattern + "\"")
        .haveAtLeast(1, linesMatching(pattern)));
  }

  private Condition<String> linesMatching(Pattern pattern) {
    return new Condition<>(pattern.asPredicate(), "lines matching \"" + pattern + "\"");
  }

  private Stream<String> logsForClient() throws IOException {
    return Files.lines(clientLogFile);
  }

  private Stream<String> logsForMember(String name) throws IOException {
    Path logFile = rootFolder.resolve(name).resolve(name + ".log");
    return Files.lines(logFile);
  }

  private void startClient() throws IOException, GeneralSecurityException, InterruptedException {
    CertificateMaterial clientCert = new CertificateBuilder()
        .commonName("client")
        .issuedBy(caCert)
        .sanDnsName("localhost")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate();

    clientKeyStore = createFile(rootFolder.resolve("client-keystore.jks"));
    writeCertsToKeyStore(clientKeyStore, clientCert);

    clientTrustStore = createFile(rootFolder.resolve("client-truststore.jks"));
    writeCertsToTrustStore(clientTrustStore, caCert);

    Path clientSecurityProperties = createFile(rootFolder.resolve("client-security.properties"));
    Properties properties = CertStores.propertiesWith("all", "any", "any",
        clientTrustStore, dummyStorePass, clientKeyStore, dummyStorePass, true, true);
    properties.store(new FileOutputStream(clientSecurityProperties.toFile()), "");

    clientLogFile = createFile(rootFolder.resolve("client.log"));

    client = new ClientCacheFactory(properties)
        .addPoolLocator("localhost", locatorPort)
        .set("log-file", clientLogFile.toString())
        // prevent the client from creating a connection until the first cache operation
        .setPoolMinConnections(0)
        .create();

    region = client.<String, String>createClientRegionFactory(PROXY)
        .create(regionName);

    waitForClientToLogMessage(compile(quote("Started watching " + clientKeyStore)));
    waitForClientToLogMessage(compile(quote("Started watching " + clientTrustStore)));

    /*
     * This sleep is needed to ensure that any updates to the key or trust store file are detected
     * by the client. Without it, the timestamp on the updated file might be the same as the
     * timestamp before the update, preventing the client from noticing the change.
     */
    Thread.sleep(Duration.ofSeconds(5).toMillis());
  }

  private void startCluster() throws IOException, GeneralSecurityException {
    CertificateMaterial clusterCert = new CertificateBuilder()
        .commonName("cluster")
        .issuedBy(caCert)
        .sanDnsName("localhost")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate();

    clusterKeyStore = createFile(rootFolder.resolve("cluster-keystore.jks"));
    writeCertsToKeyStore(clusterKeyStore, clusterCert);

    clusterTrustStore = createFile(rootFolder.resolve("cluster-truststore.jks"));
    writeCertsToTrustStore(clusterTrustStore, caCert);

    clusterSecurityProperties = createFile(rootFolder.resolve("cluster-security.properties"));
    Properties properties = CertStores.propertiesWith("all", "any", "any",
        clusterTrustStore, dummyStorePass, clusterKeyStore, dummyStorePass, true, true);
    properties.store(new FileOutputStream(clusterSecurityProperties.toFile()), "");

    memberNames = new String[] {"locator", "server1", "server2"};

    startLocator(memberNames[0]);
    startServer(memberNames[1]);
    startServer(memberNames[2]);
    createRegion();
  }

  private void startLocator(String name) throws IOException {
    Path dir = createDirectories(rootFolder.resolve(name));

    int[] availablePorts = getRandomAvailableTCPPorts(3);
    locatorPort = availablePorts[0];
    locatorHttpPort = availablePorts[1];
    int locatorJmxPort = availablePorts[2];

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--connect=false",
        "--name=" + name,
        "--dir=" + dir,
        "--bind-address=127.0.0.1",
        "--port=" + locatorPort,
        "--http-service-port=" + locatorHttpPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort,
        "--security-properties-file=" + clusterSecurityProperties);

    gfshRule.execute(startLocatorCommand);
  }

  private void startServer(String name) throws IOException {
    Path dir = createDirectories(rootFolder.resolve(name));

    int[] availablePorts = getRandomAvailableTCPPorts(1);
    int port = availablePorts[0];

    String locatorString = "localhost[" + locatorPort + "]";

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + name,
        "--dir=" + dir,
        "--bind-address=127.0.0.1",
        "--server-port=" + port,
        "--locators=" + locatorString,
        "--security-properties-file=" + clusterSecurityProperties);

    gfshRule.execute(startServerCommand);
  }

  private void createRegion() {
    String connectToLocatorCommand = String.join(" ",
        "connect",
        "--use-http",
        "--use-ssl",
        "--url=https://localhost:" + locatorHttpPort + "/geode-mgmt/v1",
        "--security-properties-file=" + clusterSecurityProperties);

    String createRegionCommand = String.join(" ",
        "create region",
        "--name=" + regionName,
        "--type=REPLICATE");

    gfshRule.execute(connectToLocatorCommand, createRegionCommand);
  }

  private void shutdownCluster() {
    String connectToLocatorCommand = String.join(" ",
        "connect",
        "--use-http",
        "--use-ssl",
        "--url=https://localhost:" + locatorHttpPort + "/geode-mgmt/v1",
        "--security-properties-file=" + clusterSecurityProperties);

    String shutdownCommand = "shutdown --include-locators=true";
    gfshRule.execute(connectToLocatorCommand, shutdownCommand);
  }
}
