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
package org.apache.geode.internal.cache.wan.misc;

import static java.lang.String.join;
import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.stream;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import util.TestException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class WANSSLDistributedTest implements Serializable {
  private static final long serialVersionUID = -4552428155768617989L;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTemporaryFolder tempFolder = new SerializableTemporaryFolder();

  // Cluster
  private VM locatorSite1;
  private VM locatorSite2;
  private VM server1;
  private VM server2;
  private static final String LOCATOR_1_NAME = "locator1";
  private static final String LOCATOR_2_NAME = "locator2";
  private int[] ports;
  private String hostName;

  // Region
  private static final String REGION_NAME = "regionName";
  private static final String KEY_PREFIX = "key";
  private static final String VALUE_PREFIX = "value";
  private static final int ENTRIES_PER_SERVER = 100;

  // WAN
  private static final int SITE_1_DSID = 1;
  private static final int SITE_2_DSID = 2;
  private static final String SITE_1_SENDER_NAME = "site1Sender";
  private static final String SITE_2_SENDER_NAME = "site2Sender";

  // SSL
  private static final String TLS13 = "TLSv1.3";
  private static final String TLS12 = "TLSv1.2";
  private File keyStoreFile;
  private File trustStoreFile;
  private static final String STORE_PASSWORD = "password";
  private static final String SIGNING_ALGORITHM = "SHA256withRSA";
  private static final String[] TLS12_CIPHERS = getAllowedCiphersTLS12();
  private static final String[] TLS13_CIPHERS = getAllowedCiphersTLS13();

  @Before
  public void setUp() throws Exception {
    // Ignore this exception so that if tests fail, they fail with assertion errors rather than
    // suspect strings
    addIgnoredException(SSLHandshakeException.class);

    ports = getRandomAvailableTCPPorts(2);
    hostName = InetAddress.getLocalHost().getCanonicalHostName();

    keyStoreFile = tempFolder.newFile();
    trustStoreFile = tempFolder.newFile();

    locatorSite1 = VM.getVM(0);
    locatorSite2 = VM.getVM(1);
    server1 = VM.getVM(2);
    server2 = VM.getVM(3);
  }

  @After
  public void tearDown() {
    IgnoredException.removeAllExpectedExceptions();
  }

  /*
   * Sets up bidirectional WAN with a replicated region, serial gateway sender and gateway receiver
   * on each server using TLSv1.2 and various ciphers for gateway communications and confirms
   * that data is able to flow from each site to the other and that the expected protocol and
   * ciphers are being used
   */
  @Test
  @Parameters(method = "getAllowedCiphersTLS12")
  @TestCaseName("{method} cipher:{0}")
  public void biDirectionalWANSSLWithValidTLS12Ciphers_usesExpectedCipher(final String cipher)
      throws GeneralSecurityException, IOException {
    runValidProtocolsAndCiphersTest(TLS12, cipher, TLS12, cipher, TLS12, cipher);
  }

  /*
   * Sets up bidirectional WAN with a replicated region, serial gateway sender and gateway receiver
   * on each server using TLSv1.3 and various ciphers for gateway communications and confirms
   * that data is able to flow from each site to the other and that the expected protocol and
   * ciphers are being used
   */
  @Test
  @Parameters(method = "getAllowedCiphersTLS13")
  @TestCaseName("{method} cipher:{0}")
  public void biDirectionalWANSSLWithValidTLS13Ciphers_usesExpectedCipher(final String cipher)
      throws GeneralSecurityException, IOException {
    runValidProtocolsAndCiphersTest(TLS13, cipher, TLS13, cipher, TLS13, cipher);
  }

  /*
   * Sets up bidirectional WAN with a replicated region, serial gateway sender and gateway receiver
   * on each server using various SSL protocols and ciphers for gateway communications and confirms
   * that data is able to flow from each site to the other and that the expected protocol and
   * ciphers are being used
   */
  @Test
  @Parameters(method = "getOtherValidParams")
  @TestCaseName("{method} server1 protocols:{0}, server1 ciphers:{1}, server2 protocols:{2}, server2 ciphers:{3}")
  public void biDirectionalWANSSLWithValidProtocolsAndCiphers_usesExpectedProtocolAndCipher(
      final String server1Protocols, final String server1Ciphers, final String server2Protocols,
      final String server2Ciphers, final String expectedNegotiatedProtocol,
      final String expectedNegotiatedCipher) throws GeneralSecurityException, IOException {
    runValidProtocolsAndCiphersTest(server1Protocols, server1Ciphers, server2Protocols,
        server2Ciphers, expectedNegotiatedProtocol, expectedNegotiatedCipher);
  }

  private void runValidProtocolsAndCiphersTest(String server1Protocols, String server1Ciphers,
      String server2Protocols,
      String server2Ciphers, String expectedNegotiatedProtocol,
      String expectedNegotiatedCipher)
      throws IOException, GeneralSecurityException {
    generateKeyAndTrustStore(hostName, keyStoreFile, trustStoreFile);

    startLocatorsAndServers(server1Protocols, server1Ciphers, server2Protocols, server2Ciphers);

    setUpWAN(server1, SITE_1_SENDER_NAME, SITE_2_DSID);

    setUpWAN(server2, SITE_2_SENDER_NAME, SITE_1_DSID);

    server1.invoke("check SSL parameters for gateway sender socket",
        () -> checkProtocolAndCipher(SITE_1_SENDER_NAME, expectedNegotiatedProtocol,
            expectedNegotiatedCipher));

    server2.invoke("check SSL parameters for gateway sender socket",
        () -> checkProtocolAndCipher(SITE_2_SENDER_NAME, expectedNegotiatedProtocol,
            expectedNegotiatedCipher));

    server1.invoke("do puts in server1", () -> createData(0));
    server2.invoke("do puts in server2", () -> createData(ENTRIES_PER_SERVER));

    server1.invoke("validate data in server1", this::validateData);
    server2.invoke("validate data in server2", this::validateData);
  }

  /*
   * Sets up bidirectional WAN with a replicated region, serial gateway sender and gateway receiver
   * on each server using TLSv1.2 with unsupported cipher suites and confirms that servers cannot
   * communicate via WAN
   */
  @Test
  @Parameters(method = "getUnsupportedCiphersTLS12")
  @TestCaseName("{method} cipher:{0}")
  public void biDirectionalWANSSLWithInvalidTLS12Ciphers_failsToMakeConnection(final String cipher)
      throws GeneralSecurityException, IOException {
    runInvalidProtocolsAndCipherCombinationsTest(TLS12, cipher, TLS12, cipher);
  }

  /*
   * Sets up bidirectional WAN with a replicated region, serial gateway sender and gateway receiver
   * on each server using TLSv1.3 with unsupported cipher suites and confirms that servers cannot
   * communicate via WAN
   */
  @Test
  @Parameters(method = "getUnsupportedCiphersTLS13")
  @TestCaseName("{method} cipher:{0}")
  public void biDirectionalWANSSLWithInvalidTLS13Ciphers_failsToMakeConnection(final String cipher)
      throws GeneralSecurityException, IOException {
    runInvalidProtocolsAndCipherCombinationsTest(TLS13, cipher, TLS13, cipher);
  }

  /*
   * Sets up bidirectional WAN with a replicated region, serial gateway sender and gateway receiver
   * on each server using SSL protocol and cipher combinations such that servers cannot communicate
   */
  @Test
  @Parameters(method = "getOtherInvalidProtocolAndCipherCombinationParams")
  public void biDirectionalWANSSLWithInvalidProtocolsAndCiphers_failsToMakeConnection(
      final String server1Protocols, final String server1Ciphers, final String server2Protocols,
      final String server2Ciphers) throws GeneralSecurityException, IOException {
    runInvalidProtocolsAndCipherCombinationsTest(server1Protocols, server1Ciphers, server2Protocols,
        server2Ciphers);
  }

  private void runInvalidProtocolsAndCipherCombinationsTest(String server1Protocols,
      String server1Ciphers,
      String server2Protocols, String server2Ciphers) throws IOException, GeneralSecurityException {
    generateKeyAndTrustStore(hostName, keyStoreFile, trustStoreFile);

    startLocatorsAndServers(server1Protocols, server1Ciphers, server2Protocols, server2Ciphers);

    setUpWAN(server1, SITE_1_SENDER_NAME, SITE_2_DSID);

    setUpWAN(server2, SITE_2_SENDER_NAME, SITE_1_DSID);

    server1.invoke("confirm sender cannot create connection",
        () -> verifyConnectionCannotBeMade(SITE_1_SENDER_NAME));
    server2.invoke("confirm sender cannot create connection",
        () -> verifyConnectionCannotBeMade(SITE_2_SENDER_NAME));
  }

  /*
   * Attempts to set up bidirectional WAN with a replicated region, serial gateway sender and
   * gateway receiver on each server using nonexistent SSL protocol or cipher name and confirms that
   */
  @Test
  @Parameters(method = "getNonexistentProtocolAndCipherParams")
  public void biDirectionalWANSSLWithNonexistentProtocolsAndCiphers_failsToCreateReceiverAndSender(
      final String server1Protocols, final String server1Ciphers, final String server2Protocols,
      final String server2Ciphers) throws GeneralSecurityException, IOException {
    generateKeyAndTrustStore(hostName, keyStoreFile, trustStoreFile);

    startLocatorsAndServers(server1Protocols, server1Ciphers, server2Protocols, server2Ciphers);

    server1.invoke("create replicated region", () -> createReplicatedRegionWithSender(
        SITE_1_SENDER_NAME));
    server1.invoke("verify gateway receiver cannot start", this::verifyReceiverCannotBeStarted);
    server1.invoke("verify gateway sender cannot create connection",
        () -> verifySenderCannotCreateConnection(SITE_1_SENDER_NAME, SITE_2_DSID));

    server2.invoke("create replicated region", () -> createReplicatedRegionWithSender(
        SITE_2_SENDER_NAME));
    server2.invoke("create gateway receiver", this::verifyReceiverCannotBeStarted);
    server2.invoke("verify gateway sender cannot create connection",
        () -> verifySenderCannotCreateConnection(SITE_2_SENDER_NAME, SITE_1_DSID));
  }

  private void startLocatorsAndServers(final String server1Protocols, final String server1Ciphers,
      final String server2Protocols,
      final String server2Ciphers) {
    int locator1Port = ports[0];
    locatorSite1.invoke("start locator1 in site 1",
        () -> startLocator(LOCATOR_1_NAME, locator1Port, SITE_1_DSID, -1));

    int locator2Port = ports[1];
    locatorSite2.invoke("start locator2 in site 2",
        () -> startLocator(LOCATOR_2_NAME, locator2Port, SITE_2_DSID, locator1Port));

    server1.invoke("start server1 in site 1",
        () -> startServer(locator1Port, server1Protocols, server1Ciphers));

    server2.invoke("start server2 in site 2",
        () -> startServer(locator2Port, server2Protocols, server2Ciphers));
  }

  private void startLocator(final String locatorName, int locatorPort, int dsID,
      int remoteLocatorPort) throws IOException {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder()
        .setMemberName(locatorName)
        .setPort(locatorPort)
        .set(BIND_ADDRESS, hostName)
        .setWorkingDirectory(tempFolder.newFolder(locatorName).getAbsolutePath())
        .set(DISTRIBUTED_SYSTEM_ID, String.valueOf(dsID))
        .set(MCAST_PORT, "0")
        .set(HTTP_SERVICE_PORT, "0");
    if (remoteLocatorPort != -1) {
      builder.set(REMOTE_LOCATORS, hostName + "[" + remoteLocatorPort + "]");
    }
    LocatorLauncher locatorLauncher = builder.build();
    locatorLauncher.start();
  }

  private void startServer(int locatorPort, final String protocols, final String ciphers) {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(HTTP_SERVICE_PORT, "0");
    serverProperties.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    serverProperties.setProperty(SSL_ENABLED_COMPONENTS, "gateway");
    serverProperties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    serverProperties.setProperty(SSL_KEYSTORE_TYPE, "jks");
    serverProperties.setProperty(SSL_KEYSTORE, keyStoreFile.getAbsolutePath());
    serverProperties.setProperty(SSL_KEYSTORE_PASSWORD, STORE_PASSWORD);
    serverProperties.setProperty(SSL_TRUSTSTORE, trustStoreFile.getAbsolutePath());
    serverProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, STORE_PASSWORD);
    serverProperties.setProperty(SSL_PROTOCOLS, protocols);
    serverProperties.setProperty(SSL_CIPHERS, ciphers);
    cacheRule.createCache(serverProperties);
  }

  private void setUpWAN(VM serverVM, final String senderName, int remoteDSID) {
    serverVM.invoke("create replicated region", () -> createReplicatedRegionWithSender(senderName));
    serverVM.invoke("create gateway receiver", this::createReceiver);
    serverVM.invoke("create gateway sender", () -> createSerialSender(senderName, remoteDSID));
  }

  private void createReplicatedRegionWithSender(final String senderName) {
    Cache cache = cacheRule.getCache();
    cache.<String, String>createRegionFactory(RegionShortcut.REPLICATE)
        .addGatewaySenderId(senderName)
        .create(REGION_NAME);
  }

  private void createReceiver() {
    GatewayReceiver receiver = cacheRule.getCache()
        .createGatewayReceiverFactory()
        .create();
    await().untilAsserted(() -> assertThat(receiver.isRunning()).isTrue());
  }

  private void createSerialSender(final String senderName, int remoteDSID) {
    cacheRule.getCache().createGatewaySenderFactory()
        .setParallel(false)
        .setDispatcherThreads(1)
        .create(senderName, remoteDSID);
  }

  private void checkProtocolAndCipher(final String senderName, final String expectedProtocol,
      final String expectedCipher) {
    PoolImpl pool =
        ((AbstractGatewaySender) cacheRule.getCache().getGatewaySender(senderName)).getProxy();

    Connection connection = pool.acquireConnection();
    SSLSession session = ((SSLSocket) connection.getSocket()).getSession();

    assertThat(session.getProtocol()).isEqualTo(expectedProtocol);
    assertThat(session.getCipherSuite()).isEqualTo(expectedCipher);

    pool.returnConnection(connection);
  }

  private void createData(int startingIndex) {
    Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
    for (int i = startingIndex; i < ENTRIES_PER_SERVER + startingIndex; ++i) {
      region.put(KEY_PREFIX + i, VALUE_PREFIX + i);
    }
  }

  private void validateData() {
    Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
    int totalExpectedEntries = ENTRIES_PER_SERVER * 2;
    await().atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(region.size()).isEqualTo(totalExpectedEntries));
    for (int i = 0; i < ENTRIES_PER_SERVER * 2; ++i) {
      String key = KEY_PREFIX + i;
      String value = VALUE_PREFIX + i;
      assertThat(region.get(key)).isEqualTo(value);
    }
  }

  private void verifyConnectionCannotBeMade(final String senderName) {
    PoolImpl pool =
        ((AbstractGatewaySender) cacheRule.getCache().getGatewaySender(senderName)).getProxy();

    assertThatThrownBy(pool::acquireConnection).isInstanceOf(NoAvailableServersException.class);
  }

  private void verifyReceiverCannotBeStarted() {
    assertThatThrownBy(this::createReceiver).isInstanceOf(IllegalArgumentException.class);
  }

  private void verifySenderCannotCreateConnection(final String senderName, int remoteDSID) {
    createSerialSender(senderName, remoteDSID);
    verifyConnectionCannotBeMade(senderName);
  }

  private static void generateKeyAndTrustStore(final String hostName, final File keyStoreFile,
      final File trustStoreFile)
      throws IOException, GeneralSecurityException {
    final CertificateMaterial ca = new CertificateBuilder(1,
        WANSSLDistributedTest.SIGNING_ALGORITHM)
            .commonName("Test CA")
            .isCA()
            .generate();

    final CertificateMaterial certificate = new CertificateBuilder(1,
        WANSSLDistributedTest.SIGNING_ALGORITHM)
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

  // These allowed ciphers are well-defined and known for the TLSv1.2 protocol and should be updated
  // if the protocol is changed to add or remove support
  private static String[] getAllowedCiphersTLS12() {
    String[] ciphers = {
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA"
    };
    ciphers = removeUnsupportedCiphers(ciphers, TLS12);
    return ciphers;
  }

  // These allowed ciphers are well-defined and known for the TLSv1.3 protocol and should be updated
  // if the protocol is changed to add or remove support
  private static String[] getAllowedCiphersTLS13() {
    String[] ciphers = {
        "TLS_AES_256_GCM_SHA384",
        "TLS_AES_128_GCM_SHA256",
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_CCM_8_SHA256",
        "TLS_AES_128_CCM_SHA256"
    };
    ciphers = removeUnsupportedCiphers(ciphers, TLS13);
    return ciphers;
  }

  private static String[] removeUnsupportedCiphers(final String[] ciphers, final String protocol) {
    Set<String> supportedCiphers = getSupportedCiphersForProtocol(protocol);
    return stream(ciphers)
        .filter(supportedCiphers::contains)
        .toArray(String[]::new);
  }

  private static Set<String> getSupportedCiphersForProtocol(final String protocol) {
    try {
      SSLContext ssl = SSLContext.getInstance(protocol);
      ssl.init(null, null, new java.security.SecureRandom());
      return new HashSet<>(Arrays.asList(ssl.getSocketFactory().getSupportedCipherSuites()));
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new TestException("Could not initialize supported ciphers", e);
    }
  }

  // Parameters are: server1 protocols, server1 ciphers,
  // server2 protocols, server2 ciphers,
  // expected negotiated protocol, expected negotiated cipher suite
  @NotNull
  @SuppressWarnings("unused")
  private String[][] getOtherValidParams() {
    return new String[][] {
        // Basic scenarios
        new String[] {"any", "any", "any", "any", TLS13, TLS13_CIPHERS[0]},
        new String[] {TLS12, "any", TLS12, "any", TLS12, TLS12_CIPHERS[0]},
        new String[] {TLS13, "any", TLS13, "any", TLS13, TLS13_CIPHERS[0]},

        // Protocol specified on one server only
        new String[] {TLS12, "any", "any", "any", TLS12, TLS12_CIPHERS[0]},
        new String[] {TLS13, "any", "any", "any", TLS13, TLS13_CIPHERS[0]},

        // Cipher specified on one server only
        new String[] {TLS12, TLS12_CIPHERS[1], TLS12, "any", TLS12, TLS12_CIPHERS[1]},
        new String[] {TLS13, TLS13_CIPHERS[1], TLS13, "any", TLS13, TLS13_CIPHERS[1]},

        // Cipher specified on one server only with "any" protocol
        new String[] {"any", TLS12_CIPHERS[1], "any", "any", TLS12, TLS12_CIPHERS[1]},
        new String[] {"any", TLS13_CIPHERS[1], "any", "any", TLS13, TLS13_CIPHERS[1]},

        // Multiple protocols specified
        new String[] {TLS13, "any", TLS13 + "," + TLS12, "any", TLS13, TLS13_CIPHERS[0]},
        new String[] {TLS13, "any", TLS12 + "," + TLS13, "any", TLS13, TLS13_CIPHERS[0]},
        new String[] {TLS12, "any", TLS13 + "," + TLS12, "any", TLS12, TLS12_CIPHERS[0]},
        new String[] {TLS12, "any", TLS12 + "," + TLS13, "any", TLS12, TLS12_CIPHERS[0]},
        new String[] {TLS13 + "," + TLS12, "any", TLS12 + "," + TLS13, "any",
            TLS13, TLS13_CIPHERS[0]},
        new String[] {TLS12 + "," + TLS13, "any", TLS13 + "," + TLS12, "any",
            TLS13, TLS13_CIPHERS[0]},

        // Multiple ciphers specified
        new String[] {TLS13, join(",", TLS13_CIPHERS), TLS13, join(",", TLS13_CIPHERS),
            TLS13, TLS13_CIPHERS[0]},
        new String[] {TLS12, join(",", TLS12_CIPHERS), TLS12, join(",", TLS12_CIPHERS),
            TLS12, TLS12_CIPHERS[0]},
        new String[] {TLS13, join(",", TLS13_CIPHERS), TLS13, "any",
            TLS13, TLS13_CIPHERS[0]},
        new String[] {TLS12, join(",", TLS12_CIPHERS), TLS12, "any",
            TLS12, TLS12_CIPHERS[0]},

        // Overlapping ciphers requiring negotiation to the one they have in common
        new String[] {TLS12, join(",", copyOfRange(TLS12_CIPHERS, 1, 3)),
            TLS12, join(",", copyOfRange(TLS12_CIPHERS, 2, 4)),
            TLS12, TLS12_CIPHERS[2]},

        // Multiple protocols with only a TLSv1.3 cipher in common, but TLSv1.2 specified as first
        // protocol
        new String[] {TLS12 + "," + TLS13,
            join(",", new String[] {TLS12_CIPHERS[0], TLS13_CIPHERS[1]}),
            TLS12 + "," + TLS13,
            join(",", new String[] {TLS12_CIPHERS[1], TLS13_CIPHERS[1]}),
            TLS13, TLS13_CIPHERS[1]},
    };
  }

  // Each array of parameters returned from this method uses the same protocol and single invalid
  // cipher on both of the servers
  @SuppressWarnings("unused")
  private String[] getUnsupportedCiphersTLS12() {
    Set<String> invalidCiphers = getSupportedCiphersForProtocol(TLS12);
    Arrays.asList(TLS12_CIPHERS).forEach(invalidCiphers::remove);
    return invalidCiphers.toArray(new String[] {});
  }

  // Each array of parameters returned from this method uses the same protocol and single invalid
  // cipher on both of the servers
  @SuppressWarnings("unused")
  private String[] getUnsupportedCiphersTLS13() {
    Set<String> invalidCiphers = getSupportedCiphersForProtocol(TLS13);
    Arrays.asList(TLS13_CIPHERS).forEach(invalidCiphers::remove);
    return invalidCiphers.toArray(new String[] {});
  }

  @NotNull
  @SuppressWarnings("unused")
  private String[][] getOtherInvalidProtocolAndCipherCombinationParams() {
    return new String[][] {
        // Protocols do not match
        new String[] {TLS12, "any", TLS13, "any"},
        new String[] {TLS13, "any", TLS12, "any"},

        // Ciphers do not match
        new String[] {"any", TLS12_CIPHERS[0], "any", TLS12_CIPHERS[1]},
        new String[] {TLS12, TLS12_CIPHERS[0], TLS12, TLS12_CIPHERS[1]},
        new String[] {"any", TLS13_CIPHERS[0], "any", TLS13_CIPHERS[1]},
        new String[] {TLS13, TLS13_CIPHERS[0], TLS13, TLS13_CIPHERS[1]},

        // Multiple protocols with only a TLSv1.2 cipher in common, but TLSv1.3 specified as first
        // protocol
        new String[] {TLS13 + "," + TLS12,
            join(",", new String[] {TLS13_CIPHERS[0], TLS12_CIPHERS[2]}),
            TLS13 + "," + TLS12,
            join(",", new String[] {TLS13_CIPHERS[1], TLS12_CIPHERS[2]})},
    };
  }

  @NotNull
  @SuppressWarnings("unused")
  private String[][] getNonexistentProtocolAndCipherParams() {
    return new String[][] {
        // Nonexistent protocol
        new String[] {"myReallyCoolSSLProtocol", "any", "myReallyCoolSSLProtocol", "any"},

        // Nonexistent cipher
        new String[] {"any", "myReallyCoolSSLCipher", "any", "myReallyCoolSSLCipher"},
    };
  }
}
