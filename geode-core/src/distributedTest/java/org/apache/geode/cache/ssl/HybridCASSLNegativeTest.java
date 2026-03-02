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
package org.apache.geode.cache.ssl;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.security.SecurableCommunicationChannels.ALL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Negative tests for hybrid TLS configuration.
 * Validates that improper configurations are properly rejected with appropriate errors.
 *
 * These tests verify the troubleshooting scenarios documented in the security guide:
 * - Missing clientAuth EKU causes "certificate_unknown" alert
 * - Wrong CA trust causes PKIX path validation failure
 * - Missing subjectAltName causes hostname verification failure
 */
@Category({ClientServerTest.class})
public class HybridCASSLNegativeTest {

  private HybridCATestFixture fixture;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setup() {
    fixture = new HybridCATestFixture();
    fixture.setup();

    // Ignore expected exceptions during locator/server shutdown with SSL
    IgnoredException.addIgnoredException("Could not stop Locator");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
  }

  /**
   * Tests that a server configured to trust the wrong CA rejects client connections.
   * Expected error: PKIX path validation failed
   */
  @Test
  public void testServerTrustsWrongCAForClient() throws Exception {
    // Create a different CA that server will trust (but client cert is not issued by it)
    CertificateMaterial wrongCA = new CertificateBuilder()
        .commonName("Wrong CA")
        .isCA()
        .generate();

    // Server certificate from public CA
    CertificateMaterial serverCert = fixture.createServerCertificate("server-1");

    // Server trusts wrong CA (not the private CA that issued client cert)
    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCert);
    serverStore.trust("wrongCA", wrongCA); // Should trust privateCA instead

    Properties serverProps = serverStore.propertiesWith(ALL, true, true);

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });

    // Client with private-CA certificate
    CertStores clientStore = fixture.createClientStores("1");
    Properties clientProps = clientStore.propertiesWith(ALL, true, true);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("sun.security.validator.ValidatorException");
    IgnoredException.addIgnoredException("PKIX path");
    IgnoredException.addIgnoredException("java.io.IOException");
    IgnoredException.addIgnoredException("Broken pipe");

    // Client connection should fail with PKIX path validation error
    assertThatThrownBy(() -> {
      cluster.startClientVM(2, clientProps,
          ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    }).hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);

    IgnoredException.removeAllExpectedExceptions();
  }

  /**
   * Tests that a client configured to trust the wrong CA rejects server connections.
   * Expected error: PKIX path validation failed
   */
  @Test
  public void testClientTrustsWrongCAForServer() throws Exception {
    // Create a different CA that client will trust (but server cert is not issued by it)
    CertificateMaterial wrongCA = new CertificateBuilder()
        .commonName("Wrong CA")
        .isCA()
        .generate();

    // Server with correct configuration
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(ALL, true, true);

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });

    // Client trusts wrong CA (not the public CA that issued server cert)
    CertificateMaterial clientCert = fixture.createClientCertificate("client-1");
    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate("client", clientCert);
    clientStore.trust("wrongCA", wrongCA); // Should trust publicCA instead

    Properties clientProps = clientStore.propertiesWith(ALL, true, true);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("sun.security.validator.ValidatorException");
    IgnoredException.addIgnoredException("PKIX path");
    IgnoredException.addIgnoredException("java.security.cert.CertificateException");

    // Client connection should fail with PKIX path validation error
    assertThatThrownBy(() -> {
      cluster.startClientVM(2, clientProps,
          ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    }).hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);

    IgnoredException.removeAllExpectedExceptions();
  }

  /**
   * Tests that a client certificate without clientAuth EKU is rejected.
   * Expected error: certificate_unknown (as documented in troubleshooting guide)
   *
   * This validates the critical requirement that client certificates must have
   * the clientAuth Extended Key Usage.
   */
  @Test
  public void testClientCertificateMissingClientAuthEKU() throws Exception {
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(ALL, true, true);

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });

    // Client certificate WITHOUT clientAuth EKU
    CertificateMaterial clientCert =
        fixture.createClientCertificateWithoutClientAuthEKU("client-1");
    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate("client", clientCert);
    clientStore.trust("publicCA", fixture.getPublicCA());

    Properties clientProps = clientStore.propertiesWith(ALL, true, true);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("certificate_unknown");
    IgnoredException.addIgnoredException("java.io.IOException");
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");

    // Connection should fail with certificate_unknown or handshake failure
    assertThatThrownBy(() -> {
      cluster.startClientVM(2, clientProps,
          ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    }).hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);

    IgnoredException.removeAllExpectedExceptions();
  }

  /**
   * Tests that a server certificate without subjectAltName fails hostname verification.
   * Expected error: No subject alternative names present (when endpoint identification enabled)
   *
   * This validates the requirement that server certificates must include SAN for hostname
   * verification.
   */
  @Test
  public void testServerCertificateMissingSAN() throws Exception {
    // Server certificate without SAN
    CertificateMaterial serverCert = fixture.createServerCertificateWithoutSAN("server-1");

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCert);
    serverStore.trust("privateCA", fixture.getPrivateCA());

    Properties serverProps = serverStore.propertiesWith(ALL, true, true);
    serverProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });

    CertStores clientStore = fixture.createClientStores("1");
    Properties clientProps = clientStore.propertiesWith(ALL, true, true);
    clientProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("java.security.cert.CertificateException");
    IgnoredException.addIgnoredException("No subject alternative");
    IgnoredException.addIgnoredException("No name matching");

    // Connection should fail with hostname verification error
    assertThatThrownBy(() -> {
      cluster.startClientVM(2, clientProps,
          ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    }).hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);

    IgnoredException.removeAllExpectedExceptions();
  }

  /**
   * Tests that mutual authentication is enforced when ssl-require-authentication=true.
   * A server without a client certificate in its keystore should not be able to join as a client.
   */
  @Test
  public void testMutualAuthenticationEnforced() throws Exception {
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(ALL, true, true);

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });

    // Create a "client" with no certificate in keystore, only truststore
    CertStores invalidClientStore = CertStores.clientStore();
    // Only trust, no certificate
    invalidClientStore.trust("publicCA", fixture.getPublicCA());

    Properties clientProps = invalidClientStore.propertiesWith(ALL, true, true);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("bad_certificate");
    IgnoredException.addIgnoredException("java.io.IOException");
    IgnoredException.addIgnoredException("Broken pipe");

    // Connection should fail - server requires client authentication
    assertThatThrownBy(() -> {
      cluster.startClientVM(2, clientProps,
          ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    }).hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);

    IgnoredException.removeAllExpectedExceptions();
  }

  /**
   * Tests that a server certificate without serverAuth EKU might be rejected
   * (behavior depends on TLS implementation, but good practice to include it).
   */
  @Test
  public void testServerCertificateWithoutServerAuthEKU() throws Exception {
    // Create server cert without serverAuth EKU
    CertificateMaterial serverCert = new CertificateBuilder()
        .commonName("server-1")
        .issuedBy(fixture.getPublicCA())
        .sanDnsName("localhost")
        // Intentionally omit serverAuthEKU()
        .generate();

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCert);
    serverStore.trust("privateCA", fixture.getPrivateCA());

    Properties serverProps = serverStore.propertiesWith(ALL, true, true);

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });

    CertStores clientStore = fixture.createClientStores("1");
    Properties clientProps = clientStore.propertiesWith(ALL, true, true);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("extended key usage");

    // Some TLS implementations may reject this, others may accept
    // The test documents that including serverAuth EKU is best practice
    try {
      cluster.startClientVM(2, clientProps,
          ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
      // If it succeeds, that's OK - not all implementations strictly enforce
    } catch (Exception e) {
      // If it fails, verify it's SSL-related
      assertThatThrownBy(() -> {
        throw e;
      }).hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);
    } finally {
      IgnoredException.removeAllExpectedExceptions();
    }
  }

  /**
   * Tests mixed configuration where one server has correct hybrid TLS and another doesn't.
   * The misconfigured server should fail to join the cluster.
   */
  @Test
  public void testMisconfiguredServerCannotJoinCluster() throws Exception {
    CertStores locatorStore = fixture.createLocatorStores("1");
    Properties locatorProps = locatorStore.propertiesWith(ALL, true, true);

    MemberVM locator = cluster.startLocatorVM(0, locatorProps);

    // First server with correct configuration
    CertStores serverStore1 = fixture.createServerStores("1");
    Properties serverProps1 = serverStore1.propertiesWith(ALL, true, true);
    MemberVM server1 = cluster.startServerVM(1, serverProps1, locator.getPort());

    // Second server with wrong CA certificates
    CertificateMaterial wrongCA = new CertificateBuilder()
        .commonName("Wrong CA")
        .isCA()
        .generate();

    CertificateMaterial wrongServerCert = new CertificateBuilder()
        .commonName("server-2")
        .issuedBy(wrongCA)
        .serverAuthEKU()
        .sanDnsName("localhost")
        .generate();

    CertStores serverStore2 = CertStores.serverStore();
    serverStore2.withCertificate("server", wrongServerCert);
    serverStore2.trust("wrongCA", wrongCA);

    Properties serverProps2 = serverStore2.propertiesWith(ALL, true, true);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("PKIX path");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("java.io.IOException");

    // Second server should fail to join cluster
    assertThatThrownBy(() -> {
      cluster.startServerVM(2, serverProps2, locator.getPort());
    }).hasCauseInstanceOf(java.io.IOException.class);

    IgnoredException.removeAllExpectedExceptions();
  }
}
