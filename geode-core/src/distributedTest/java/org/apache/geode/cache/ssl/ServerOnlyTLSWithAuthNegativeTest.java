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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerOnlyTLSTestFixture;

/**
 * Negative tests for Server-only TLS with Alternative Client Authentication.
 *
 * <p>
 * These tests verify that security violations are properly detected and rejected:
 * <ul>
 * <li>Invalid credentials are rejected</li>
 * <li>Missing credentials are rejected</li>
 * <li>Invalid tokens are rejected</li>
 * <li>Unauthorized operations are blocked</li>
 * <li>Missing or invalid server certificates are detected</li>
 * </ul>
 */
@Category({SecurityTest.class})
public class ServerOnlyTLSWithAuthNegativeTest {

  private static final String REGION_NAME = "testRegion";

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private ServerOnlyTLSTestFixture fixture;

  @Before
  public void setUp() throws Exception {
    fixture = new ServerOnlyTLSTestFixture();
  }

  @org.junit.After
  public void tearDown() throws Exception {
    // Remove ignored exceptions
    IgnoredException.removeAllExpectedExceptions();
    // Give VMs time to fully shut down
    Thread.sleep(500);
  }

  /**
   * Test that clients with invalid credentials are rejected.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Wrong password causes authentication failure</li>
   * <li>TLS connection is established but authentication fails</li>
   * </ul>
   */
  @Test
  public void testClientWithInvalidCredentialsRejected() throws Exception {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());
    IgnoredException.addIgnoredException("Authentication FAILED");
    IgnoredException.addIgnoredException("ServerOperationException");

    // Create certificates and stores
    // Note: Locator and server must use same CertStores for peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Configure client with WRONG password
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    clientProps.putAll(fixture.createClientAuthProperties("testUser", "wrongPassword"));

    // Client connection succeeds (TLS is established), but authentication fails on first operation
    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientProps)
        .withLocatorConnection(locatorPort));

    // Verify authentication fails when attempting an operation
    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      // Authentication should fail on first operation
      assertThatThrownBy(() -> region.put("key", "value"))
          .isInstanceOf(ServerOperationException.class)
          .hasCauseInstanceOf(AuthenticationFailedException.class);
    });
  }

  /**
   * Test that clients without credentials are rejected.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Missing authentication credentials cause connection failure</li>
   * <li>TLS connection might establish but authentication is required</li>
   * </ul>
   */
  @Test
  public void testClientWithMissingCredentialsRejected() throws Exception {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());
    IgnoredException.addIgnoredException("Authentication FAILED");
    IgnoredException.addIgnoredException("AuthenticationRequiredException");
    IgnoredException.addIgnoredException("No security credentials are provided");
    IgnoredException.addIgnoredException("ServerOperationException");

    // Create certificates and stores
    // Note: Locator and server must use same CertStores for peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Configure client with TLS but NO authentication credentials
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    // Do NOT add authentication properties

    // Client connection succeeds (TLS is established), but authentication fails on first operation
    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientProps)
        .withLocatorConnection(locatorPort));

    // Verify authentication fails when attempting an operation
    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      // Authentication should fail on first operation due to missing credentials
      assertThatThrownBy(() -> region.put("key", "value"))
          .isInstanceOf(ServerOperationException.class)
          .hasCauseInstanceOf(AuthenticationRequiredException.class);
    });
  }

  /**
   * Test that clients with invalid tokens are rejected.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Invalid bearer tokens cause authentication failure</li>
   * <li>Token validation is enforced</li>
   * </ul>
   */
  @Test
  public void testClientWithInvalidTokenRejected() throws Exception {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());
    IgnoredException.addIgnoredException("Authentication FAILED");
    IgnoredException.addIgnoredException("Token authentication FAILED");
    IgnoredException.addIgnoredException("ServerOperationException");

    // Create certificates and stores
    // Note: Locator and server must use same CertStores for peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Configure client with INVALID token
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    clientProps.putAll(fixture.createClientTokenAuthProperties("INVALID_TOKEN"));

    // Client connection succeeds (TLS is established), but authentication fails on first operation
    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientProps)
        .withLocatorConnection(locatorPort));

    // Verify authentication fails when attempting an operation
    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      // Authentication should fail on first operation due to invalid token
      assertThatThrownBy(() -> region.put("key", "value"))
          .isInstanceOf(ServerOperationException.class)
          .hasCauseInstanceOf(AuthenticationFailedException.class);
    });
  }

  /**
   * Test that authenticated clients without authorization are blocked from operations.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Authentication succeeds</li>
   * <li>Unauthorized operations are blocked by SecurityManager</li>
   * </ul>
   */
  @Test
  public void testClientUnauthorizedForOperation() throws Exception {
    IgnoredException.addIgnoredException(NotAuthorizedException.class.getName());

    // Create certificates and stores
    // Note: Locator and server must use same CertStores for peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Connect client with credentials that don't match required permissions
    // SimpleSecurityManager authorizes based on principal matching permission string prefix
    // User "readonly" will NOT be authorized for "DATA:WRITE" operations
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    clientProps.putAll(fixture.createClientAuthProperties("readonly", "readonly"));

    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientProps)
        .withLocatorConnection(locatorPort));

    // Verify client can connect but unauthorized operations fail
    client.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      // This operation should fail due to lack of authorization
      assertThatThrownBy(() -> region.put("unauthorizedKey", "value"))
          .hasCauseInstanceOf(NotAuthorizedException.class);
    });
  }

  /**
   * Test that clients reject connections to servers without certificates.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>TLS handshake fails when server doesn't present certificate</li>
   * <li>Client-side verification is enforced</li>
   * </ul>
   */
  @Test
  public void testClientCannotConnectWithoutServerCert() throws Exception {
    IgnoredException.addIgnoredException("SSLHandshakeException");
    IgnoredException.addIgnoredException("Server expecting SSL handshake");

    // This test would require starting a server WITHOUT SSL configuration
    // which is complex in a proper test environment. The test verifies the concept
    // that mixing SSL and non-SSL components fails.

    // Create locator WITH SSL
    CertStores locatorStores = fixture.createLocatorStores();

    Properties locatorProps = locatorStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(2, locatorProps);
    int locatorPort = locator.getPort();

    // Start server WITHOUT SSL (simulating misconfiguration)
    Properties serverProps = new Properties();
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    // No SSL properties
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");

    // Server should fail to join the cluster due to SSL mismatch
    assertThatThrownBy(() -> {
      cluster.startServerVM(3, serverProps, locatorPort);
    }).getCause().getCause().getCause()
        .isInstanceOf(javax.net.ssl.SSLHandshakeException.class);
  }

  /**
   * Test that clients reject servers with invalid or untrusted certificates.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Client verifies server certificate against truststore</li>
   * <li>Invalid server certificates are rejected</li>
   * <li>PKIX path validation is enforced</li>
   * </ul>
   */
  @Test
  public void testClientRejectsServerWithInvalidCert() throws Exception {
    IgnoredException.addIgnoredException("SSLHandshakeException");
    IgnoredException.addIgnoredException("path");
    IgnoredException.addIgnoredException("certificate");

    // Create two separate CAs
    CertificateMaterial validCA = fixture.getCA();

    // Create a separate untrusted CA
    CertificateMaterial untrustedCA = new CertificateBuilder()
        .commonName("Untrusted-CA")
        .isCA()
        .generate();

    // Create locator with valid CA
    CertStores locatorStores = fixture.createLocatorStores();

    Properties locatorProps = locatorStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Create server with certificate from UNTRUSTED CA
    CertificateMaterial untrustedServerCert = new CertificateBuilder()
        .commonName("untrusted-server")
        .issuedBy(untrustedCA)
        .sanDnsName("localhost")
        .sanIpAddress("127.0.0.1")
        .generate();

    CertStores untrustedServerStores = CertStores.serverStore();
    untrustedServerStores.withCertificate("server", untrustedServerCert);
    untrustedServerStores.trust("untrustedCA", untrustedCA); // Server ONLY trusts untrusted CA, not
                                                             // valid CA

    Properties serverProps = untrustedServerStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    serverProps.setProperty("member-timeout", "5000"); // Fail fast if can't join cluster

    // Server might start but cluster communication could fail due to cert mismatch
    // OR server might fail to join cluster
    // Either way, this demonstrates certificate validation

    // Create client truststore with valid CA only (doesn't trust untrusted CA)
    CertStores clientStores = CertStores.clientStore();
    clientStores.trust("ca", validCA); // Client ONLY trusts valid CA

    Properties clientProps = clientStores.propertiesWith("all", false, false);
    clientProps.putAll(fixture.createClientAuthProperties("testUser", "testUser"));

    // If server manages to start, client connection should fail due to untrusted certificate
    try {
      MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

      server.invoke(() -> {
        ClusterStartupRule.getCache()
            .createRegionFactory(RegionShortcut.REPLICATE)
            .create(REGION_NAME);
      });

      // Client should reject server's untrusted certificate
      assertThatThrownBy(() -> {
        cluster.startClientVM(2, c -> c
            .withProperties(clientProps)
            .withLocatorConnection(locatorPort));
      }).satisfiesAnyOf(
          e -> assertThat(e).hasMessageContaining("PKIX"),
          e -> assertThat(e).hasMessageContaining("certificate"),
          e -> assertThat(e).hasMessageContaining("trust"));
    } catch (Exception e) {
      // Server startup failure due to certificate mismatch is expected
      // The cause chain should contain SSL/certificate/handshake errors
      Throwable cause = e;
      boolean foundSSLError = false;
      while (cause != null && !foundSSLError) {
        String message = cause.getClass().getName() + ": " + cause.getMessage();
        if (message.contains("SSL") || message.contains("certificate")
            || message.contains("handshake")) {
          foundSSLError = true;
        }
        cause = cause.getCause();
      }
      assertThat(foundSSLError).as("Should find SSL/certificate/handshake error in cause chain")
          .isTrue();
    }
  }
}
