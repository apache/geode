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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Test fixture for creating hybrid TLS certificate configurations where servers use
 * public-CA-issued certificates and clients use private-CA-issued certificates.
 */
public class HybridCATestFixture {
  private CertificateMaterial publicCA;
  private CertificateMaterial privateCA;

  /**
   * Initialize the fixture by creating both public and private CAs.
   */
  public void setup() {
    // Create public CA (simulates certificates from Let's Encrypt, DigiCert, etc.)
    publicCA = new CertificateBuilder()
        .commonName("Public CA Root")
        .isCA()
        .generate();

    // Create private/enterprise CA (for client certificates)
    privateCA = new CertificateBuilder()
        .commonName("Enterprise Internal CA")
        .isCA()
        .generate();
  }

  /**
   * Get the public CA certificate material.
   */
  public CertificateMaterial getPublicCA() {
    return publicCA;
  }

  /**
   * Get the private CA certificate material.
   */
  public CertificateMaterial getPrivateCA() {
    return privateCA;
  }

  /**
   * Create a server certificate issued by the public CA.
   * The certificate includes:
   * - serverAuth Extended Key Usage
   * - subjectAltName with DNS names and IP addresses for hostname verification
   *
   * @param hostname the server's hostname/common name
   * @return certificate material for the server
   */
  public CertificateMaterial createServerCertificate(String hostname) {
    try {
      return new CertificateBuilder()
          .commonName(hostname)
          .issuedBy(publicCA)
          .serverAuthEKU()
          .sanDnsName(hostname)
          .sanDnsName("localhost")
          .sanDnsName(InetAddress.getLocalHost().getHostName())
          .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
          .sanIpAddress(InetAddress.getLocalHost())
          .sanIpAddress(InetAddress.getLoopbackAddress())
          .sanIpAddress("0.0.0.0") // for Windows compatibility
          .generate();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to determine localhost information", e);
    }
  }

  /**
   * Create a server certificate with minimal SAN (for negative testing).
   *
   * @param hostname the server's hostname/common name
   * @return certificate material for the server
   */
  public CertificateMaterial createServerCertificateMinimalSAN(String hostname) {
    return new CertificateBuilder()
        .commonName(hostname)
        .issuedBy(publicCA)
        .serverAuthEKU()
        .sanDnsName(hostname)
        .generate();
  }

  /**
   * Create a server certificate without any SAN (for negative testing).
   *
   * @param hostname the server's hostname/common name
   * @return certificate material for the server
   */
  public CertificateMaterial createServerCertificateWithoutSAN(String hostname) {
    return new CertificateBuilder()
        .commonName(hostname)
        .issuedBy(publicCA)
        .serverAuthEKU()
        .generate();
  }

  /**
   * Create a client certificate issued by the private CA.
   * The certificate includes clientAuth Extended Key Usage which is required for mTLS.
   *
   * @param clientName the client's common name (e.g., "client1@example.com")
   * @return certificate material for the client
   */
  public CertificateMaterial createClientCertificate(String clientName) {
    return new CertificateBuilder()
        .commonName(clientName)
        .issuedBy(privateCA)
        .clientAuthEKU()
        .generate();
  }

  /**
   * Create a client certificate WITHOUT clientAuth EKU (for negative testing).
   * This certificate will be rejected by servers requiring client authentication.
   *
   * @param clientName the client's common name
   * @return certificate material for the client (invalid for mTLS)
   */
  public CertificateMaterial createClientCertificateWithoutClientAuthEKU(String clientName) {
    return new CertificateBuilder()
        .commonName(clientName)
        .issuedBy(privateCA)
        // Intentionally omit .clientAuthEKU()
        .generate();
  }

  /**
   * Create server CertStores configured for hybrid TLS.
   * - Keystore contains server certificate issued by public CA
   * - Truststore contains BOTH CAs:
   * * Public CA: to validate other servers/locators (peer-to-peer)
   * * Private CA: to validate client certificates
   *
   * @param serverId identifier for this server's certificates
   * @return configured CertStores for server
   */
  public CertStores createServerStores(String serverId) {
    CertificateMaterial serverCert = createServerCertificate("server-" + serverId);

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCert);
    serverStore.trust("publicCA", publicCA); // Trust public CA for peer-to-peer
    serverStore.trust("privateCA", privateCA); // Trust private CA to validate clients

    return serverStore;
  }

  /**
   * Create client CertStores configured for hybrid TLS.
   * - Keystore contains client certificate issued by private CA
   * - Truststore contains public CA to validate server certificates
   *
   * @param clientId identifier for this client's certificates
   * @return configured CertStores for client
   */
  public CertStores createClientStores(String clientId) {
    CertificateMaterial clientCert = createClientCertificate("client-" + clientId);

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate("client", clientCert);
    clientStore.trust("publicCA", publicCA); // Trust public CA to validate servers

    return clientStore;
  }

  /**
   * Create locator CertStores configured for hybrid TLS.
   * Locators use the same configuration as servers:
   * - Keystore contains locator certificate issued by public CA
   * - Truststore contains BOTH CAs:
   * * Public CA: to validate other locators/servers (peer-to-peer)
   * * Private CA: to validate client certificates
   *
   * @param locatorId identifier for this locator's certificates
   * @return configured CertStores for locator
   */
  public CertStores createLocatorStores(String locatorId) {
    CertificateMaterial locatorCert = createServerCertificate("locator-" + locatorId);

    CertStores locatorStore = CertStores.locatorStore();
    locatorStore.withCertificate("locator", locatorCert);
    locatorStore.trust("publicCA", publicCA); // Trust public CA for peer-to-peer
    locatorStore.trust("privateCA", privateCA); // Trust private CA to validate clients

    return locatorStore;
  }
}
