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
package org.apache.geode.test.junit.rules;

import java.util.Properties;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.templates.UserPasswordAuthInit;

/**
 * Test fixture for Server-only TLS with Alternative Client Authentication scenarios.
 *
 * <p>
 * This fixture creates:
 * <ul>
 * <li>A Certificate Authority (CA)</li>
 * <li>Server certificates signed by the CA (for locators and servers)</li>
 * <li>NO client certificates - clients authenticate via username/password or tokens</li>
 * <li>Server/locator keystores with server certificates</li>
 * <li>Client truststores with CA certificate (to verify servers)</li>
 * <li>Security configuration using SimpleSecurityManager</li>
 * </ul>
 *
 * <p>
 * Key characteristics:
 * <ul>
 * <li>ssl-require-authentication=false - servers don't require client certificates</li>
 * <li>All transport is TLS-encrypted in both directions</li>
 * <li>Clients authenticate using application-layer credentials (username/password or tokens)</li>
 * </ul>
 */
public class ServerOnlyTLSTestFixture {

  private static final String CA_CN = "Server-Only-TLS-CA";
  private static final String SERVER_CN = "geode-server";
  private static final String LOCATOR_CN = "geode-locator";

  private final CertificateMaterial ca;
  private final CertificateMaterial serverCertificate;
  private final CertificateMaterial locatorCertificate;

  public ServerOnlyTLSTestFixture() throws Exception {
    // Create CA
    ca = createCA();

    // Create server and locator certificates (no client certificates needed)
    serverCertificate = createServerCertificate(ca);
    locatorCertificate = createLocatorCertificate(ca);
  }

  /**
   * Creates a Certificate Authority for signing server certificates.
   */
  private CertificateMaterial createCA() throws Exception {
    CertificateBuilder caBuilder = new CertificateBuilder()
        .commonName(CA_CN)
        .isCA();

    return caBuilder.generate();
  }

  /**
   * Creates a server certificate signed by the CA.
   * Includes comprehensive Subject Alternative Names for server verification.
   */
  private CertificateMaterial createServerCertificate(CertificateMaterial ca) throws Exception {
    CertificateBuilder serverBuilder = new CertificateBuilder()
        .commonName(SERVER_CN)
        .issuedBy(ca)
        .sanDnsName("localhost")
        .sanDnsName("server.localdomain")
        .sanIpAddress("127.0.0.1")
        .sanIpAddress("0.0.0.0");

    return serverBuilder.generate();
  }

  /**
   * Creates a locator certificate signed by the CA.
   * Includes comprehensive Subject Alternative Names for locator verification.
   */
  private CertificateMaterial createLocatorCertificate(CertificateMaterial ca) throws Exception {
    CertificateBuilder locatorBuilder = new CertificateBuilder()
        .commonName(LOCATOR_CN)
        .issuedBy(ca)
        .sanDnsName("localhost")
        .sanDnsName("locator.localdomain")
        .sanIpAddress("127.0.0.1")
        .sanIpAddress("0.0.0.0");

    return locatorBuilder.generate();
  }

  /**
   * Creates and returns server CertStores for server-only TLS.
   *
   * <p>
   * Server truststore contains the CA certificate to verify peer connections.
   * <p>
   * Server keystore contains the server's certificate for presentation to clients.
   *
   * @return CertStores configured for server with server certificate
   */
  public CertStores createServerStores() {
    CertStores certStores = CertStores.serverStore();
    certStores.withCertificate("server", serverCertificate);
    certStores.trust("ca", ca);
    return certStores;
  }

  /**
   * Creates and returns locator CertStores for server-only TLS.
   *
   * <p>
   * Locator truststore contains the CA certificate to verify peer connections.
   * <p>
   * Locator keystore contains the locator's certificate for presentation to clients and peers.
   *
   * @return CertStores configured for locator with locator certificate
   */
  public CertStores createLocatorStores() {
    CertStores certStores = CertStores.locatorStore();
    certStores.withCertificate("locator", locatorCertificate);
    certStores.trust("ca", ca);
    return certStores;
  }

  /**
   * Creates and returns cluster CertStores for both locator and server.
   *
   * <p>
   * For peer SSL communication, both locator and server need compatible certificates.
   * This method creates CertStores with the server certificate that works for both.
   * The server certificate includes SANs for localhost/127.0.0.1 which work for both roles.
   *
   * @return CertStores configured with server certificate and CA trust
   */
  public CertStores createClusterStores() {
    CertStores certStores = CertStores.serverStore();
    // Use server certificate for both locator and server (SANs cover both roles)
    certStores.withCertificate("server", serverCertificate);
    certStores.trust("ca", ca);
    return certStores;
  }

  /**
   * Creates and returns client CertStores for server-only TLS.
   *
   * <p>
   * Client truststore contains ONLY the CA certificate to verify server certificates.
   * <p>
   * NO keystore is created - clients do not present certificates.
   *
   * @return CertStores configured for client with only truststore (no client certificate)
   */
  public CertStores createClientStores() {
    CertStores certStores = CertStores.clientStore();
    // Client only needs truststore with CA to verify servers
    certStores.trust("ca", ca);
    // NO client keystore - clients don't present certificates
    return certStores;
  }

  /**
   * Adds security manager configuration to properties.
   * Uses SimpleSecurityManager for authentication and authorization.
   */
  public Properties addSecurityManagerConfig(Properties props) {
    props.setProperty("security-manager", SimpleSecurityManager.class.getName());
    return props;
  }

  /**
   * Adds peer authentication credentials for server/locator-to-locator connections.
   * Required when security-manager is enabled.
   *
   * @param props the properties to add authentication to
   * @param username the username for peer authentication
   * @param password the password for peer authentication
   * @return the properties with peer authentication configured
   */
  public Properties addPeerAuthProperties(Properties props, String username, String password) {
    props.setProperty("security-username", username);
    props.setProperty("security-password", password);
    return props;
  }

  /**
   * Creates client authentication properties with username and password.
   *
   * @param username the username
   * @param password the password
   * @return Properties with authentication configuration
   */
  public Properties createClientAuthProperties(String username, String password) {
    Properties props = new Properties();
    props.setProperty("security-client-auth-init",
        UserPasswordAuthInit.class.getName() + ".create");
    props.setProperty(UserPasswordAuthInit.USER_NAME, username);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    return props;
  }

  /**
   * Creates client authentication properties with a bearer token.
   *
   * @param token the bearer token
   * @return Properties with token authentication configuration
   */
  public Properties createClientTokenAuthProperties(String token) {
    Properties props = new Properties();
    props.setProperty("security-client-auth-init",
        "org.apache.geode.security.templates.TokenAuthInit.create");
    props.setProperty("security-bearer-token", token);
    return props;
  }

  /**
   * Gets the CA certificate material for testing purposes.
   */
  public CertificateMaterial getCA() {
    return ca;
  }

  /**
   * Gets the server certificate material for testing purposes.
   */
  public CertificateMaterial getServerCertificate() {
    return serverCertificate;
  }

  /**
   * Gets the locator certificate material for testing purposes.
   */
  public CertificateMaterial getLocatorCertificate() {
    return locatorCertificate;
  }
}
