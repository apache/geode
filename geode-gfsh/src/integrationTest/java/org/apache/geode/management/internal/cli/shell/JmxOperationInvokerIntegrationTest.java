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

package org.apache.geode.management.internal.cli.shell;

import static java.lang.String.valueOf;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.AvailablePortHelper;

public class JmxOperationInvokerIntegrationTest {

  private static final String CERTIFICATE_ALGORITHM = "SHA256withRSA";
  private static final int CERTIFICATE_EXPIRATION_IN_DAYS = 1;
  private static final String STORE_PASSWORD = "geode";
  private static final String STORE_TYPE = "jks";

  final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
  final int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
  final int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
  final Properties properties = new Properties();

  @TempDir
  public Path tempPath;

  private Path keyStoreFile;
  private Path trustStoreFile;

  public JmxOperationInvokerIntegrationTest() throws UnknownHostException {
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(HTTP_SERVICE_PORT, "0");
    properties.setProperty(LOG_FILE, "");
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(JMX_MANAGER_PORT, valueOf(jmxPort));
    properties.setProperty(JMX_MANAGER_START, "true");
  }

  @BeforeEach
  public void beforeEach() throws GeneralSecurityException, IOException {
    keyStoreFile = tempPath.resolve("keystore.jks").toAbsolutePath();
    trustStoreFile = tempPath.resolve("truststore.jks").toAbsolutePath();
    generateKeyAndTrustStore(hostName, keyStoreFile, trustStoreFile);
  }

  @Test
  public void canConnectToLocatorWithoutSsl() throws Exception {
    final Locator locator = Locator.startLocatorAndDS(locatorPort, null, properties);
    try {
      new JmxOperationInvoker(hostName, jmxPort, properties).stop();
    } finally {
      locator.stop();
    }
  }

  @Test
  public void canConnectToLocatorWithSsl() throws Exception {
    properties.putAll(generateSecurityProperties(false, keyStoreFile, trustStoreFile));

    final Locator locator = Locator.startLocatorAndDS(locatorPort, null, properties);
    try {
      new JmxOperationInvoker(hostName, jmxPort, properties).stop();
    } finally {
      locator.stop();
    }
  }

  @Test
  public void canConnectToLocatorWithSslAndEndpointValidationEnabled() throws Exception {
    properties.putAll(generateSecurityProperties(true, keyStoreFile, trustStoreFile));

    final Locator locator = Locator.startLocatorAndDS(locatorPort, null, properties);
    try {
      new JmxOperationInvoker(hostName, jmxPort, properties).stop();
    } finally {
      locator.stop();
    }
  }

  public static void generateKeyAndTrustStore(final String hostName, final Path keyStoreFile,
      final Path trustStoreFile) throws IOException, GeneralSecurityException {
    final CertificateMaterial ca =
        new CertificateBuilder(CERTIFICATE_EXPIRATION_IN_DAYS, CERTIFICATE_ALGORITHM)
            .commonName("Test CA")
            .isCA()
            .generate();

    final CertificateMaterial certificate = new CertificateBuilder(CERTIFICATE_EXPIRATION_IN_DAYS,
        CERTIFICATE_ALGORITHM)
            .commonName(hostName)
            .issuedBy(ca)
            .sanDnsName(hostName)
            .generate();

    final CertStores store = new CertStores(hostName);
    store.withCertificate("geode", certificate);
    store.trust("ca", ca);

    store.createKeyStore(keyStoreFile.toString(), STORE_PASSWORD);
    store.createTrustStore(trustStoreFile.toString(), STORE_PASSWORD);
  }

  private static Properties generateSecurityProperties(final boolean endpointIdentificationEnabled,
      final Path keyStoreFile, final Path trustStoreFile) {
    final Properties properties = new Properties();

    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, valueOf(true));
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED,
        valueOf(endpointIdentificationEnabled));
    properties.setProperty(SSL_PROTOCOLS, "any");

    properties.setProperty(SSL_KEYSTORE, keyStoreFile.toString());
    properties.setProperty(SSL_KEYSTORE_TYPE, STORE_TYPE);
    properties.setProperty(SSL_KEYSTORE_PASSWORD, STORE_PASSWORD);

    properties.setProperty(SSL_TRUSTSTORE, trustStoreFile.toString());
    properties.setProperty(SSL_TRUSTSTORE_TYPE, STORE_TYPE);
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, STORE_PASSWORD);

    return properties;
  }

}
