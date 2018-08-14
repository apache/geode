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

import static org.apache.geode.cache.ssl.TestSSLUtils.createKeyStore;
import static org.apache.geode.cache.ssl.TestSSLUtils.createTrustStore;
import static org.apache.geode.cache.ssl.TestSSLUtils.generateKeyPair;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
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

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CertStores {
  private final String alias;
  private final String storePrefix;

  private Map<String, X509Certificate> trustedCerts = new HashMap<>();

  private File keyStoreFile;

  private String trustStorePassword = "password";
  private String keyStorePassword = "password";

  private X509Certificate cert;

  public static CertStores locatorStore() {
    return new CertStores("locator", "locator");
  }

  public static CertStores serverStore() {
    return new CertStores("server", "server");
  }

  public static CertStores clientStore() {
    return new CertStores("client", "client");
  }

  public CertStores(String alias, String storePrefix) {
    this.alias = alias;
    this.storePrefix = storePrefix;
  }

  public String alias() {
    return alias;
  }

  public X509Certificate certificate() {
    return cert;
  }

  public CertStores withCertificate(TestSSLUtils.CertificateBuilder certificateBuilder)
      throws GeneralSecurityException, IOException {
    keyStoreFile = File.createTempFile(storePrefix + "KS", ".jks");
    withCertificate(certificateBuilder, keyStoreFile);
    return this;
  }

  private void withCertificate(TestSSLUtils.CertificateBuilder certificateBuilder,
      File keyStoreFile) throws GeneralSecurityException, IOException {
    KeyPair keyPair = generateKeyPair("RSA");
    cert = certificateBuilder.generate(keyPair);
    createKeyStore(keyStoreFile.getPath(), keyStorePassword, alias, keyPair.getPrivate(), cert);
  }

  public CertStores trustSelf() {
    this.trustedCerts.put(alias, cert);
    return this;
  }

  public CertStores trust(String alias, X509Certificate certificate) {
    this.trustedCerts.put(alias, certificate);
    return this;
  }

  public Properties propertiesWith(String components)
      throws GeneralSecurityException, IOException {
    return this.propertiesWith(components, "any", "any", true, true);
  }

  public Properties propertiesWith(String components, boolean requireAuth,
      boolean endPointIdentification)
      throws GeneralSecurityException, IOException {
    return this.propertiesWith(components, "any", "any", requireAuth, endPointIdentification);
  }

  public Properties propertiesWith(String components, String protocols,
      String ciphers, boolean requireAuth, boolean endPointIdentification)
      throws GeneralSecurityException, IOException {
    File trustStoreFile = File.createTempFile(storePrefix + "TS", ".jks");
    trustStoreFile.deleteOnExit();

    createTrustStore(trustStoreFile.getPath(), trustStorePassword, trustedCerts);

    return propertiesWith(components, protocols, ciphers, trustStoreFile, keyStoreFile, requireAuth,
        endPointIdentification);
  }

  private Properties propertiesWith(String components, String protocols, String ciphers,
      File trustStoreFile, File keyStoreFile, boolean requireAuth, boolean endPointVerification) {

    Properties sslConfigs = new Properties();
    sslConfigs.setProperty(SSL_ENABLED_COMPONENTS, components);
    sslConfigs.setProperty(SSL_KEYSTORE, keyStoreFile.getPath());
    sslConfigs.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    sslConfigs.setProperty(SSL_KEYSTORE_PASSWORD, keyStorePassword);
    sslConfigs.setProperty(SSL_TRUSTSTORE, trustStoreFile.getPath());
    sslConfigs.setProperty(SSL_TRUSTSTORE_PASSWORD, trustStorePassword);
    sslConfigs.setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    sslConfigs.setProperty(SSL_PROTOCOLS, protocols);
    sslConfigs.setProperty(SSL_CIPHERS, ciphers);
    sslConfigs.setProperty(SSL_REQUIRE_AUTHENTICATION, String.valueOf(requireAuth));
    sslConfigs.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED,
        String.valueOf(endPointVerification));

    return sslConfigs;
  }
}
