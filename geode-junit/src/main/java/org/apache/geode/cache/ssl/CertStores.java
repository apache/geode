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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The {@code CertStores} class encapsulates the key and trust stores typically used by various
 * components in a Geode cluster. It currently supports certificate collections for servers,
 * locators and clients. All certificates are signed by a single Root Certificate Authority.
 */
public class CertStores {
  private final String storePrefix;

  // Contents of keystore
  private Map<String, CertificateMaterial> keyStoreEntries = new HashMap<>();

  // Contents of truststore
  private Map<String, CertificateMaterial> trustedCerts = new HashMap<>();

  private String trustStorePassword = "password";
  private String keyStorePassword = "password";

  public static CertStores locatorStore() {
    return new CertStores("locator");
  }

  public static CertStores serverStore() {
    return new CertStores("server");
  }

  public static CertStores clientStore() {
    return new CertStores("client");
  }

  public CertStores(String storePrefix) {
    this.storePrefix = storePrefix;
  }

  public CertStores withCertificate(String alias, CertificateMaterial material) {
    keyStoreEntries.put(alias, material);
    return this;
  }

  public CertStores trust(String alias, CertificateMaterial material) {
    this.trustedCerts.put(alias, material);
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
    File trustStoreFile = File.createTempFile(storePrefix + "-TS-", ".jks");
    trustStoreFile.deleteOnExit();
    createTrustStore(trustStoreFile.getPath(), trustStorePassword);

    File keyStoreFile = File.createTempFile(storePrefix + "-KS-", ".jks");
    keyStoreFile.deleteOnExit();
    createKeyStore(keyStoreFile.getPath(), keyStorePassword);

    return propertiesWith(components, protocols, ciphers, trustStoreFile, trustStorePassword,
        keyStoreFile, keyStorePassword, requireAuth, endPointIdentification);
  }

  public static Properties propertiesWith(String components, String protocols, String ciphers,
      File trustStoreFile, String trustStorePassword, File keyStoreFile, String keyStorePassword,
      boolean requireAuth, boolean endPointVerification) {

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

  public void createTrustStore(String filename, String password)
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    try (InputStream in = Files.newInputStream(Paths.get(filename))) {
      ks.load(in, password.toCharArray());
    } catch (EOFException e) {
      ks = createEmptyKeyStore();
    }
    for (Map.Entry<String, CertificateMaterial> cert : trustedCerts.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue().getCertificate());
    }

    try (OutputStream out = Files.newOutputStream(Paths.get(filename))) {
      ks.store(out, password.toCharArray());
    }
  }

  public void createKeyStore(String filename, String password)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();

    for (Map.Entry<String, CertificateMaterial> entry : keyStoreEntries.entrySet()) {
      CertificateMaterial cert = entry.getValue();

      List<Certificate> chain = new ArrayList<>();
      chain.add(cert.getCertificate());

      cert.getIssuer().ifPresent(chain::add);

      ks.setKeyEntry(entry.getKey(), cert.getPrivateKey(), password.toCharArray(),
          chain.toArray(new Certificate[] {}));
    }
    try (OutputStream out = Files.newOutputStream(Paths.get(filename))) {
      ks.store(out, password.toCharArray());
    }
  }


  private KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

}
