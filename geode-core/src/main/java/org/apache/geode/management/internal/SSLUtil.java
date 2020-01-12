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
package org.apache.geode.management.internal;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Objects;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.admin.SSLConfig;

/**
 *
 * @since GemFire 8.1
 */
public class SSLUtil {
  public static SSLContext getSSLContextInstance(SSLConfig sslConfig) {
    String[] protocols = sslConfig.getProtocolsAsStringArray();
    SSLContext sslContext = null;
    if (protocols != null && protocols.length > 0) {
      for (String protocol : protocols) {
        if (!protocol.equals("any")) {
          try {
            sslContext = SSLContext.getInstance(protocol);
            break;
          } catch (NoSuchAlgorithmException e) {
            // continue
          }
        }
      }
    }
    if (sslContext != null) {
      return sslContext;
    }
    // lookup known algorithms
    String[] knownAlgorithms = {"SSL", "SSLv2", "SSLv3", "TLS", "TLSv1", "TLSv1.1", "TLSv1.2"};
    for (String algo : knownAlgorithms) {
      try {
        sslContext = SSLContext.getInstance(algo);
        break;
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    return sslContext;
  }

  /** Read an array of values from a string, whitespace or comma separated. */
  public static String[] readArray(String text) {
    if (StringUtils.isBlank(text)) {
      return null;
    }

    return text.split("[\\s,]+");
  }

  public static SSLContext createAndConfigureSSLContext(SSLConfig sslConfig,
      boolean skipSslVerification) {
    try {
      if (sslConfig.useDefaultSSLContext()) {
        return SSLContext.getDefault();
      }
      SSLContext ssl = getSSLContextInstance(sslConfig);

      KeyManager[] keyManagers = getKeyManagers(sslConfig);
      TrustManager[] trustManagers = getTrustManagers(sslConfig, skipSslVerification);

      ssl.init(keyManagers, trustManagers, new SecureRandom());
      return ssl;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private static KeyManager[] getKeyManagers(SSLConfig sslConfig) throws Exception {
    FileInputStream keyStoreStream = null;
    KeyManagerFactory keyManagerFactory = null;

    try {
      if (StringUtils.isNotBlank(sslConfig.getKeystore())) {
        String keyStoreType = Objects.toString(sslConfig.getKeystoreType(), "JKS");
        KeyStore clientKeys = KeyStore.getInstance(keyStoreType);
        keyStoreStream = new FileInputStream(sslConfig.getKeystore());
        clientKeys.load(keyStoreStream, sslConfig.getKeystorePassword().toCharArray());

        keyManagerFactory =
            KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(clientKeys, sslConfig.getKeystorePassword().toCharArray());
      }
    } finally {
      if (keyStoreStream != null) {
        keyStoreStream.close();
      }
    }

    return keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null;
  }

  private static TrustManager[] getTrustManagers(SSLConfig sslConfig, boolean skipSslVerification)
      throws Exception {
    FileInputStream trustStoreStream = null;
    TrustManagerFactory trustManagerFactory = null;

    if (skipSslVerification) {
      TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager() {
        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType) {}

      }};
      return trustAllCerts;
    }

    try {
      // load server public key
      if (StringUtils.isNotBlank(sslConfig.getTruststore())) {
        String trustStoreType = Objects.toString(sslConfig.getTruststoreType(), "JKS");
        KeyStore serverPub = KeyStore.getInstance(trustStoreType);
        trustStoreStream = new FileInputStream(sslConfig.getTruststore());
        serverPub.load(trustStoreStream, sslConfig.getTruststorePassword().toCharArray());
        trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(serverPub);
      }
    } finally {
      if (trustStoreStream != null) {
        trustStoreStream.close();
      }
    }
    return trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null;
  }

}
