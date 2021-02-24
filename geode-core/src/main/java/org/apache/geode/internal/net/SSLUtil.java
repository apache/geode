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
package org.apache.geode.internal.net;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedKeyManager;
import org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedTrustManager;

public class SSLUtil {
  /**
   * This is a list of the algorithms that are tried, in order, when "any" is specified. Update
   * this list as new algorithms become available and are supported by Geode. Remove old,
   * no-longer trusted algorithms.
   */
  protected static final String[] DEFAULT_ALGORITMS = {
      "TLSv1.3",
      "TLSv1.2"}; // TLSv1.3 is not available in JDK 8 at this time



  public static SSLContext getSSLContextInstance(SSLConfig sslConfig)
      throws NoSuchAlgorithmException {
    String[] protocols = sslConfig.getProtocolsAsStringArray();
    return findSSLContextForProtocols(protocols, DEFAULT_ALGORITMS);
  }

  /**
   * Search for a context supporting one of the given prioritized list of
   * protocols. The second argument is a list of protocols to try if the
   * first list contains "any". The second argument should also be in prioritized
   * order. If there are no matches for any of the protocols in the second
   * argument we will continue in the first argument list.
   * with a first argument of A, B, any, C
   * and a second argument of D, E
   * the search order would be A, B, D, E, C
   */
  @VisibleForTesting
  protected static SSLContext findSSLContextForProtocols(final String[] protocols,
      final String[] protocolsForAny)
      throws NoSuchAlgorithmException {
    for (String protocol : protocols) {
      if (protocol.equalsIgnoreCase("any")) {
        try {
          return findSSLContextForProtocols(protocolsForAny, new String[0]);
        } catch (NoSuchAlgorithmException e) {
          // none of the default algorithms is available - continue to see if there
          // are any others in the requested list
        }
      }
      try {
        return SSLContext.getInstance(protocol);
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    throw new NoSuchAlgorithmException();
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

      KeyManager[] keyManagers = null;
      if (sslConfig.getKeystore() != null) {
        Path path = Paths.get(sslConfig.getKeystore());
        keyManagers = new KeyManager[] {
            FileWatchingX509ExtendedKeyManager.forPath(path, () -> getKeyManagers(sslConfig))
        };
      }

      TrustManager[] trustManagers = null;
      if (sslConfig.getTruststore() != null) {
        Path path = Paths.get(sslConfig.getTruststore());
        trustManagers = new TrustManager[] {
            FileWatchingX509ExtendedTrustManager.forPath(path,
                () -> getTrustManagers(sslConfig, skipSslVerification))
        };
      }

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

        keyManagerFactory = getDefaultKeyManagerFactory();
        keyManagerFactory.init(clientKeys, sslConfig.getKeystorePassword().toCharArray());
      }
    } finally {
      if (keyStoreStream != null) {
        keyStoreStream.close();
      }
    }

    return keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null;
  }

  static KeyManagerFactory getDefaultKeyManagerFactory() throws NoSuchAlgorithmException {
    return KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
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
        trustManagerFactory = getDefaultTrustManagerFactory();
        trustManagerFactory.init(serverPub);
      }
    } finally {
      if (trustStoreStream != null) {
        trustStoreStream.close();
      }
    }
    return trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null;
  }

  static TrustManagerFactory getDefaultTrustManagerFactory()
      throws NoSuchAlgorithmException {
    return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
  }

}
