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
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Objects;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import org.apache.geode.annotations.VisibleForTesting;

public class SSLUtil {
  /**
   * This is a list of the algorithms that are tried, in order, when "any" is specified. Update
   * this list as new algorithms become available and are supported by Geode. Remove old,
   * no-longer trusted algorithms.
   */
  protected static final String[] DEFAULT_ALGORITMS_PRE_JAVA11 = {
      "TLSv1.2"};
  protected static final String[] DEFAULT_ALGORITMS = {
      "TLSv1.3",
      "TLSv1.2"}; // TLSv1.3 is not available in JDK 8 at this time



  public static SSLContext getSSLContextInstance(SSLConfig sslConfig)
      throws NoSuchAlgorithmException {
    String[] protocols = sslConfig.getProtocolsAsStringArray();
    String[] protocolsForAny = getDefaultAlgorithms();
    return findSSLContextForProtocols(protocols, protocolsForAny);
  }

  /**
   * Returns the default algorithms that are used to search for an SSLContext
   * when "any" is given as the protocol by the user.
   */
  public static String[] getDefaultAlgorithms() {
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_11)) {
      return DEFAULT_ALGORITMS;
    } else {
      // tlsv1.3 is not supported by Geode before JAVA 11
      return DEFAULT_ALGORITMS_PRE_JAVA11;
    }
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
    SSLContext result = null;
    for (String protocol : protocols) {
      if (protocol.equalsIgnoreCase("any")) {
        try {
          result = findSSLContextForProtocols(protocolsForAny, new String[0]);
          break;
        } catch (NoSuchAlgorithmException e) {
          // none of the default algorithms is available - continue to see if there
          // are any others in the requested list
        }
      }
      try {
        result = SSLContext.getInstance(protocol);
        break;
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    if (result != null) {
      if (result.getProtocol().equalsIgnoreCase("tlsv1.3") &&
          SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_10)) {
        throw new IllegalStateException("TLSv1.3 is not supported for this JRE - please use TLSv1.2"
            + " or upgrade to Java 11");
      }
      return result;
    }
    throw new NoSuchAlgorithmException("unable to find support for configured TLS protocols: " +
        Arrays.toString(protocols));
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
