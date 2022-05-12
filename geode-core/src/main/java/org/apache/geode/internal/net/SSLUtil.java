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

import static org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedKeyManager.newFileWatchingKeyManager;
import static org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedTrustManager.newFileWatchingTrustManager;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class SSLUtil {
  /**
   * This is a list of the protocols that are tried, in order, to create an SSLContext. Update
   * this list as new protocols become available and are supported by Geode. Remove old,
   * no-longer trusted protocols.
   */
  static final String[] SUPPORTED_CONTEXTS = {"TLSv1.3", "TLSv1.2"};

  public static @NotNull SSLContext getSSLContextInstance()
      throws NoSuchAlgorithmException {
    for (String protocol : SUPPORTED_CONTEXTS) {
      try {
        return SSLContext.getInstance(protocol);
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    throw new NoSuchAlgorithmException();
  }

  /** Splits an array of values from a string, whitespace or comma separated. */
  static @NotNull String[] split(final @Nullable String text) {
    if (StringUtils.isBlank(text)) {
      return new String[0];
    }

    return text.split("[\\s,]+");
  }

  public static @NotNull SSLContext createAndConfigureSSLContext(final @NotNull SSLConfig sslConfig,
      final boolean skipSslVerification) {
    try {
      if (sslConfig.useDefaultSSLContext()) {
        return SSLContext.getDefault();
      }

      final SSLContext ssl = getSSLContextInstance();

      final KeyManager[] keyManagers;
      if (sslConfig.getKeystore() != null) {
        keyManagers = new KeyManager[] {newFileWatchingKeyManager(sslConfig)};
      } else {
        keyManagers = null;
      }

      final TrustManager[] trustManagers;
      if (skipSslVerification) {
        trustManagers = new TrustManager[] {new X509TrustManager() {
          @Override
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
          }

          @Override
          public void checkClientTrusted(X509Certificate[] certs, String authType) {}

          @Override
          public void checkServerTrusted(X509Certificate[] certs, String authType) {}

        }};
      } else if (sslConfig.getTruststore() != null) {
        trustManagers = new TrustManager[] {newFileWatchingTrustManager(sslConfig)};
      } else {
        trustManagers = null;
      }

      ssl.init(keyManagers, trustManagers, new SecureRandom());
      return ssl;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  static @NotNull KeyManagerFactory getDefaultKeyManagerFactory() throws NoSuchAlgorithmException {
    return KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
  }

  static @NotNull TrustManagerFactory getDefaultTrustManagerFactory()
      throws NoSuchAlgorithmException {
    return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
  }
}
