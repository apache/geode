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
package org.apache.geode.security.templates;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.Authenticator;

/**
 * An implementation of {@link Authenticator} that uses PKCS.
 */
public class PKCSAuthenticator implements Authenticator {

  private static final Logger logger = LogService.getLogger();

  public static final String PUBLIC_KEY_FILE = "security-publickey-filepath";
  public static final String PUBLIC_KEYSTORE_PASSWORD = "security-publickey-pass";

  private String pubKeyFilePath;
  private String pubKeyPass;
  private Map aliasCertificateMap;

  private LogWriter systemLogWriter;
  private LogWriter securityLogWriter;

  public static Authenticator create() {
    return new PKCSAuthenticator();
  }

  @Override
  public void init(final Properties securityProperties, final LogWriter systemLogWriter,
      final LogWriter securityLogWriter) throws AuthenticationFailedException {
    this.systemLogWriter = systemLogWriter;
    this.securityLogWriter = securityLogWriter;

    this.pubKeyFilePath = securityProperties.getProperty(PUBLIC_KEY_FILE);
    if (this.pubKeyFilePath == null) {
      throw new AuthenticationFailedException("PKCSAuthenticator: property " + PUBLIC_KEY_FILE
          + " not specified as the public key file.");
    }

    this.pubKeyPass = securityProperties.getProperty(PUBLIC_KEYSTORE_PASSWORD);
    this.aliasCertificateMap = new HashMap();

    populateMap();
  }

  @Override
  public Principal authenticate(final Properties credentials, final DistributedMember member)
      throws AuthenticationFailedException {
    final String alias = (String) credentials.get(PKCSAuthInit.KEYSTORE_ALIAS);
    if (alias == null || alias.length() <= 0) {
      throw new AuthenticationFailedException("No alias received");
    }

    try {
      final X509Certificate cert = getCertificate(alias);
      if (cert == null) {
        throw newException("No certificate found for alias:" + alias);
      }

      final byte[] signatureBytes = (byte[]) credentials.get(PKCSAuthInit.SIGNATURE_DATA);
      if (signatureBytes == null) {
        throw newException(
            "signature data property [" + PKCSAuthInit.SIGNATURE_DATA + "] not provided");
      }

      final Signature sig = Signature.getInstance(cert.getSigAlgName());
      sig.initVerify(cert);
      sig.update(alias.getBytes("UTF-8"));

      if (!sig.verify(signatureBytes)) {
        throw newException("verification of client signature failed");
      }

      return new PKCSPrincipal(alias);

    } catch (Exception ex) {
      throw newException(ex.toString(), ex);
    }
  }

  @Override
  public void close() {}

  private void populateMap() {
    try {
      final KeyStore keyStore = KeyStore.getInstance("JKS");
      final char[] passPhrase = this.pubKeyPass != null ? this.pubKeyPass.toCharArray() : null;
      final FileInputStream keyStoreFile = new FileInputStream(this.pubKeyFilePath);

      try {
        keyStore.load(keyStoreFile, passPhrase);
      } finally {
        keyStoreFile.close();
      }

      for (Enumeration e = keyStore.aliases(); e.hasMoreElements();) {
        final Object alias = e.nextElement();
        final Certificate cert = keyStore.getCertificate((String) alias);
        if (cert instanceof X509Certificate) {
          this.aliasCertificateMap.put(alias, cert);
        }
      }

    } catch (Exception e) {
      throw new AuthenticationFailedException(
          "Exception while getting public keys: " + e.getMessage(), e);
    }
  }

  private AuthenticationFailedException newException(final String message, final Exception cause) {
    final String fullMessage =
        "PKCSAuthenticator: Authentication of client failed due to: " + message;
    if (cause != null) {
      return new AuthenticationFailedException(fullMessage, cause);
    } else {
      return new AuthenticationFailedException(fullMessage);
    }
  }

  private AuthenticationFailedException newException(final String message) {
    return newException(message, null);
  }

  private X509Certificate getCertificate(final String alias)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    if (this.aliasCertificateMap.containsKey(alias)) {
      return (X509Certificate) this.aliasCertificateMap.get(alias);
    }
    return null;
  }
}
