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
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

/**
 * An {@link AuthInitialize} implementation that obtains the digital signature for use with PKCS
 * scheme on server from the given set of properties.
 *
 * To use this class the {@code security-client-auth-init} property should be set to the fully
 * qualified name the static {@code create} function viz.
 * {@code org.apache.geode.security.templates.PKCSAuthInit.create}
 *
 * @since GemFire 5.5
 */
public class PKCSAuthInit implements AuthInitialize {

  private static final Logger logger = LogService.getLogger();

  public static final String KEYSTORE_FILE_PATH = "security-keystorepath";
  public static final String KEYSTORE_ALIAS = "security-alias";
  public static final String KEYSTORE_PASSWORD = "security-keystorepass";
  public static final String SIGNATURE_DATA = "security-signature";

  protected LogWriter systemLogWriter;
  protected LogWriter securityLogWriter;

  public static AuthInitialize create() {
    return new PKCSAuthInit();
  }

  @Override
  public void init(final LogWriter systemLogWriter, final LogWriter securityLogWriter)
      throws AuthenticationFailedException {
    this.systemLogWriter = systemLogWriter;
    this.securityLogWriter = securityLogWriter;
  }

  @Override
  public Properties getCredentials(final Properties securityProperties,
      final DistributedMember server, final boolean isPeer) throws AuthenticationFailedException {
    final String keyStorePath = securityProperties.getProperty(KEYSTORE_FILE_PATH);
    if (keyStorePath == null) {
      throw new AuthenticationFailedException(
          "PKCSAuthInit: key-store file path property [" + KEYSTORE_FILE_PATH + "] not set.");
    }

    final String alias = securityProperties.getProperty(KEYSTORE_ALIAS);
    if (alias == null) {
      throw new AuthenticationFailedException(
          "PKCSAuthInit: key alias name property [" + KEYSTORE_ALIAS + "] not set.");
    }

    final String keyStorePass = securityProperties.getProperty(KEYSTORE_PASSWORD);

    try {
      final KeyStore ks = KeyStore.getInstance("PKCS12");
      final char[] passPhrase = (keyStorePass != null ? keyStorePass.toCharArray() : null);
      final FileInputStream certificatefile = new FileInputStream(keyStorePath);

      try {
        ks.load(certificatefile, passPhrase);
      } finally {
        certificatefile.close();
      }

      final Key key = ks.getKey(alias, passPhrase);

      if (key instanceof PrivateKey) {
        final PrivateKey privKey = (PrivateKey) key;
        final X509Certificate cert = (X509Certificate) ks.getCertificate(alias);
        final Signature sig = Signature.getInstance(cert.getSigAlgName());

        sig.initSign(privKey);
        sig.update(alias.getBytes("UTF-8"));
        final byte[] signatureBytes = sig.sign();

        final Properties newprops = new Properties();
        newprops.put(KEYSTORE_ALIAS, alias);
        newprops.put(SIGNATURE_DATA, signatureBytes);
        return newprops;

      } else {
        throw new AuthenticationFailedException(
            "PKCSAuthInit: " + "Failed to load private key from the given file: " + keyStorePath);
      }

    } catch (Exception ex) {
      throw new AuthenticationFailedException(
          "PKCSAuthInit: Exception while getting credentials: " + ex, ex);
    }
  }

  @Override
  public void close() {}
}
