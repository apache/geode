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

package org.apache.geode.cache.client.internal.provider;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.X509ExtendedKeyManager;


public abstract class CustomKeyManagerFactory extends KeyManagerFactorySpi {

  private final Logger logger = Logger.getLogger(this.getClass().getName());

  private final String algorithm;
  private final String keyStorePath;
  private KeyManagerFactory customKeyManagerFactory;
  private X509ExtendedKeyManager customKeyManager;

  private CustomKeyManagerFactory(String algorithm, String keyStorePath) {
    this.algorithm = algorithm;
    this.keyStorePath = keyStorePath;
  }

  @Override
  public final KeyManager[] engineGetKeyManagers() {
    X509ExtendedKeyManager systemKeyManager = getCustomKeyManager();
    return new KeyManager[] {systemKeyManager};
  }

  @Override
  protected final void engineInit(ManagerFactoryParameters managerFactoryParameters) {
    // not supported right now
    throw new UnsupportedOperationException("use engineInit with keystore");
  }

  @Override
  public final void engineInit(KeyStore keyStore, char[] chars) {
    // ignore the passed in keystore as it will be null
    init();
  }

  private void init() {
    String SSL_KEYSTORE_TYPE = "JKS";
    String SSL_KEYSTORE_PASSWORD = "password";

    try {
      FileInputStream fileInputStream = new FileInputStream(keyStorePath);
      KeyStore keyStore = KeyStore.getInstance(SSL_KEYSTORE_TYPE);
      keyStore.load(fileInputStream, SSL_KEYSTORE_PASSWORD.toCharArray());
      this.customKeyManagerFactory = KeyManagerFactory.getInstance(this.algorithm, "SunJSSE");
      this.customKeyManagerFactory.init(keyStore, SSL_KEYSTORE_PASSWORD.toCharArray());
    } catch (NoSuchAlgorithmException | IOException | CertificateException
        | UnrecoverableKeyException | KeyStoreException | NoSuchProviderException e) {
      throw new UndeclaredThrowableException(e);
    }
  }

  private X509ExtendedKeyManager getCustomKeyManager() {
    if (this.customKeyManager == null) {
      for (KeyManager candidate : this.customKeyManagerFactory.getKeyManagers()) {
        if (candidate instanceof X509ExtendedKeyManager) {
          this.logger.info("Adding System Key Manager");
          this.customKeyManager = (X509ExtendedKeyManager) candidate;
          break;
        }
      }
    }
    return this.customKeyManager;
  }

  public static final class PKIXFactory extends CustomKeyManagerFactory {
    public PKIXFactory(String keyStorePath) {
      super("PKIX", keyStorePath);
    }
  }

  public static final class SimpleFactory extends CustomKeyManagerFactory {
    public SimpleFactory(String keyStorePath) {
      super("SunX509", keyStorePath);
    }
  }
}
