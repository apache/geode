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
import java.security.cert.CertificateException;
import java.util.logging.Logger;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509ExtendedTrustManager;


public abstract class CustomTrustManagerFactory extends TrustManagerFactorySpi {

  private final Logger logger = Logger.getLogger(this.getClass().getName());

  private final String algorithm;
  private final String trustStorePath;
  private TrustManagerFactory customTrustManagerFactory;
  private X509ExtendedTrustManager customTrustManager;

  private CustomTrustManagerFactory(String algorithm, String trustStorePath) {
    this.algorithm = algorithm;
    this.trustStorePath = trustStorePath;
  }

  @Override
  public final TrustManager[] engineGetTrustManagers() {
    X509ExtendedTrustManager systemTrustManager = getCustomTrustManager();
    return new TrustManager[] {systemTrustManager};
  }

  @Override
  public final void engineInit(ManagerFactoryParameters managerFactoryParameters) {
    // not supported right now
    throw new UnsupportedOperationException("use engineInit with keystore");
  }

  @Override
  public final void engineInit(KeyStore keyStore) {
    // ignore the passed in keystore as it will be null
    init();
  }

  private X509ExtendedTrustManager getCustomTrustManager() {
    if (this.customTrustManager == null) {
      for (TrustManager candidate : this.customTrustManagerFactory.getTrustManagers()) {
        if (candidate instanceof X509ExtendedTrustManager) {
          this.logger.info("Adding System Trust Manager");
          this.customTrustManager = (X509ExtendedTrustManager) candidate;
          break;
        }
      }
    }
    return this.customTrustManager;
  }

  private void init() {
    String trustStoreType = "JKS";
    String trustStorePassword = "password";

    try {
      FileInputStream fileInputStream = new FileInputStream(trustStorePath);
      KeyStore trustStore = KeyStore.getInstance(trustStoreType);
      trustStore.load(fileInputStream, trustStorePassword.toCharArray());
      this.customTrustManagerFactory = TrustManagerFactory.getInstance(this.algorithm, "SunJSSE");
      this.customTrustManagerFactory.init(trustStore);
    } catch (NoSuchAlgorithmException | IOException | CertificateException | KeyStoreException
        | NoSuchProviderException e) {
      throw new UndeclaredThrowableException(e);
    }
  }

  public static final class PKIXFactory extends CustomTrustManagerFactory {
    public PKIXFactory(String trustStorePath) {
      super("PKIX", trustStorePath);
    }
  }

  public static final class SimpleFactory extends CustomTrustManagerFactory {
    public SimpleFactory(String trustStorePath) {
      super("SunX509", trustStorePath);
    }
  }
}
