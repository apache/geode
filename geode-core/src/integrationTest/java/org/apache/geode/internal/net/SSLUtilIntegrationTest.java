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

import static org.assertj.core.api.Assertions.assertThat;

import java.security.NoSuchAlgorithmException;
import java.security.Security;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Test;

public class SSLUtilIntegrationTest {

  static final String SSL_KEY_MANAGER_FACTORY_ALGORITHM = "ssl.KeyManagerFactory.algorithm";
  static final String SSL_TRUST_MANAGER_FACTORY_ALGORITHM = "ssl.TrustManagerFactory.algorithm";
  static final String PKIX = "PKIX";
  static final String SUN_X_509 = "SunX509";

  @Test
  public void getDefaultKeyManagerFactoryControlledBySystemProperty()
      throws NoSuchAlgorithmException {
    String original = Security.getProperty(SSL_KEY_MANAGER_FACTORY_ALGORITHM);
    try {
      Security.setProperty(SSL_KEY_MANAGER_FACTORY_ALGORITHM, PKIX);
      final KeyManagerFactory pkixKeyManagerFactory = SSLUtil.getDefaultKeyManagerFactory();
      assertThat(pkixKeyManagerFactory).isNotNull();
      assertThat(pkixKeyManagerFactory.getAlgorithm()).isEqualTo(PKIX);

      Security.setProperty(SSL_KEY_MANAGER_FACTORY_ALGORITHM, SUN_X_509);
      final KeyManagerFactory x509KeyManagerFactory = SSLUtil.getDefaultKeyManagerFactory();
      assertThat(x509KeyManagerFactory).isNotNull();
      assertThat(x509KeyManagerFactory.getAlgorithm()).isEqualTo(SUN_X_509);
    } finally {
      Security.setProperty(SSL_KEY_MANAGER_FACTORY_ALGORITHM, original);
    }
  }

  @Test
  public void getDefaultTrustManagerFactoryControlledBySystemProperty()
      throws NoSuchAlgorithmException {
    String original = Security.getProperty(SSL_TRUST_MANAGER_FACTORY_ALGORITHM);
    try {
      Security.setProperty(SSL_TRUST_MANAGER_FACTORY_ALGORITHM, PKIX);
      final TrustManagerFactory pkixTrustManagerFactory = SSLUtil.getDefaultTrustManagerFactory();
      assertThat(pkixTrustManagerFactory).isNotNull();
      assertThat(pkixTrustManagerFactory.getAlgorithm()).isEqualTo(PKIX);

      Security.setProperty(SSL_TRUST_MANAGER_FACTORY_ALGORITHM, SUN_X_509);
      final TrustManagerFactory x509TrustManagerFactory = SSLUtil.getDefaultTrustManagerFactory();
      assertThat(x509TrustManagerFactory).isNotNull();
      assertThat(x509TrustManagerFactory.getAlgorithm()).isEqualTo(SUN_X_509);
    } finally {
      Security.setProperty(SSL_TRUST_MANAGER_FACTORY_ALGORITHM, original);
    }
  }
}
