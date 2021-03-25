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
package org.apache.geode.internal.net.filewatch;


import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.apache.commons.lang3.StringUtils;

/**
 * ExtendedAliasKeyManager supports use of certificate aliases in distributed system
 * properties.
 */
final class ExtendedAliasKeyManager extends X509ExtendedKeyManager {

  private final X509ExtendedKeyManager delegate;

  private final String keyAlias;

  /**
   * Constructor.
   *
   * @param mgr The X509KeyManager used as a delegate
   * @param keyAlias The alias name of the server's keypair and supporting certificate chain
   */
  ExtendedAliasKeyManager(X509ExtendedKeyManager mgr, String keyAlias) {
    this.delegate = mgr;
    this.keyAlias = keyAlias;
  }


  @Override
  public String[] getClientAliases(final String s, final Principal[] principals) {
    return delegate.getClientAliases(s, principals);
  }

  @Override
  public String chooseClientAlias(final String[] strings, final Principal[] principals,
      final Socket socket) {
    if (!StringUtils.isEmpty(this.keyAlias)) {
      return keyAlias;
    }
    return delegate.chooseClientAlias(strings, principals, socket);
  }

  @Override
  public String[] getServerAliases(final String s, final Principal[] principals) {
    return delegate.getServerAliases(s, principals);
  }

  @Override
  public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
    if (!StringUtils.isEmpty(this.keyAlias)) {
      PrivateKey key = this.delegate.getPrivateKey(this.keyAlias);
      return getKeyAlias(keyType, key);
    }
    return this.delegate.chooseServerAlias(keyType, issuers, socket);

  }

  @Override
  public X509Certificate[] getCertificateChain(final String s) {
    if (!StringUtils.isEmpty(this.keyAlias)) {
      return delegate.getCertificateChain(keyAlias);
    }
    return delegate.getCertificateChain(s);
  }

  @Override
  public PrivateKey getPrivateKey(final String alias) {
    return delegate.getPrivateKey(alias);
  }

  @Override
  public String chooseEngineClientAlias(String[] keyTypes, Principal[] principals,
      SSLEngine sslEngine) {
    return delegate.chooseEngineClientAlias(keyTypes, principals, sslEngine);
  }

  @Override
  public String chooseEngineServerAlias(final String keyType, final Principal[] principals,
      final SSLEngine sslEngine) {
    if (!StringUtils.isEmpty(this.keyAlias)) {
      PrivateKey key = this.delegate.getPrivateKey(this.keyAlias);
      return getKeyAlias(keyType, key);
    }
    return this.delegate.chooseEngineServerAlias(keyType, principals, sslEngine);

  }

  private String getKeyAlias(final String keyType, final PrivateKey key) {
    if (key != null) {
      if (key.getAlgorithm().equals(keyType)) {
        return this.keyAlias;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }
}
