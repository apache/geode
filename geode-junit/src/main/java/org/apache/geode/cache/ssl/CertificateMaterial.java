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
package org.apache.geode.cache.ssl;

import java.io.Serializable;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.Optional;

/**
 * Class which encapsulates a {@link X509Certificate} as well as the associated
 * {@link KeyPair}. If the certificate is not self-signed it will also hold an issuer.
 * <p/>
 * {@code CertificateMaterial} is produced by {@link CertificateBuilder}s.
 *
 * @see CertificateBuilder
 * @see CertStores
 */
public class CertificateMaterial implements Serializable {
  private final X509Certificate certificate;
  private final KeyPair keyPair;
  private final X509Certificate issuer;

  public CertificateMaterial(X509Certificate certificate, KeyPair keyPair, X509Certificate issuer) {
    this.certificate = certificate;
    this.keyPair = keyPair;
    this.issuer = issuer;
  }

  public X509Certificate getCertificate() {
    return certificate;
  }

  public PublicKey getPublicKey() {
    return keyPair.getPublic();
  }

  public PrivateKey getPrivateKey() {
    return keyPair.getPrivate();
  }

  public Optional<X509Certificate> getIssuer() {
    return Optional.ofNullable(issuer);
  }
}
