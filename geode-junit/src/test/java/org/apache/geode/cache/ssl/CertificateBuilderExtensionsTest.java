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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

/**
 * Verifies that the certificates produced by {@link CertificateBuilder} carry the expected X.509
 * extensions, so that the Bouncy Castle implementation is functionally equivalent to the previous
 * {@code sun.security.x509}-based one (GEODE-10509).
 */
public class CertificateBuilderExtensionsTest {

  // X509Certificate.getSubjectAlternativeNames() general-name type tags (RFC 5280)
  private static final int SAN_DNS = 2;
  private static final int SAN_IP = 7;

  @Test
  public void subjectIsSetFromCommonName() {
    X509Certificate cert = new CertificateBuilder().commonName("test-host").generate()
        .getCertificate();

    assertThat(cert.getSubjectX500Principal().getName())
        .contains("CN=test-host")
        .contains("O=Geode");
    assertThat(cert.getVersion()).isEqualTo(3);
  }

  @Test
  public void subjectAlternativeNamesContainDnsAndIp() throws Exception {
    X509Certificate cert = new CertificateBuilder()
        .commonName("test-host")
        .sanDnsName("example.com")
        .sanIpAddress(InetAddress.getByName("127.0.0.1"))
        .generate()
        .getCertificate();

    Collection<List<?>> sans = cert.getSubjectAlternativeNames();
    assertThat(sans).isNotNull();
    assertThat(sans).anySatisfy(san -> {
      assertThat(san.get(0)).isEqualTo(SAN_DNS);
      assertThat(san.get(1)).isEqualTo("example.com");
    });
    assertThat(sans).anySatisfy(san -> {
      assertThat(san.get(0)).isEqualTo(SAN_IP);
      assertThat(san.get(1)).isEqualTo("127.0.0.1");
    });
  }

  @Test
  public void caCertificateHasBasicConstraintsAndKeyCertSign() {
    X509Certificate ca = new CertificateBuilder().commonName("my ca").isCA().generate()
        .getCertificate();

    // getBasicConstraints() returns the path length (>= 0) for a CA, or -1 for a non-CA.
    assertThat(ca.getBasicConstraints()).isGreaterThanOrEqualTo(0);
    // KeyUsage bit 5 is keyCertSign.
    assertThat(ca.getKeyUsage()).isNotNull();
    assertThat(ca.getKeyUsage()[5]).isTrue();
  }

  @Test
  public void nonCaCertificateHasNoBasicConstraints() {
    X509Certificate cert = new CertificateBuilder().commonName("leaf").generate().getCertificate();

    assertThat(cert.getBasicConstraints()).isEqualTo(-1);
  }

  @Test
  public void extendedKeyUsageContainsServerAndClientAuth() throws Exception {
    X509Certificate cert = new CertificateBuilder()
        .commonName("svc")
        .serverAuthEKU()
        .clientAuthEKU()
        .generate()
        .getCertificate();

    assertThat(cert.getExtendedKeyUsage())
        .contains("1.3.6.1.5.5.7.3.1", "1.3.6.1.5.5.7.3.2");
  }

  @Test
  public void issuedCertificateIsSignedByAndChainsToTheIssuer() {
    CertificateMaterial ca = new CertificateBuilder().commonName("my ca").isCA().generate();
    X509Certificate leaf = new CertificateBuilder()
        .commonName("leaf")
        .issuedBy(ca)
        .generate()
        .getCertificate();

    assertThat(leaf.getIssuerX500Principal())
        .isEqualTo(ca.getCertificate().getSubjectX500Principal());
    // The leaf's signature must verify against the issuer's public key.
    assertThatCode(() -> leaf.verify(ca.getCertificate().getPublicKey())).doesNotThrowAnyException();
  }
}
