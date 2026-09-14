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

import java.security.cert.X509Certificate;

import org.junit.Test;

public class CertificateBuilderJUnitTest {

  private static final int DNS_NAME = 2;
  private static final int IP_ADDRESS = 7;
  private static final String SERVER_AUTH = "1.3.6.1.5.5.7.3.1";

  @Test
  public void generatedCertificateContainsRequestedExtensions() throws Exception {
    CertificateMaterial issuer = new CertificateBuilder()
        .commonName("issuer")
        .isCA()
        .generate();

    CertificateMaterial material = new CertificateBuilder()
        .commonName("server")
        .sanDnsName("localhost")
        .sanIpAddress("127.0.0.1")
        .isCA()
        .serverAuthEKU()
        .issuedBy(issuer)
        .generate();

    X509Certificate certificate = material.getCertificate();

    assertThat(certificate.getSubjectAlternativeNames())
        .extracting(name -> name.get(0) + ":" + name.get(1))
        .containsExactlyInAnyOrder(DNS_NAME + ":localhost", IP_ADDRESS + ":127.0.0.1");
    assertThat(certificate.getExtendedKeyUsage()).containsExactly(SERVER_AUTH);
    assertThat(certificate.getBasicConstraints()).isNotEqualTo(-1);
    assertThatCode(() -> certificate.verify(issuer.getPublicKey()))
        .doesNotThrowAnyException();
  }
}
