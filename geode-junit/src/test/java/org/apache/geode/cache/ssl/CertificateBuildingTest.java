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

import java.security.KeyStore;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.junit.Test;

public class CertificateBuildingTest {

  @Test
  public void builtCertificatesCanBeValidatedCorrectly() throws Exception {
    CertificateMaterial ca = new CertificateBuilder(10, "SHA256withRSA")
        .commonName("my test CA")
        .isCA()
        .generate();

    CertificateMaterial cert = new CertificateBuilder(10, "SHA256withRSA")
        .commonName("test-host")
        .sanDnsName("another-hostname")
        .sanIpAddress("127.0.0.1")
        .issuedBy(ca)
        .generate();

    KeyStore certStore = KeyStore.getInstance("JKS");
    certStore.load(null, "password".toCharArray());
    certStore.setCertificateEntry("ca", ca.getCertificate());

    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(certStore);

    for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
      if (trustManager instanceof X509TrustManager) {
        X509TrustManager x509TrustManager = (X509TrustManager) trustManager;
        x509TrustManager
            .checkServerTrusted(new X509Certificate[] {
                ca.getCertificate(),
                cert.getCertificate()},
                "RSA");
      }
    }
  }

}
