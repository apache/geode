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
package org.apache.geode.client.sni;

import java.io.File;
import java.net.URL;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;

/**
 * Modified to make one-off keystore and truststore for GEM-3317 RCA
 *
 * This program generates the trust and key stores used by SNI acceptance tests.
 * The stores have a 100 year expiration date, but if you need to generate new ones
 * use this program, modified as necessary to correct problems, to generate new
 * stores.
 */
public class GenerateSNIKeyAndTrustStores2 {

  public static void main(String... args) throws Exception {
    new GenerateSNIKeyAndTrustStores2().generateStores();
  }

  public void generateStores() throws Exception {
    CertificateMaterial ca = new CertificateBuilder(365 * 100, "SHA256withRSA")
        .commonName("Test CA")
        .isCA()
        .generate();

    final String resourceFilename = "geode-config/gemfire.properties";
    final URL resource = SingleServerSNIAcceptanceTest.class.getResource(resourceFilename);
    String path = resource.getPath();
    path = path.substring(0, path.length() - "gemfire.properties".length());

    /**
     * add this line to /etc/hosts:
     *
     * 192.168.1.27 bburcham-a01.vmware.com
     */
    {
      String certName = "bburcham-a01.vmware.com";

      CertificateMaterial certificate = new CertificateBuilder(365 * 100, "SHA256withRSA")
          .commonName(certName)
          .issuedBy(ca)
          .sanDnsName(certName)
          // .sanIpAddress(InetAddress.getByName("192.168.1.27"))
          .generate();

      CertStores store = new CertStores(certName);
      store.withCertificate("geode", certificate);
      store.trust("ca", ca);

      File keyStoreFile = new File(path + certName + "-keystore.jks");
      keyStoreFile.createNewFile();
      store.createKeyStore(keyStoreFile.getAbsolutePath(), "geode");
      System.out.println("created " + keyStoreFile.getAbsolutePath());

      File trustStoreFile = new File(path + certName + "-truststore.jks");
      trustStoreFile.createNewFile();
      store.createTrustStore(trustStoreFile.getPath(), "geode");
      System.out.println("created " + trustStoreFile.getAbsolutePath());
    }
  }

}
