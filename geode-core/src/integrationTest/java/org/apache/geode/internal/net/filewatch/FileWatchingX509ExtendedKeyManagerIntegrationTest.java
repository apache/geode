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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.net.SSLConfig;

public class FileWatchingX509ExtendedKeyManagerIntegrationTest {
  private static final String dummyPassword = "geode";
  private static final String alias = "alias";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File keyStore;
  private SSLConfig sslConfig;
  private FileWatchingX509ExtendedKeyManager target;

  @Before
  public void setUp() throws Exception {
    keyStore = temporaryFolder.newFile("keystore.jks");
    sslConfig = new SSLConfig.Builder()
        .setKeystore(keyStore.getAbsolutePath())
        .setKeystorePassword(dummyPassword)
        .build();
  }

  @After
  public void stopWatching() {
    if (target != null) {
      target.stopWatching();
    }
  }

  @Test
  public void initializesKeyManager() throws Exception {
    CertificateMaterial cert = storeCertificate();

    target = FileWatchingX509ExtendedKeyManager.newFileWatchingKeyManager(sslConfig);

    assertThat(target.getCertificateChain(alias)).containsExactly(cert.getCertificate());
  }

  @Test
  public void updatesKeyManager() throws Exception {
    storeCertificate();

    target = FileWatchingX509ExtendedKeyManager.newFileWatchingKeyManager(sslConfig);

    /*
     * Some file systems only have 1-second granularity for file timestamps. This sleep is needed
     * so that the timestamp AFTER the update will be greater than the timestamp BEFORE the update.
     * Otherwise, the file watcher cannot detect the change. The sleep duration needs to be several
     * seconds longer than the granularity since the sleep duration is only approximate.
     */
    Thread.sleep(Duration.ofSeconds(5).toMillis());

    CertificateMaterial updated = storeCertificate();

    await().untilAsserted(() -> assertThat(target.getCertificateChain(alias))
        .containsExactly(updated.getCertificate()));
  }

  private CertificateMaterial storeCertificate() throws Exception {
    CertificateMaterial cert = new CertificateBuilder().commonName("geode").generate();
    CertStores store = new CertStores("");
    store.withCertificate(alias, cert);
    store.createKeyStore(keyStore.getAbsolutePath(), dummyPassword);
    return cert;
  }
}
