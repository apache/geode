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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.time.Duration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class FileWatchingX509ExtendedKeyManagerIntegrationTest {
  private static final String dummyPassword = "geode";
  private static final String alias = "alias";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExecutorServiceRule executorService = new ExecutorServiceRule();

  private File keyStore;

  @Before
  public void createKeyStoreFile() throws Exception {
    keyStore = temporaryFolder.newFile("keystore.jks");
  }

  @Test
  public void initializesKeyManager() throws Exception {
    CertificateMaterial cert = storeCertificate();

    FileWatchingX509ExtendedKeyManager target = new FileWatchingX509ExtendedKeyManager(
        keyStore.toPath(), this::loadKeyManagerFromStore, executorService.getExecutorService());

    assertThat(target.getCertificateChain(alias)).containsExactly(cert.getCertificate());
  }

  @Test
  public void updatesKeyManager() throws Exception {
    storeCertificate();

    FileWatchingX509ExtendedKeyManager target = new FileWatchingX509ExtendedKeyManager(
        keyStore.toPath(), this::loadKeyManagerFromStore, executorService.getExecutorService());

    await().until(target::isWatching);

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

  @Test
  public void throwsIfX509ExtendedKeyManagerNotFound() {
    KeyManager[] keyManagers = new KeyManager[] {new NotAnX509ExtendedKeyManager()};

    Throwable thrown = catchThrowable(() -> new FileWatchingX509ExtendedKeyManager(
        keyStore.toPath(), () -> keyManagers, executorService.getExecutorService()));

    assertThat(thrown).isNotNull();
  }

  private CertificateMaterial storeCertificate() throws Exception {
    CertificateMaterial cert = new CertificateBuilder().commonName("geode").generate();
    CertStores store = new CertStores("");
    store.withCertificate(alias, cert);
    store.createKeyStore(keyStore.getAbsolutePath(), dummyPassword);
    return cert;
  }

  private KeyManager[] loadKeyManagerFromStore() {
    try {
      KeyStore clientKeys = KeyStore.getInstance("JKS");
      try (FileInputStream stream = new FileInputStream(keyStore)) {
        clientKeys.load(stream, dummyPassword.toCharArray());
      }

      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(clientKeys, dummyPassword.toCharArray());
      return keyManagerFactory.getKeyManagers();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class NotAnX509ExtendedKeyManager implements KeyManager {
  }
}
