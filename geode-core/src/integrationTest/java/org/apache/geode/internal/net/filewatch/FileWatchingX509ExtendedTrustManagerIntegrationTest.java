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
import java.security.cert.X509Certificate;
import java.time.Duration;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class FileWatchingX509ExtendedTrustManagerIntegrationTest {
  private static final String dummyPassword = "geode";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExecutorServiceRule executorService = new ExecutorServiceRule();

  private File trustStore;
  private FileWatchingX509ExtendedTrustManager target;

  @Before
  public void createTrustStoreFile() throws Exception {
    trustStore = temporaryFolder.newFile("truststore.jks");
  }

  @Test
  public void initializesTrustManager() throws Exception {
    CertificateMaterial caCert = storeCa();

    target = new FileWatchingX509ExtendedTrustManager(
        trustStore.toPath(), this::loadTrustManagerFromStore, executorService.getExecutorService());

    assertThat(target.getAcceptedIssuers()).containsExactly(caCert.getCertificate());
  }

  @Test
  public void updatesTrustManager() throws Exception {
    storeCa();

    target = new FileWatchingX509ExtendedTrustManager(
        trustStore.toPath(), this::loadTrustManagerFromStore, executorService.getExecutorService());

    await()
        // give the file watcher time to start watching for changes
        .pollDelay(Duration.ofSeconds(5))
        .untilAsserted(this::detectsChangeToTrustStoreFile);
  }

  @Test
  public void throwsIfX509ExtendedTrustManagerNotFound() {
    TrustManager[] trustManagers = new TrustManager[] {new NotAnX509ExtendedTrustManager()};

    Throwable thrown = catchThrowable(() -> new FileWatchingX509ExtendedTrustManager(
        trustStore.toPath(), () -> trustManagers, executorService.getExecutorService()));

    assertThat(thrown).isNotNull();
  }

  private void detectsChangeToTrustStoreFile() throws Exception {
    CertificateMaterial updated = storeCa();

    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(() -> {
          X509Certificate[] issuers = target.getAcceptedIssuers();
          assertThat(issuers).containsExactly(updated.getCertificate());
        });
  }

  private CertificateMaterial storeCa() throws Exception {
    CertificateMaterial cert = new CertificateBuilder().commonName("geode").generate();
    CertStores store = new CertStores("");
    store.trust("default", cert);
    store.createTrustStore(trustStore.getAbsolutePath(), dummyPassword);
    return cert;
  }

  private TrustManager[] loadTrustManagerFromStore() {
    try {
      KeyStore store = KeyStore.getInstance("JKS");
      try (FileInputStream stream = new FileInputStream(trustStore)) {
        store.load(stream, dummyPassword.toCharArray());
      }

      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(store);
      return trustManagerFactory.getTrustManagers();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class NotAnX509ExtendedTrustManager implements TrustManager {
  }
}
