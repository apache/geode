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

import static org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedTrustManager.newFileWatchingTrustManager;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.net.SSLConfig;

public class FileWatchingX509ExtendedTrustManagerIntegrationTest {
  private static final String dummyPassword = "geode";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path trustStore;
  private FileWatchingX509ExtendedTrustManager target;

  @Before
  public void createTrustStoreFile() throws Exception {
    trustStore = temporaryFolder.newFile("truststore.jks").toPath();
  }

  @After
  public void stopWatchingTrustStoreFile() {
    if (target != null && target.isWatching()) {
      target.stopWatching();
    }
  }

  @Test
  public void initializesTrustManager() throws Exception {
    CertificateMaterial caCert = storeCa(trustStore);

    target = newFileWatchingTrustManager(sslConfigFor(trustStore));

    X509Certificate[] issuers = target.getAcceptedIssuers();
    assertThat(issuers).containsExactly(caCert.getCertificate());
  }

  @Test
  public void updatesTrustManager() throws Exception {
    storeCa(trustStore);

    target = newFileWatchingTrustManager(sslConfigFor(trustStore));

    pauseForFileWatcherToStartDetectingChanges();

    CertificateMaterial updated = storeCa(trustStore);

    await().untilAsserted(() -> {
      X509Certificate[] issuers = target.getAcceptedIssuers();
      assertThat(issuers).containsExactly(updated.getCertificate());
    });
  }

  @Test
  public void updatesTrustManagerBehindSymbolicLink() throws Exception {
    // symlink requires elevated permissions on Windows
    Assume.assumeFalse("Test ignored on Windows.", SystemUtils.IS_OS_WINDOWS);

    storeCa(trustStore);

    Path symlink = temporaryFolder.getRoot().toPath().resolve("truststore-symlink.jks");
    Files.createSymbolicLink(symlink, trustStore);

    target = newFileWatchingTrustManager(sslConfigFor(symlink));

    pauseForFileWatcherToStartDetectingChanges();

    CertificateMaterial updated = storeCa(trustStore);

    await().untilAsserted(() -> {
      X509Certificate[] issuers = target.getAcceptedIssuers();
      assertThat(issuers).containsExactly(updated.getCertificate());
    });
  }

  @Test
  public void returnsSameInstanceForSamePath() throws Exception {
    storeCa(trustStore);

    target = newFileWatchingTrustManager(sslConfigFor(trustStore));

    assertThat(target).isSameAs(newFileWatchingTrustManager(sslConfigFor(trustStore)));
  }

  @Test
  public void returnsNewInstanceForDifferentPath() throws Exception {
    Path differentPath = temporaryFolder.newFile("another-keystore.jks").toPath();
    storeCa(differentPath);
    storeCa(trustStore);

    target = newFileWatchingTrustManager(sslConfigFor(trustStore));

    FileWatchingX509ExtendedTrustManager other =
        newFileWatchingTrustManager(sslConfigFor(differentPath));
    try {
      assertThat(target).isNotSameAs(other);
    } finally {
      other.stopWatching();
    }
  }

  @Test
  public void throwsIfUnableToLoadTrustManager() {
    Path notFoundFile = temporaryFolder.getRoot().toPath().resolve("notfound");

    Throwable thrown =
        catchThrowable(() -> newFileWatchingTrustManager(sslConfigFor(notFoundFile)));

    assertThat(thrown).isNotNull();
  }

  @Test
  public void stopsWatchingWhenTrustManagerIsNoLongerValid() throws Exception {
    storeCa(trustStore);

    target = newFileWatchingTrustManager(sslConfigFor(trustStore));

    pauseForFileWatcherToStartDetectingChanges();

    Files.delete(trustStore);

    await().until(() -> !target.isWatching());
  }

  private CertificateMaterial storeCa(Path trustStore) throws Exception {
    CertificateMaterial cert = new CertificateBuilder().commonName("geode").generate();
    CertStores store = new CertStores("");
    store.trust("default", cert);
    store.createTrustStore(trustStore.toString(), dummyPassword);
    return cert;
  }

  private SSLConfig sslConfigFor(Path trustStore) {
    return new SSLConfig.Builder()
        .setTruststore(trustStore.toString())
        .setTruststorePassword(dummyPassword)
        .build();
  }

  private void pauseForFileWatcherToStartDetectingChanges() throws InterruptedException {
    /*
     * Some file systems only have 1-second granularity for file timestamps. This sleep is needed
     * so that the timestamp AFTER the update will be greater than the timestamp BEFORE the update.
     * Otherwise, the file watcher cannot detect the change. The sleep duration needs to be several
     * seconds longer than the granularity since the sleep duration is only approximate.
     */
    Thread.sleep(Duration.ofSeconds(5).toMillis());
  }
}
