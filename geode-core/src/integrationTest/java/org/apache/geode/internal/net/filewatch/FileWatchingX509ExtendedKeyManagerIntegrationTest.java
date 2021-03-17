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

import static org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedKeyManager.newFileWatchingKeyManager;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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

  private Path keyStore;
  private FileWatchingX509ExtendedKeyManager target;

  @Before
  public void createKeyStoreFile() throws Exception {
    keyStore = temporaryFolder.newFile("keystore.jks").toPath();
  }

  @After
  public void stopWatchingKeyStoreFile() {
    if (target != null && target.isWatching()) {
      target.stopWatching();
    }
  }

  @Test
  public void initializesKeyManager() throws Exception {
    Map<String, CertificateMaterial> entries = storeCertificates(keyStore, alias);

    target = newFileWatchingKeyManager(sslConfigFor(keyStore, alias));

    X509Certificate[] chain = target.getCertificateChain(alias);
    assertThat(chain).containsExactly(entries.get(alias).getCertificate());
  }

  @Test
  public void initializesKeyManagerWithoutAliasSpecified() throws Exception {
    String anotherAlias = "another-alias";
    Map<String, CertificateMaterial> entries = storeCertificates(keyStore, alias, anotherAlias);

    target = newFileWatchingKeyManager(sslConfigFor(keyStore, null));

    assertThat(target.getCertificateChain(alias))
        .as("The certificate chain for alias " + alias + " contains the correct cert")
        .containsExactly(entries.get(alias).getCertificate());

    assertThat(target.getCertificateChain(anotherAlias))
        .as("The certificate chain for alias " + anotherAlias + " contains the correct cert")
        .containsExactly(entries.get(anotherAlias).getCertificate());
  }

  @Test
  public void updatesKeyManager() throws Exception {
    storeCertificates(keyStore, alias);

    target = newFileWatchingKeyManager(sslConfigFor(keyStore, alias));

    pauseForFileWatcherToStartDetectingChanges();

    Map<String, CertificateMaterial> updated = storeCertificates(keyStore, alias);

    await().untilAsserted(() -> {
      X509Certificate[] chain = target.getCertificateChain(alias);
      assertThat(chain).containsExactly(updated.get(alias).getCertificate());
    });
  }

  @Test
  public void returnsSameInstanceForSamePathAndAlias() throws Exception {
    storeCertificates(keyStore, alias);

    target = newFileWatchingKeyManager(sslConfigFor(keyStore, alias));

    assertThat(target).isSameAs(newFileWatchingKeyManager(sslConfigFor(keyStore, alias)));
  }

  @Test
  public void returnsNewInstanceForDifferentPath() throws Exception {
    Path differentPath = temporaryFolder.newFile("another-keystore.jks").toPath();
    storeCertificates(differentPath, alias);
    storeCertificates(keyStore, alias);

    target = newFileWatchingKeyManager(sslConfigFor(keyStore, alias));

    FileWatchingX509ExtendedKeyManager other =
        newFileWatchingKeyManager(sslConfigFor(differentPath, alias));
    try {
      assertThat(target).isNotSameAs(other);
    } finally {
      other.stopWatching();
    }
  }

  @Test
  public void returnsNewInstanceForDifferentAlias() throws Exception {
    String differentAlias = "different-alias";
    storeCertificates(keyStore, alias, differentAlias);

    target = newFileWatchingKeyManager(sslConfigFor(keyStore, alias));

    FileWatchingX509ExtendedKeyManager other =
        newFileWatchingKeyManager(sslConfigFor(keyStore, differentAlias));
    try {
      assertThat(target).isNotSameAs(other);
    } finally {
      other.stopWatching();
    }
  }

  @Test
  public void throwsIfUnableToLoadKeyManager() {
    Path notFoundFile = temporaryFolder.getRoot().toPath().resolve("notfound");

    Throwable thrown =
        catchThrowable(() -> newFileWatchingKeyManager(sslConfigFor(notFoundFile, alias)));

    assertThat(thrown).isNotNull();
  }

  @Test
  public void stopsWatchingWhenKeyManagerIsNoLongerValid() throws Exception {
    storeCertificates(keyStore, alias);

    target = newFileWatchingKeyManager(sslConfigFor(keyStore, alias));

    pauseForFileWatcherToStartDetectingChanges();

    Files.delete(keyStore);

    await().until(() -> !target.isWatching());
  }

  private Map<String, CertificateMaterial> storeCertificates(Path keyStore, String... aliases)
      throws Exception {
    Map<String, CertificateMaterial> entries = new HashMap<>();
    CertStores store = new CertStores("");
    for (String alias : aliases) {
      CertificateMaterial cert = new CertificateBuilder().commonName("geode").generate();
      store.withCertificate(alias, cert);
      entries.put(alias, cert);
    }
    store.createKeyStore(keyStore.toString(), dummyPassword);
    return entries;
  }

  private SSLConfig sslConfigFor(Path keyStore, String alias) {
    return new SSLConfig.Builder()
        .setKeystore(keyStore.toString())
        .setKeystorePassword(dummyPassword)
        .setAlias(alias)
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
