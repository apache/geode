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

import java.io.FileInputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.util.PasswordUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Watches a key store file and updates the underlying key manager when the file is changed.
 */
public final class FileWatchingX509ExtendedKeyManager extends X509ExtendedKeyManager {

  private static final Logger logger = LogService.getLogger();

  /*
   * This annotation is needed for the PMD checks to pass, but it probably should not be a
   * cache-scoped field since there only needs to be one set of instances per JVM.
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<PathAndAlias, FileWatchingX509ExtendedKeyManager> instances =
      new ConcurrentHashMap<>();

  private final AtomicReference<X509ExtendedKeyManager> keyManager = new AtomicReference<>();
  private final Path keyStorePath;
  private final String keyStoreType;
  private final String keyStorePassword;
  private final String keyStoreAlias;
  private final PollingFileWatcher fileWatcher;

  private FileWatchingX509ExtendedKeyManager(Path keyStorePath, String keyStoreType,
      String keyStorePassword, String keyStoreAlias) {
    this.keyStorePath = keyStorePath;
    this.keyStoreType = keyStoreType;
    this.keyStorePassword = keyStorePassword;
    this.keyStoreAlias = keyStoreAlias;

    loadKeyManager();

    fileWatcher =
        new PollingFileWatcher(this.keyStorePath, this::loadKeyManager, this::stopWatching);
  }

  /**
   * Returns a {@link FileWatchingX509ExtendedKeyManager} for the given SSL config. A new instance
   * will be created only if one does not already exist for the provided key store path and alias.
   *
   * @param config The SSL config to use to load the key manager
   */
  public static FileWatchingX509ExtendedKeyManager newFileWatchingKeyManager(SSLConfig config) {
    return newFileWatchingKeyManager(Paths.get(config.getKeystore()), config.getKeystoreType(),
        config.getKeystorePassword(), config.getAlias());
  }

  /**
   * Returns a {@link FileWatchingX509ExtendedKeyManager} for the given SSL config. A new instance
   * will be created only if one does not already exist for the provided key store path and alias.
   *
   * @param keyStorePath The path of the key store to watch for changes (or create if one does not
   *        yet exist).
   * @param type The type of store to create - typically "JKS"
   * @param password The password to use to secure the store
   * @param alias The alias to use
   */
  public static FileWatchingX509ExtendedKeyManager newFileWatchingKeyManager(Path keyStorePath,
      String type, String password, String alias) {
    return instances.computeIfAbsent(new PathAndAlias(keyStorePath, alias),
        (PathAndAlias k) -> new FileWatchingX509ExtendedKeyManager(keyStorePath, type, password,
            alias));
  }

  @Override
  public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
    return keyManager.get().chooseClientAlias(strings, principals, socket);
  }

  @Override
  public String chooseEngineClientAlias(String[] strings, Principal[] principals,
      SSLEngine sslEngine) {
    return keyManager.get().chooseEngineClientAlias(strings, principals, sslEngine);
  }

  @Override
  public String chooseEngineServerAlias(String s, Principal[] principals, SSLEngine sslEngine) {
    return keyManager.get().chooseEngineServerAlias(s, principals, sslEngine);
  }

  @Override
  public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
    return keyManager.get().chooseServerAlias(s, principals, socket);
  }

  @Override
  public X509Certificate[] getCertificateChain(String s) {
    return keyManager.get().getCertificateChain(s);
  }

  @Override
  public String[] getClientAliases(String s, Principal[] principals) {
    return keyManager.get().getClientAliases(s, principals);
  }

  @Override
  public PrivateKey getPrivateKey(String s) {
    return keyManager.get().getPrivateKey(s);
  }

  @Override
  public String[] getServerAliases(String s, Principal[] principals) {
    return keyManager.get().getServerAliases(s, principals);
  }

  @VisibleForTesting
  void stopWatching() {
    fileWatcher.stop();
    instances.remove(new PathAndAlias(keyStorePath, keyStoreAlias), this);
  }

  @VisibleForTesting
  boolean isWatching() {
    return instances.get(new PathAndAlias(keyStorePath, keyStoreAlias)) == this;
  }

  private void loadKeyManager() {
    KeyManager[] keyManagers;
    try {
      KeyStore keyStore;
      if (StringUtils.isEmpty(keyStoreType)) {
        keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      } else {
        keyStore = KeyStore.getInstance(keyStoreType);
      }

      String keyStoreFilePath = keyStorePath.toString();
      if (StringUtils.isEmpty(keyStoreFilePath)) {
        keyStoreFilePath =
            System.getProperty("user.home") + System.getProperty("file.separator") + ".keystore";
      }

      char[] password = null;
      try (FileInputStream fileInputStream = new FileInputStream(keyStoreFilePath)) {
        String passwordString = keyStorePassword;
        if (passwordString != null) {
          if (passwordString.trim().equals("")) {
            String encryptedPass = System.getenv("javax.net.ssl.keyStorePassword");
            if (!StringUtils.isEmpty(encryptedPass)) {
              String toDecrypt = "encrypted(" + encryptedPass + ")";
              passwordString = PasswordUtil.decrypt(toDecrypt);
              password = passwordString.toCharArray();
            }
          } else {
            password = passwordString.toCharArray();
          }
        }
        keyStore.load(fileInputStream, password);
      }

      // default algorithm can be changed by setting property "ssl.KeyManagerFactory.algorithm" in
      // security properties
      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, password);
      keyManagers = keyManagerFactory.getKeyManagers();

      // follow the security tip in java doc
      if (password != null) {
        java.util.Arrays.fill(password, ' ');
      }
    } catch (Exception e) {
      throw new InternalGemFireException("Unable to load KeyManager", e);
    }

    for (KeyManager km : keyManagers) {
      if (km instanceof X509ExtendedKeyManager) {

        ExtendedAliasKeyManager extendedAliasKeyManager =
            new ExtendedAliasKeyManager((X509ExtendedKeyManager) km, keyStoreAlias);

        if (keyManager.getAndSet(extendedAliasKeyManager) == null) {
          logger.info("Initialized KeyManager for {}", keyStorePath);
        } else {
          logger.info("Updated KeyManager for {}", keyStorePath);
        }

        return;
      }
    }

    throw new IllegalStateException("No X509ExtendedKeyManager available");
  }

  private static class PathAndAlias {
    private final Path keyStorePath;
    private final String keyStoreAlias;

    public PathAndAlias(Path keyStorePath, String keyStoreAlias) {
      this.keyStorePath = keyStorePath;
      this.keyStoreAlias = keyStoreAlias;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PathAndAlias that = (PathAndAlias) o;
      return Objects.equals(keyStorePath, that.keyStorePath) && Objects
          .equals(keyStoreAlias, that.keyStoreAlias);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyStorePath, keyStoreAlias);
    }
  }
}
