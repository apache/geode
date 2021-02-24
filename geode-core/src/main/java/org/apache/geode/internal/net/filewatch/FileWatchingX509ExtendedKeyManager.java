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

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.net.Socket;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Watches a key store file and updates the underlying key manager when the file is changed.
 */
public final class FileWatchingX509ExtendedKeyManager extends X509ExtendedKeyManager {

  private static final Logger logger = LogService.getLogger();
  private static final ConcurrentHashMap<Path, FileWatchingX509ExtendedKeyManager> instances =
      new ConcurrentHashMap<>();

  private final AtomicReference<X509ExtendedKeyManager> keyManager = new AtomicReference<>();
  private final Path keyStorePath;
  private final ThrowingSupplier<KeyManager[]> keyManagerSupplier;

  @VisibleForTesting
  FileWatchingX509ExtendedKeyManager(Path keystorePath,
      ThrowingSupplier<KeyManager[]> keyManagerSupplier,
      ExecutorService executor) {
    this.keyStorePath = keystorePath;
    this.keyManagerSupplier = keyManagerSupplier;

    loadKeyManager();

    executor.submit(new FileWatcher(this.keyStorePath, this::loadKeyManager));
  }

  /**
   * Returns a {@link FileWatchingX509ExtendedKeyManager} for the given path. A new instance will be
   * created only if one does not already exist for that path.
   *
   * @param path The path to the key store file
   * @param supplier A supplier which returns an {@link X509ExtendedKeyManager}
   */
  public static FileWatchingX509ExtendedKeyManager forPath(Path path,
      ThrowingSupplier<KeyManager[]> supplier) {
    return instances.computeIfAbsent(path,
        (Path p) -> new FileWatchingX509ExtendedKeyManager(p, supplier, newSingleThreadExecutor()));
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

  private void loadKeyManager() {
    KeyManager[] keyManagers;
    try {
      keyManagers = keyManagerSupplier.get();
    } catch (Throwable t) {
      throw new RuntimeException("Unable to load KeyManager", t);
    }

    for (KeyManager km : keyManagers) {
      if (km instanceof X509ExtendedKeyManager) {
        if (keyManager.getAndSet((X509ExtendedKeyManager) km) == null) {
          logger.info("Initialized KeyManager for {}", keyStorePath);
        } else {
          logger.info("Updated KeyManager for {}", keyStorePath);
        }
        return;
      }
    }

    throw new IllegalStateException("No X509ExtendedKeyManager available");
  }
}
