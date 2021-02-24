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
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Watches a trust store file and updates the underlying trust manager when the file is changed.
 */
public final class FileWatchingX509ExtendedTrustManager extends X509ExtendedTrustManager {

  private static final Logger logger = LogService.getLogger();
  private static final ConcurrentHashMap<Path, FileWatchingX509ExtendedTrustManager> instances =
      new ConcurrentHashMap<>();

  private final AtomicReference<X509ExtendedTrustManager> trustManager = new AtomicReference<>();
  private final Path trustStorePath;
  private final ThrowingSupplier<TrustManager[]> trustManagerSupplier;

  @VisibleForTesting
  FileWatchingX509ExtendedTrustManager(Path trustStorePath,
      ThrowingSupplier<TrustManager[]> trustManagerSupplier, ExecutorService executor) {
    this.trustStorePath = trustStorePath;
    this.trustManagerSupplier = trustManagerSupplier;

    loadTrustManager();

    executor.submit(new FileWatcher(this.trustStorePath, this::loadTrustManager));
  }

  /**
   * Returns a {@link FileWatchingX509ExtendedTrustManager} for the given path. A new instance will
   * be created only if one does not already exist for that path.
   *
   * @param path The path to the trust store file
   * @param supplier A supplier which returns an {@link X509ExtendedTrustManager}
   */
  public static FileWatchingX509ExtendedTrustManager forPath(Path path,
      ThrowingSupplier<TrustManager[]> supplier) {
    return instances.computeIfAbsent(path, (Path p) -> new FileWatchingX509ExtendedTrustManager(p,
        supplier, newSingleThreadExecutor()));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket)
      throws CertificateException {
    trustManager.get().checkClientTrusted(x509Certificates, s, socket);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine)
      throws CertificateException {
    trustManager.get().checkClientTrusted(x509Certificates, s, sslEngine);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
      throws CertificateException {
    trustManager.get().checkClientTrusted(x509Certificates, s);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine)
      throws CertificateException {
    trustManager.get().checkServerTrusted(x509Certificates, s, sslEngine);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket)
      throws CertificateException {
    trustManager.get().checkServerTrusted(x509Certificates, s, socket);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
      throws CertificateException {
    trustManager.get().checkServerTrusted(x509Certificates, s);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return trustManager.get().getAcceptedIssuers();
  }

  private void loadTrustManager() {
    TrustManager[] trustManagers;
    try {
      trustManagers = trustManagerSupplier.get();
    } catch (Throwable t) {
      throw new RuntimeException("Unable to load TrustManager", t);
    }

    for (TrustManager tm : trustManagers) {
      if (tm instanceof X509ExtendedTrustManager) {
        if (trustManager.getAndSet((X509ExtendedTrustManager) tm) == null) {
          logger.info("Initialized TrustManager for {}", trustStorePath);
        } else {
          logger.info("Updated TrustManager for {}", trustStorePath);
        }
        return;
      }
    }

    throw new IllegalStateException("No X509ExtendedTrustManager available");
  }
}
