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
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.util.PasswordUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Watches a trust store file and updates the underlying trust manager when the file is changed.
 */
public final class FileWatchingX509ExtendedTrustManager extends X509ExtendedTrustManager {

  private static final Logger logger = LogService.getLogger();

  /*
   * This annotation is needed for the PMD checks to pass, but it probably should not be a
   * cache-scoped field since there only needs to be one set of instances per JVM.
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<Path, FileWatchingX509ExtendedTrustManager> instances =
      new ConcurrentHashMap<>();

  private final AtomicReference<X509ExtendedTrustManager> trustManager = new AtomicReference<>();
  private final Path trustStorePath;
  private final String trustStoreType;
  private final String trustStorePassword;
  private final PollingFileWatcher fileWatcher;

  private FileWatchingX509ExtendedTrustManager(Path trustStorePath, String trustStoreType,
      String trustStorePassword) {
    this.trustStorePath = trustStorePath;
    this.trustStoreType = trustStoreType;
    this.trustStorePassword = trustStorePassword;

    loadTrustManager();

    fileWatcher =
        new PollingFileWatcher(this.trustStorePath, this::loadTrustManager, this::stopWatching);
  }

  /**
   * Returns a {@link FileWatchingX509ExtendedTrustManager} for the given SSL config. A new instance
   * will be created only if one does not already exist for the provided trust store path.
   *
   * @param config The SSL config to use to load the trust manager
   */
  public static FileWatchingX509ExtendedTrustManager newFileWatchingTrustManager(SSLConfig config) {
    return newFileWatchingTrustManager(Paths.get(config.getTruststore()),
        config.getTruststoreType(), config.getTruststorePassword());
  }

  /**
   * Returns a {@link FileWatchingX509ExtendedTrustManager} for the options. A new instance
   * will be created only if one does not already exist for the provided trust store path.
   *
   * @param trustStorePath The path of the trust store to watch for changes (or create if one does
   *        not yet exist).
   * @param type The type of store to create - typically "JKS"
   * @param password The password to use to secure the store
   */
  public static FileWatchingX509ExtendedTrustManager newFileWatchingTrustManager(
      Path trustStorePath,
      String type, String password) {
    return instances.computeIfAbsent(trustStorePath,
        (Path p) -> new FileWatchingX509ExtendedTrustManager(trustStorePath, type, password));
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

  @VisibleForTesting
  void stopWatching() {
    fileWatcher.stop();
    instances.remove(trustStorePath, this);
  }

  @VisibleForTesting
  boolean isWatching() {
    return instances.get(trustStorePath) == this;
  }

  private void loadTrustManager() {
    TrustManager[] trustManagers;
    try {
      KeyStore trustStore;
      if (StringUtils.isEmpty(trustStoreType)) {
        trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      } else {
        trustStore = KeyStore.getInstance(trustStoreType);
      }

      char[] password = null;
      try (FileInputStream fis = new FileInputStream(trustStorePath.toString())) {
        String passwordString = trustStorePassword;
        if (passwordString != null) {
          if (passwordString.trim().equals("")) {
            if (!StringUtils.isEmpty(passwordString)) {
              String toDecrypt = "encrypted(" + passwordString + ")";
              passwordString = PasswordUtil.decrypt(toDecrypt);
              password = passwordString.toCharArray();
            }
          } else {
            password = passwordString.toCharArray();
          }
        }
        trustStore.load(fis, password);
      }

      // default algorithm can be changed by setting property "ssl.TrustManagerFactory.algorithm" in
      // security properties
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      trustManagers = tmf.getTrustManagers();

      // follow the security tip in java doc
      if (password != null) {
        java.util.Arrays.fill(password, ' ');
      }
    } catch (Exception e) {
      throw new InternalGemFireException("Unable to load TrustManager", e);
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
