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
package org.apache.geode.experimental.driver;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Objects;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SocketFactory {
  private InetAddress host;
  private int port;
  private int timeout = -1;
  private String keyStorePath;
  private String trustStorePath;
  private String protocols;
  private String ciphers;

  public SocketFactory() {
    // Do nothing.
  }

  public InetAddress getHost() {
    return host;
  }

  public SocketFactory setHost(InetAddress host) {
    this.host = host;
    return this;
  }

  public int getPort() {
    return port;
  }

  public SocketFactory setPort(int port) {
    this.port = port;
    return this;
  }

  public int getTimeout() {
    return timeout;
  }

  public SocketFactory setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public SocketFactory setKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
    return this;
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public SocketFactory setTrustStorePath(String trustStorePath) {
    this.trustStorePath = trustStorePath;
    return this;
  }

  public String getProtocols() {
    return protocols;
  }

  public SocketFactory setProtocols(String protocols) {
    this.protocols = protocols;
    return this;
  }

  public String getCiphers() {
    return ciphers;
  }

  public SocketFactory setCiphers(String ciphers) {
    this.ciphers = ciphers;
    return this;
  }

  public boolean isSsl() {
    return (!Objects.isNull(keyStorePath) && !keyStorePath.isEmpty())
        || (!Objects.isNull(trustStorePath) && !trustStorePath.isEmpty());
  }

  public Socket connect() throws GeneralSecurityException, IOException {
    Socket socket;

    SocketAddress sockaddr = new InetSocketAddress(host, port);
    if (isSsl()) {
      final SSLContext sslContext = getSSLContextInstance();
      final KeyManager[] keyManagers = getKeyManagers();
      final TrustManager[] trustManagers = getTrustManagers();
      sslContext.init(keyManagers, trustManagers, null /* use the default secure random */);

      javax.net.SocketFactory socketFactory = sslContext.getSocketFactory();
      socket = socketFactory.createSocket();

      socket.connect(sockaddr, Math.max(timeout, 0));
      if (socket instanceof SSLSocket) {
        SSLSocket sslSocket = (SSLSocket) socket;
        sslSocket.setUseClientMode(true);
        sslSocket.setEnableSessionCreation(true);
        if (timeout > 0) {
          sslSocket.setSoTimeout(timeout);
        }
        if (protocols != null) {
          sslSocket.setEnabledProtocols(protocols.split(" "));
        }
        if (ciphers != null) {
          sslSocket.setEnabledCipherSuites(ciphers.split(" "));
        }
        sslSocket.startHandshake();
      }
    } else {
      socket = new Socket();
      socket.connect(sockaddr, Math.max(timeout, 0));
    }

    return socket;
  }

  private SSLContext getSSLContextInstance() throws IOException {
    String[] knownAlgorithms = {"SSL", "SSLv2", "SSLv3", "TLS", "TLSv1", "TLSv1.1", "TLSv1.2"};
    for (String algo : knownAlgorithms) {
      try {
        return SSLContext.getInstance(algo);
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    throw new IOException("SSL not configured correctly, unable create an SSLContext");
  }

  private TrustManager[] getTrustManagers()
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
    if (Objects.isNull(trustStorePath)) {
      return new TrustManager[0];
    }

    String trustStoreType = "jks";
    KeyStore keyStore = KeyStore.getInstance(trustStoreType);
    FileInputStream fileInputStream = new FileInputStream(trustStorePath);
    char[] password = "password".toCharArray();
    keyStore.load(fileInputStream, password);

    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(keyStore);
    return tmf.getTrustManagers();
  }

  private KeyManager[] getKeyManagers() throws KeyStoreException, IOException,
      NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
    if (Objects.isNull(keyStorePath)) {
      return new KeyManager[0];
    }

    String keyStoreType = "jks";
    KeyStore keyStore = KeyStore.getInstance(keyStoreType);
    FileInputStream fileInputStream = new FileInputStream(keyStorePath);
    char[] password = "password".toCharArray();
    keyStore.load(fileInputStream, password);

    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, password);
    return keyManagerFactory.getKeyManagers();
  }
}
