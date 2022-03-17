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

package org.apache.geode.internal.net;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.internal.DMStats;

/**
 * Test NioSslEngine (a subclass of NioFilter) interaction with SSLEngine.
 */
public class NioSslEngineKeyUpdateTest {

  private static DMStats mockStats;
  private static BufferPool bufferPool;
  private static SSLContext sslContext;
  private static KeyStore keystore;
  private static char[] keystorePassword;
  private static KeyStore truststore;

  private NioSslEngine clientFilter;
  private NioSslEngine serverFilter;
  private int packetBufferSize;

  {
    // "-Djava.security.properties=/Users/bburcham/Projects/geode/geode-core/src/test/resources/org/apache/geode/internal/net/java.security"
    // 2^6 fails handshake test, 2^7 succeeds
    Security.setProperty("jdk.tls.keyLimits", "AES/GCM/NoPadding KeyUpdate 2^6");
    // Security.setProperty("jdk.tls.disabledAlgorithms","TLSv1.3, RC4, MD5withRSA, DH keySize <
    // 768");
  }

  @BeforeClass
  public static void beforeClass() throws GeneralSecurityException, IOException {
    mockStats = mock(DMStats.class);
    bufferPool = new BufferPool(mockStats);

    final Properties securityProperties = createKeystoreAndTruststore();
    keystore = KeyStore.getInstance("JKS");
    keystorePassword = securityProperties.getProperty(SSL_KEYSTORE_PASSWORD).toCharArray();
    keystore.load(new FileInputStream(securityProperties.getProperty(SSL_KEYSTORE)),
        keystorePassword);
    truststore = KeyStore.getInstance("JKS");
    final char[] truststorePassword =
        securityProperties.getProperty(SSL_TRUSTSTORE_PASSWORD).toCharArray();
    truststore.load(new FileInputStream(securityProperties.getProperty(SSL_TRUSTSTORE)),
        truststorePassword);
  }

  @Before
  public void before() throws NoSuchAlgorithmException, UnrecoverableKeyException,
      KeyStoreException, KeyManagementException {
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
    kmf.init(keystore, keystorePassword);
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
    tmf.init(truststore);

    sslContext = SSLContext.getInstance("TLS");
    final SecureRandom random = new SecureRandom();
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), random);

    final SSLParameters defaultParameters = sslContext.getDefaultSSLParameters();
    final String[] protocols = defaultParameters.getProtocols();
    final String[] cipherSuites = defaultParameters.getCipherSuites();
    System.out.println(
        String.format("TLS settings (default) before handshake: Protocols: %s, Cipher Suites: %s",
            Arrays.toString(protocols), Arrays.toString(cipherSuites)));

    final SSLEngine clientEngine = createSSLEngine("server-host", true, sslContext);
    packetBufferSize = clientEngine.getSession().getPacketBufferSize();
    clientFilter = new NioSslEngine(clientEngine, bufferPool);

    final SSLEngine serverEngine = createSSLEngine("client-host", false, sslContext);
    serverFilter = new NioSslEngine(serverEngine, bufferPool);
  }

  @Test
  public void handshakeTest() {
    peerTest((final SocketChannel channel,
        final NioSslEngine filter,
        final ByteBuffer peerNetData) -> {
      return handshakeTLS(channel, filter, peerNetData);
    },
        (final SocketChannel channel,
            final NioSslEngine filter,
            final ByteBuffer peerNetData) -> {
          return handshakeTLS(channel, filter, peerNetData);
        });
  }

  @Test
  public void secureDataTransferTest() {
    peerTest(
        (final SocketChannel channel,
            final NioSslEngine filter,
            final ByteBuffer peerNetData) -> {
          handshakeTLS(channel, filter, peerNetData);
          send(10_000, filter, channel);
          return true;
        },
        (final SocketChannel channel,
            final NioSslEngine filter,
            final ByteBuffer peerNetData) -> {
          handshakeTLS(channel, filter, peerNetData);
          final byte[] received = receive(10_000, filter, channel, packetBufferSize);
          assertThat(received).hasSize(10_000).containsOnly(1);
          return true;
        });
  }

  private static SSLEngine createSSLEngine(final String peerHost, final boolean useClientMode,
      final SSLContext sslContext) {
    final SSLEngine engine = sslContext.createSSLEngine(peerHost, 10001);
    engine.setEnabledProtocols(new String[] {"TLSv1.3"});
    engine.setEnabledCipherSuites(new String[] {"TLS_AES_256_GCM_SHA384"});
    engine.setUseClientMode(useClientMode);
    return engine;
  }

  private void peerTest(final PeerAction clientAction, final PeerAction serverAction) {
    final ExecutorService executorService = Executors.newFixedThreadPool(2);

    final CompletableFuture<SocketAddress> boundAddress = new CompletableFuture<>();

    final CompletableFuture<Boolean> serverHandshakeFuture =
        supplyAsync(
            () -> server(boundAddress, packetBufferSize, serverFilter,
                serverAction),
            executorService);

    final CompletableFuture<Boolean> clientHandshakeFuture =
        supplyAsync(
            () -> client(boundAddress, packetBufferSize, clientFilter,
                clientAction),
            executorService);

    CompletableFuture.allOf(serverHandshakeFuture, clientHandshakeFuture)
        .join();
  }

  private interface PeerAction {
    boolean apply(final SocketChannel acceptedChannel,
        final NioSslEngine filter,
        final ByteBuffer peerNetData) throws IOException;
  }

  private static boolean client(final CompletableFuture<SocketAddress> boundAddress,
      final int packetBufferSize, final NioSslEngine filter,
      final PeerAction peerAction) {
    try {
      try (final SocketChannel connectedChannel = SocketChannel.open()) {
        connectedChannel.connect(boundAddress.get());
        final ByteBuffer peerNetData =
            ByteBuffer.allocateDirect(packetBufferSize);

        final boolean result =
            peerAction.apply(connectedChannel, filter, peerNetData);
        return result;
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean server(final CompletableFuture<SocketAddress> boundAddress,
      final int packetBufferSize, final NioSslEngine filter,
      final PeerAction peerAction) {
    try (final ServerSocketChannel boundChannel = ServerSocketChannel.open()) {
      final InetSocketAddress bindAddress =
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
      boundChannel.bind(bindAddress);
      boundAddress.complete(boundChannel.getLocalAddress());
      try (final SocketChannel acceptedChannel = boundChannel.accept()) {
        final ByteBuffer peerNetData =
            ByteBuffer.allocateDirect(packetBufferSize);

        final boolean result =
            peerAction.apply(acceptedChannel, filter, peerNetData);
        return result;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean handshakeTLS(final SocketChannel channel,
      final NioSslEngine filter,
      final ByteBuffer peerNetData) throws IOException {
    final boolean blocking = channel.isBlocking();
    try {
      channel.configureBlocking(false);
      final boolean result =
          filter.handshake(channel, 6_000, peerNetData);
      System.out.println(
          String.format("TLS settings after successful handshake: Protocol: %s, Cipher Suite: %s",
              filter.engine.getSession().getProtocol(),
              filter.engine.getSession().getCipherSuite()));
      return result;
    } finally {
      channel.configureBlocking(blocking);
    }
  }

  private static Properties createKeystoreAndTruststore()
      throws GeneralSecurityException, IOException {
    final CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("server")
        .issuedBy(ca)
        .generate();

    final CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCertificate);
    serverStore.trust("ca", ca);
    return serverStore.propertiesWith("all", true, false);
  }

  private static byte[] receive(
      final int n,
      final NioSslEngine filter,
      final SocketChannel channel,
      final int packetBufferSize) throws IOException {
    final byte[] received = new byte[n];
    int pos = 0;
    do {
      final ByteBuffer netData = ByteBuffer.allocateDirect(packetBufferSize);
      channel.read(netData);
      netData.flip();
      try (final ByteBufferSharing appDataSharing = filter.unwrap(netData)) {
        final ByteBuffer appData = appDataSharing.getBuffer();
        appData.flip();
        final int newBytes = appData.remaining();
        assert pos + newBytes <= n;
        appData.get(received, pos, newBytes);
        pos += newBytes;
      }
    } while (pos < n);
    return received;
  }

  private static void send(
      final int n,
      final NioSslEngine filter,
      final SocketChannel channel)
      throws IOException {
    final ByteBuffer appData = ByteBuffer.allocateDirect(n);
    for (int i = 0; i < n; i++) {
      appData.put((byte) 1);
    }
    appData.flip();
    try (final ByteBufferSharing netDataSharing = filter.wrap(appData)) {
      final ByteBuffer netData = netDataSharing.getBuffer();
      while (netData.remaining() > 0) {
        channel.write(netData);
      }
    }
  }

}
