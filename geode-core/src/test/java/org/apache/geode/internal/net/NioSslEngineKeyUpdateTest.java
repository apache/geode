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
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
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
import org.junit.Test;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.internal.DMStats;

/**
 * Test NioSslEngine (a subclass of NioFilter) interaction with SSLEngine.
 */
public class NioSslEngineKeyUpdateTest {

  private DMStats mockStats;
  private NioSslEngine clientFilter;
  private BufferPool bufferPool;
  private NioSslEngine serverFilter;
  private int packetBufferSize;

  {
    Security.setProperty("jdk.tls.keyLimits", "AES/GCM/NoPadding KeyUpdate 2^7");
  }

  @Before
  public void before() throws GeneralSecurityException, IOException {
    mockStats = mock(DMStats.class);
    bufferPool = new BufferPool(mockStats);

    final Properties securityProperties = createKeystoreAndTruststore();
    final KeyStore keystore = KeyStore.getInstance("JKS");
    final char[] keystorePassword =
        securityProperties.getProperty(SSL_KEYSTORE_PASSWORD).toCharArray();
    keystore.load(new FileInputStream(securityProperties.getProperty(SSL_KEYSTORE)),
        keystorePassword);
    KeyStore truststore = KeyStore.getInstance("JKS");
    final char[] truststorePassword =
        securityProperties.getProperty(SSL_TRUSTSTORE_PASSWORD).toCharArray();
    truststore.load(new FileInputStream(securityProperties.getProperty(SSL_TRUSTSTORE)),
        truststorePassword);

    final KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
    kmf.init(keystore, keystorePassword);
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
    tmf.init(truststore);

    final SSLContext context = SSLContext.getInstance("TLS");
    final SecureRandom random = new SecureRandom();
    context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), random);

    final SSLParameters defaultParameters = context.getDefaultSSLParameters();
    final String[] protocols = defaultParameters.getProtocols();
    final String[] cipherSuites = defaultParameters.getCipherSuites();
    System.out.println(String.format("TLS settings (default) before handshake: Protocols: %s, Cipher Suites: %s",
        Arrays.toString(protocols), Arrays.toString(cipherSuites)));

    final SSLEngine clientEngine = context.createSSLEngine("server-host", 10001);
    clientEngine.setEnabledProtocols(new String[]{"TLSv1.3"});
    clientEngine.setEnabledCipherSuites(new String[]{"TLS_AES_256_GCM_SHA384"});
    clientEngine.setUseClientMode(true);
    packetBufferSize = clientEngine.getSession().getPacketBufferSize();
    clientFilter = new NioSslEngine(clientEngine, bufferPool);
    final SSLEngine serverEngine = context.createSSLEngine("client-host", 10001);
    serverEngine.setEnabledProtocols(new String[]{"TLSv1.3"});
    serverEngine.setEnabledCipherSuites(new String[]{"TLS_AES_256_GCM_SHA384"});
    serverEngine.setUseClientMode(false);
    serverFilter = new NioSslEngine(serverEngine, bufferPool);
  }

  @Test
  public void handshakeTest() {
    peerTest(NioSslEngineKeyUpdateTest::handshakeTLS,NioSslEngineKeyUpdateTest::handshakeTLS);
  }

  @Test
  public void secureDataTransferTest() {
    peerTest(
        (final SocketChannel channel,
         final NioSslEngine filter,
         final ByteBuffer peerNetData) -> {
          handshakeTLS(channel,filter,peerNetData);
          send(channel, 100);
          return true;
          },
        (final SocketChannel channel,
         final NioSslEngine filter,
         final ByteBuffer peerNetData) -> {
          handshakeTLS(channel,filter,peerNetData);
          final byte[] received = receive(channel, 100);
          assertThat(received).contains(1,1);
          return true;
        });
  }

  @Test
  public void handshakeTestWithKeyUpdate() {
    peerTest(NioSslEngineKeyUpdateTest::handshakeTLS,NioSslEngineKeyUpdateTest::handshakeTLS);
  }

  @Test
  public void keyUpdateTest() {

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
      System.out.println(String.format("TLS settings after successful handshake: Protocol: %s, Cipher Suite: %s",
          filter.engine.getSession().getProtocol(), filter.engine.getSession().getCipherSuite()));
      return result;
    } finally {
      channel.configureBlocking(blocking);
    }
  }

  private Properties createKeystoreAndTruststore() throws GeneralSecurityException, IOException {
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

  private static byte[] receive(final SocketChannel channel, final int n) throws IOException {
    final ByteBuffer inboundTest = ByteBuffer.allocateDirect(n);
    channel.read(inboundTest);
    inboundTest.flip();
    final byte[] received = new byte[n];
    inboundTest.get(received, 0, n);
    return received;
  }

  private static void send(final SocketChannel channel, final int n) throws IOException {
    final ByteBuffer outboundTest = ByteBuffer.allocateDirect(n);
    for (int i = 0; i < n; i++) {
      outboundTest.put((byte) 1);
    }
    outboundTest.flip();
    channel.write(outboundTest);
  }

}
