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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
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
  private NioSslEngine activeFilter;
  private BufferPool bufferPool;
  private NioSslEngine passiveFilter;
  private int packetBufferSize;

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
    final SSLEngine clientEngine = context.createSSLEngine("server-host", 10001);
    clientEngine.setUseClientMode(true);
    packetBufferSize = clientEngine.getSession().getPacketBufferSize();
    activeFilter = new NioSslEngine(clientEngine, bufferPool);
    final SSLEngine serverEngine = context.createSSLEngine("client-host", 10001);
    serverEngine.setUseClientMode(false);
    passiveFilter = new NioSslEngine(serverEngine, bufferPool);
  }

  @Test
  public void handshakeTest() throws InterruptedException {

    final ExecutorService executorService = Executors.newFixedThreadPool(2);

    final CompletableFuture<SocketAddress> boundAddress = new CompletableFuture<>();

    final CompletableFuture<Boolean> passiveHandshakeFuture =
        supplyAsync(
            () -> {
              try (final ServerSocketChannel boundChannel = ServerSocketChannel.open()) {
                final InetSocketAddress bindAddress =
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
                boundChannel.bind(bindAddress);
                boundAddress.complete(boundChannel.getLocalAddress());
                try (final SocketChannel acceptedChannel = boundChannel.accept()) {
                  final ByteBuffer passiveSidePeerNetData =
                      ByteBuffer.allocateDirect(packetBufferSize);

                  acceptedChannel.configureBlocking(false);

                  final boolean result =
                      passiveFilter.handshake(acceptedChannel, 6_000, passiveSidePeerNetData);
                  return result;
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            executorService);

    Thread.sleep(2_000);

    final CompletableFuture<Boolean> activeHandshakeFuture =
        supplyAsync(
            () -> {
              try {
                try (final SocketChannel connectedChannel = SocketChannel.open()) {
                  connectedChannel.connect(boundAddress.get());
                  final ByteBuffer activeSidePeerNetData =
                      ByteBuffer.allocateDirect(packetBufferSize);

                  connectedChannel.configureBlocking(false);

                  final boolean result =
                      activeFilter.handshake(connectedChannel, 6_000, activeSidePeerNetData);
                  return result;
                }
              } catch (IOException | InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            },
            executorService);


    CompletableFuture.allOf(passiveHandshakeFuture, activeHandshakeFuture)
        .join();

  }

  @Test
  public void keyUpdateTest() {

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

  private static void receiveOneByte(final SocketChannel channel) throws IOException {
    final ByteBuffer inboundTest = ByteBuffer.allocateDirect(1);
    channel.read(inboundTest);
    inboundTest.flip();
    final byte[] received = new byte[1];
    inboundTest.get(received, 0, 1);
    assertThat(received).contains(1);
  }

  private static void sendOneByte(final SocketChannel channel) throws IOException {
    final ByteBuffer outboundTest = ByteBuffer.allocateDirect(1);
    outboundTest.put((byte) 1);
    outboundTest.flip();
    channel.write(outboundTest);
  }

}
