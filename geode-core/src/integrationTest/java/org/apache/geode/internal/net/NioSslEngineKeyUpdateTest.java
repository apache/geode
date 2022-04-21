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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.internal.DMStats;

/**
 * In TLSv1.3, when a GCM-based cipher is used, there is a limit on the number
 * of bytes that may be encoded with a key. When that limit is reached, a TLS
 * KeyUpdate message is generated (by the SSLEngine). That message causes a new
 * key to be negotiated between the peer SSLEngines.
 *
 * Geode's {@link NioSslEngine} class (subclass of {@link NioFilter}) encapsulates
 * Java's {@link SSLEngine}. Geode's MsgStreamer, Connection, and ConnectionTable
 * classes interact with NioSslEngine to accomplish peer-to-peer (P2P) messaging.
 *
 * This test constructs a pair of SSLEngine's and wraps them in NioSslEngine.
 * Rather than relying on MsgStreamer, Connection, or ConnectionTable classes
 * (which themselves have a lot of dependencies on other classes), this test
 * implements simplified logic for driving the sending and receiving of data,
 * i.e. calling NioSslEngine.wrap() and NioSslEngine.unwrap(). See
 * {@link #send(int, NioFilter, SocketChannel, int)} and
 * {@link #receive(int, NioFilter, SocketChannel, ByteBuffer)}.
 *
 * The {@link #keyUpdateDuringSecureDataTransferTest()} arranges for the encrypted bytes limit
 * to be reached very quickly (see {@link #ENCRYPTED_BYTES_LIMIT}).
 * The test verifies data transfer continues correctly, after the limit is reached.
 * This indirectly verifies that the KeyUpdate protocol initiated by the sending
 * SSLEngine is correctly handled by all the components involved.
 */
public class NioSslEngineKeyUpdateTest {

  private static final String TLS_PROTOCOL = "TLSv1.3";
  private static final String TLS_CIPHER_SUITE = "TLS_AES_256_GCM_SHA384";

  // number of bytes the GCM cipher can encrypt before initiating a KeyUpdate
  private static final int ENCRYPTED_BYTES_LIMIT = 1;

  {
    Security.setProperty("jdk.tls.keyLimits",
        "AES/GCM/NoPadding KeyUpdate " + ENCRYPTED_BYTES_LIMIT);
  }

  private static BufferPool bufferPool;
  private static SSLContext sslContext;
  private static KeyStore keystore;
  private static char[] keystorePassword;
  private static KeyStore truststore;

  private SSLEngine clientEngine;
  private SSLEngine serverEngine;
  private int packetBufferSize;

  @BeforeAll
  public static void beforeClass() throws GeneralSecurityException, IOException {
    DMStats mockStats = mock(DMStats.class);
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

  @BeforeEach
  public void before() throws NoSuchAlgorithmException, UnrecoverableKeyException,
      KeyStoreException, KeyManagementException {
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
    kmf.init(keystore, keystorePassword);
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
    tmf.init(truststore);

    sslContext = SSLContext.getInstance("TLS");
    final SecureRandom random = new SecureRandom(new byte[] {1, 2, 3});
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), random);

    final SSLParameters defaultParameters = sslContext.getDefaultSSLParameters();
    final String[] protocols = defaultParameters.getProtocols();
    final String[] cipherSuites = defaultParameters.getCipherSuites();
    System.out.println(
        String.format("TLS settings (default) before handshake: Protocols: %s, Cipher Suites: %s",
            Arrays.toString(protocols), Arrays.toString(cipherSuites)));

    clientEngine = createSSLEngine("server-host", true, sslContext);
    packetBufferSize = clientEngine.getSession().getPacketBufferSize();

    serverEngine = createSSLEngine("client-host", false, sslContext);
  }

  /*
   * Verify initial handshake succeeds in the presence of KeyUpdate messages
   * (i.e. updating send-side cryptographic keys).
   *
   * This test verifies, primarily, the behavior of
   * NioSslEngine.handshake(SocketChannel, int, ByteBuffer)
   */
  @Test
  public void keyUpdateDuringInitialHandshakeTest() {
    clientServerTest(
        (channel, filter, peerNetData) -> {
          handshakeTLS(channel, filter, peerNetData, "Client:");
          return true;
        },
        (channel, filter, peerNetData1) -> {
          handshakeTLS(channel, filter, peerNetData1,
              "Server:");
          return true;
        });
  }

  /*
   * Building on keyUpdateDuringInitialHandshakeTest(), this test verifies that
   * after the handshake succeeds, subsequent data transfer succeeds in the presence
   * of KeyUpdate messages (i.e. updating send-side cryptographic keys).
   *
   * This test verifies, primarily, the behavior of NioSslEngine#wrap(ByteBuffer).
   */
  @Test
  public void keyUpdateDuringSecureDataTransferTest() {
    clientServerTest(
        (final SocketChannel channel,
            final NioSslEngine filter,
            final ByteBuffer peerNetData) -> {
          handshakeTLS(channel, filter, peerNetData, "Client:");
          /*
           * In order to verify that KeyUpdate is properly handled in NioSslEngine.wrap()
           * we must arrange for the KeyUpdate (generation and processing) to occur after
           * the initial handshake and before the handshaking during NioSslEngine.close().
           *
           * If we call send() only once, regardless of the number of bytes wrapped (even
           * if it exceeds the encryption byte limit set in jdk.tls.keyLimits, the status
           * result from SSLEngine.wrap() will be OK. We will fail to encounter the situation
           * where it is e.g. BUFFER_OVERFLOW.
           *
           * By calling send() with bytesToSend >= the limit, we can be sure that the
           * subsequent call to send() will trigger the KeyUpdate and will require proper
           * handling of that situation in NioSslEngine.wrap().
           */
          send(ENCRYPTED_BYTES_LIMIT, filter, channel, 0);
          send(ENCRYPTED_BYTES_LIMIT, filter, channel, ENCRYPTED_BYTES_LIMIT);

          return true;
        },
        (final SocketChannel channel,
            final NioSslEngine filter,
            final ByteBuffer peerNetData) -> {
          handshakeTLS(channel, filter, peerNetData, "Server:");
          /*
           * Call receive() twice (like we did for send()) just to test that receive() is
           * leaving buffers in the correct readable/writable state when it returns.
           */
          for (int i = 0; i < 2; i++) {
            final byte[] received =
                receive(ENCRYPTED_BYTES_LIMIT, filter, channel, peerNetData);
            assertThat(received).hasSize(ENCRYPTED_BYTES_LIMIT);
            for (int j = i * ENCRYPTED_BYTES_LIMIT; j < (i + 1)
                * ENCRYPTED_BYTES_LIMIT; j++) {
              assertThat(received[j % received.length]).isEqualTo((byte) j);
            }
          }
          return true;
        });
  }

  /*
   * Building on keyUpdateDuringSecureDataTransferTest(), this test verifies that
   * NioSslEngine.close() succeeds in the presence of KeyUpdate messages
   * (i.e. updating send-side cryptographic keys). This test is important because
   * NioSslEngine.close() involves some TLS handshaking.
   *
   * This test verifies, primarily, the behavior of NioSslEngine#close(SocketChannel).
   */
  @Test
  public void keyUpdateDuringSocketCloseHandshakeTest() {
    clientServerTest(
        (final SocketChannel channel,
            final NioSslEngine filter,
            final ByteBuffer peerNetData) -> {
          handshakeTLS(channel, filter, peerNetData, "Client:");
          /*
           * Leave send-side SSLEngine in a state where it will generate a KeyUpdate
           * TLS message during (but not before) NioSslEngine.close().
           */
          send(ENCRYPTED_BYTES_LIMIT, filter, channel, 0);
          return true;
        },
        (final SocketChannel channel,
            final NioSslEngine filter,
            final ByteBuffer peerNetData) -> {
          handshakeTLS(channel, filter, peerNetData, "Server:");
          receive(ENCRYPTED_BYTES_LIMIT, filter, channel, peerNetData);
          /*
           * No need to validate the received data since our purpose is only to verify that
           * NioSslEngine.close() succeeds cleanly.
           */
          return true;
        });
  }

  private static SSLEngine createSSLEngine(final String peerHost, final boolean useClientMode,
      final SSLContext sslContext) {
    final SSLEngine engine = sslContext.createSSLEngine(peerHost, 10001);
    engine.setEnabledProtocols(new String[] {TLS_PROTOCOL});
    engine.setEnabledCipherSuites(new String[] {TLS_CIPHER_SUITE});
    engine.setUseClientMode(useClientMode);
    return engine;
  }

  private void clientServerTest(final PeerAction clientAction, final PeerAction serverAction) {
    final ExecutorService executorService = Executors.newFixedThreadPool(2);

    final CompletableFuture<SocketAddress> boundAddress = new CompletableFuture<>();

    final CountDownLatch serversWaiting = new CountDownLatch(1);

    final CompletableFuture<Boolean> serverHandshakeFuture =
        supplyAsync(
            () -> server(boundAddress, packetBufferSize,
                serverAction, serversWaiting, serverEngine),
            executorService);

    final CompletableFuture<Boolean> clientHandshakeFuture =
        supplyAsync(
            () -> client(boundAddress, packetBufferSize,
                clientAction, serversWaiting, clientEngine),
            executorService);

    CompletableFuture.allOf(serverHandshakeFuture, clientHandshakeFuture)
        .join();
  }

  /*
   * An action taken on a client or server after the SocketChannel has been established.
   */
  private interface PeerAction {
    boolean apply(final SocketChannel acceptedChannel,
        final NioSslEngine filter,
        final ByteBuffer peerNetData) throws IOException;
  }

  private static boolean client(
      final CompletableFuture<SocketAddress> boundAddress,
      final int packetBufferSize,
      final PeerAction peerAction,
      final CountDownLatch serversWaiting,
      final SSLEngine engine) {
    try {
      try (final SocketChannel connectedChannel = SocketChannel.open()) {
        connectedChannel.connect(boundAddress.get());
        final ByteBuffer peerNetData =
            ByteBuffer.allocateDirect(packetBufferSize);

        final NioSslEngine filter = new NioSslEngine(engine, bufferPool);

        final boolean result =
            peerAction.apply(connectedChannel, filter, peerNetData);

        serversWaiting.await(); // wait for last server to give up before closing our socket

        filter.close(connectedChannel);

        return result;
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      printException("In client:", e);
      throw new RuntimeException(e);
    }
  }

  private static boolean server(
      final CompletableFuture<SocketAddress> boundAddress,
      final int packetBufferSize,
      final PeerAction peerAction,
      final CountDownLatch serversWaiting,
      final SSLEngine engine) {
    try (final ServerSocketChannel boundChannel = ServerSocketChannel.open()) {
      final InetSocketAddress bindAddress =
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
      boundChannel.bind(bindAddress);
      boundAddress.complete(boundChannel.getLocalAddress());
      try (final SocketChannel acceptedChannel = boundChannel.accept()) {
        final ByteBuffer peerNetData =
            ByteBuffer.allocateDirect(packetBufferSize);

        final NioSslEngine filter = new NioSslEngine(engine, bufferPool);

        final boolean result =
            peerAction.apply(acceptedChannel, filter, peerNetData);

        filter.close(acceptedChannel);

        return result;
      }
    } catch (IOException e) {
      printException("In server:", e);
      throw new RuntimeException(e);
    } finally {
      serversWaiting.countDown();
    }
  }

  private static void printException(final String context, final Exception e) {
    System.out.println(context + "\n");
    e.printStackTrace();
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

  /**
   * @param filter is a newly constructed and intitialized object; it has not been used
   *        for handshakes previously.
   * @param peerNetData on entry: don't care about read/write state or contents
   */
  private static boolean handshakeTLS(final SocketChannel channel,
      final NioSslEngine filter,
      final ByteBuffer peerNetData,
      final String context) throws IOException {
    final boolean blocking = channel.isBlocking();
    try {
      channel.configureBlocking(false);
      final boolean result =
          filter.handshake(channel, 6_000, peerNetData);
      System.out.println(
          String.format(
              "%s TLS settings after successful handshake: Protocol: %s, Cipher Suite: %s",
              context,
              filter.engine.getSession().getProtocol(),
              filter.engine.getSession().getCipherSuite()));
      return result;
    } finally {
      channel.configureBlocking(blocking);
    }
  }

  /**
   * This method is trying to do what Connection readMessages() and processInputBuffer() do
   * together.
   *
   * Note well: peerNetData may contain content on entry to this method. Also the filter's
   * buffers (e.g. the buffer returned by unwrap) may already contain data from previous
   * calls to unwrap().
   *
   * @param peerNetData will be in write mode on entry and may already contain content
   */
  private static byte[] receive(
      final int bytesToReceive,
      final NioFilter filter,
      final SocketChannel channel,
      final ByteBuffer peerNetData) throws IOException {
    final byte[] received = new byte[bytesToReceive];
    // peerNetData in write mode
    peerNetData.flip();
    // peerNetData in read mode
    int pos = 0;
    while (pos < bytesToReceive) {
      /*
       * On first iteration unwrap() is called before the channel is read. This is necessary
       * since a previous call to this method (receive()) could have left data in the filter
       * and there might not be any more data coming on the channel (ever).
       *
       * If no data was already held in the filter's buffer, and peerNetData was empty
       * before calling unwrap() then the buffer returned by unwrap will be empty. But before
       * we start the second loop iteration, we'll read (from the channel into peerNetData).
       *
       * The filter's unwrap() method takes peerNetData in read mode and when the method returns
       * the buffer is in write mode (ready for us to add data to it if needed).
       */
      try (final ByteBufferSharing appDataSharing = filter.unwrap(peerNetData)) {
        // peerNetData in write mode
        final ByteBuffer appData = appDataSharing.getBuffer();
        // appData in write mode
        appData.flip();
        // appData in read mode
        if (appData.hasRemaining()) {
          final int newBytes = Math.min(appData.remaining(), received.length - pos);
          assert pos + newBytes <= received.length;
          appData.get(received, pos, newBytes);
          pos += newBytes;
        } else {
          channel.read(peerNetData);
        }
        peerNetData.flip();
        // peerNetData in read mode ready for filter unwrap() call
        appData.compact();
        // appData in write mode ready for filter unwrap() call
      }
    }
    peerNetData.compact();
    // peerNetData in write mode
    return received;
  }

  /*
   * This method is trying to do what Connection.writeFully() does.
   */
  private static void send(
      final int bytesToSend,
      final NioFilter filter,
      final SocketChannel channel,
      final int startingValue)
      throws IOException {
    // if we wanted to send more than one buffer-full we could add an outer loop
    final ByteBuffer appData = ByteBuffer.allocateDirect(bytesToSend);
    for (int i = 0; i < bytesToSend; i++) {
      appData.put((byte) (i + startingValue));
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
