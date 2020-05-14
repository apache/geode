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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.ALL;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.CLUSTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.internal.ByteBufferOutputStream;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.tcp.ByteBufferInputStream;
import org.apache.geode.test.dunit.IgnoredException;

/**
 * Integration tests for SocketCreatorFactory with SSL.
 * <p>
 * <p>
 * Renamed from {@code JSSESocketJUnitTest}.
 *
 * @see ClientSocketFactoryIntegrationTest
 */
public class SSLSocketIntegrationTest {

  private static final String MESSAGE = SSLSocketIntegrationTest.class.getName() + " Message";

  private AtomicReference<String> messageFromClient = new AtomicReference<>();

  private DistributionConfig distributionConfig;
  private SocketCreator socketCreator;
  private InetAddress localHost;
  private Thread serverThread;
  private ServerSocket serverSocket;
  private Socket clientSocket;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();


  private Throwable serverException;

  @Before
  public void setUp() throws Exception {
    SocketCreatorFactory.close(); // ensure nothing lingers from past tests

    IgnoredException.addIgnoredException("javax.net.ssl.SSLException: Read timed out");

    File keystore = findTestKeystore();
    // System.setProperty("javax.net.debug", "ssl,handshake");


    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(SSL_ENABLED_COMPONENTS, ALL.getConstant());
    properties.setProperty(SSL_KEYSTORE, keystore.getCanonicalPath());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_TRUSTSTORE, keystore.getCanonicalPath());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "TLSv1.2");

    this.distributionConfig = new DistributionConfigImpl(properties);

    SocketCreatorFactory.setDistributionConfig(this.distributionConfig);
    this.socketCreator =
        SocketCreatorFactory.getSocketCreatorForComponent(this.distributionConfig, CLUSTER);

    this.localHost = LocalHostUtil.getLocalHost();
  }

  @After
  public void tearDown() throws Exception {
    SocketCreatorFactory.close();
    if (this.clientSocket != null) {
      this.clientSocket.close();
    }
    if (this.serverSocket != null) {
      this.serverSocket.close();
    }
    if (this.serverThread != null && this.serverThread.isAlive()) {
      this.serverThread.interrupt();
    }
  }

  @Test
  /**
   * see GEODE-4087. Geode should not establish a default SSLContext, preventing apps from using
   * different ssl settings via standard system properties. Since this test class sets these system
   * properties to establish a default context we merely need to perform an equality check between
   * the cluster's context and the default context and assert that they aren't the same.
   */
  public void ensureSocketCreatorDoesNotOverrideDefaultSSLContext() throws Exception {
    SSLContext defaultContext = SSLContext.getDefault();
    SSLContext clusterContext = SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).getSslContext();
    assertNotEquals(clusterContext, defaultContext);
  }

  @Test
  public void socketCreatorShouldUseSsl() throws Exception {
    assertThat(this.socketCreator.useSSL()).isTrue();
  }

  @Test
  public void securedSocketTransmissionShouldWork() throws Exception {
    this.serverSocket = this.socketCreator.forCluster().createServerSocket(0, 0, this.localHost);
    this.serverThread = startServer(this.serverSocket, 15000);

    int serverPort = this.serverSocket.getLocalPort();
    this.clientSocket = this.socketCreator.forCluster()
        .connect(new HostAndPort(this.localHost.getHostAddress(), serverPort), 0, null,
            Socket::new);

    // transmit expected string from Client to Server
    ObjectOutputStream output = new ObjectOutputStream(this.clientSocket.getOutputStream());
    output.writeObject(MESSAGE);
    output.flush();

    // this is the real assertion of this test
    await().until(() -> {
      return !serverThread.isAlive();
    });
    assertNull(serverException);
    assertThat(this.messageFromClient.get()).isEqualTo(MESSAGE);
  }

  @Test
  public void testSecuredSocketTransmissionShouldWorkUsingNIO() throws Exception {
    ServerSocketChannel serverChannel = ServerSocketChannel.open();
    serverSocket = serverChannel.socket();

    InetSocketAddress addr = new InetSocketAddress(localHost, 0);
    serverSocket.bind(addr, 10);
    int serverPort = this.serverSocket.getLocalPort();

    SocketCreator clusterSocketCreator =
        SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER);
    this.serverThread = startServerNIO(serverSocket, 15000);

    await().until(() -> serverThread.isAlive());

    SocketChannel clientChannel = SocketChannel.open();
    await().until(
        () -> clientChannel.connect(new InetSocketAddress(localHost, serverPort)));

    clientSocket = clientChannel.socket();
    NioSslEngine engine =
        clusterSocketCreator.handshakeSSLSocketChannel(clientSocket.getChannel(),
            clusterSocketCreator.createSSLEngine("localhost", 1234), 0, true,
            ByteBuffer.allocate(65535), new BufferPool(mock(DMStats.class)));
    clientChannel.configureBlocking(true);

    // transmit expected string from Client to Server
    writeMessageToNIOSSLServer(clientChannel, engine);
    writeMessageToNIOSSLServer(clientChannel, engine);
    writeMessageToNIOSSLServer(clientChannel, engine);
    // this is the real assertion of this test
    await().until(() -> {
      return !serverThread.isAlive();
    });
    assertNull(serverException);
    // assertThat(this.messageFromClient.get()).isEqualTo(MESSAGE);
  }

  private void writeMessageToNIOSSLServer(SocketChannel clientChannel, NioSslEngine engine)
      throws IOException {
    System.out.println("client sending Hello World message to server");
    ByteBufferOutputStream bbos = new ByteBufferOutputStream(5000);
    DataOutputStream dos = new DataOutputStream(bbos);
    dos.writeUTF("Hello world");
    dos.flush();
    bbos.flush();
    ByteBuffer buffer = bbos.getContentBuffer();
    System.out.println(
        "client buffer position is " + buffer.position() + " and limit is " + buffer.limit());
    ByteBuffer wrappedBuffer = engine.wrap(buffer);
    System.out.println("client wrapped buffer position is " + wrappedBuffer.position()
        + " and limit is " + wrappedBuffer.limit());
    int bytesWritten = clientChannel.write(wrappedBuffer);
    System.out.println("client bytes written is " + bytesWritten);
  }

  private Thread startServerNIO(final ServerSocket serverSocket, int timeoutMillis)
      throws Exception {
    Thread serverThread = new Thread(new MyThreadGroup(this.testName.getMethodName()), () -> {
      NioSslEngine engine = null;
      Socket socket = null;
      try {
        ByteBuffer buffer = ByteBuffer.allocate(65535);

        socket = serverSocket.accept();
        SocketCreator sc = SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER);
        engine =
            sc.handshakeSSLSocketChannel(socket.getChannel(), sc.createSSLEngine("localhost", 1234),
                timeoutMillis,
                false,
                ByteBuffer.allocate(65535),
                new BufferPool(mock(DMStats.class)));

        readMessageFromNIOSSLClient(socket, buffer, engine);
        readMessageFromNIOSSLClient(socket, buffer, engine);
        readMessageFromNIOSSLClient(socket, buffer, engine);
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
        serverException = throwable;
      } finally {
        if (engine != null && socket != null) {
          final NioSslEngine nioSslEngine = engine;
          engine.close(socket.getChannel());
          assertThatThrownBy(() -> {
            nioSslEngine.unwrap(ByteBuffer.wrap(new byte[0]));
          })
              .isInstanceOf(IOException.class);
        }
      }
    }, this.testName.getMethodName() + "-server");

    serverThread.start();
    return serverThread;
  }

  private void readMessageFromNIOSSLClient(Socket socket, ByteBuffer buffer, NioSslEngine engine)
      throws IOException {

    ByteBuffer unwrapped = engine.getUnwrappedBuffer(buffer);
    // if we already have unencrypted data skip unwrapping
    if (unwrapped.position() == 0) {
      int bytesRead;
      // if we already have encrypted data skip reading from the socket
      if (buffer.position() == 0) {
        bytesRead = socket.getChannel().read(buffer);
        buffer.flip();
      } else {
        bytesRead = buffer.remaining();
      }
      System.out.println("server bytes read is " + bytesRead + ": buffer position is "
          + buffer.position() + " and limit is " + buffer.limit());
      unwrapped = engine.unwrap(buffer);
      unwrapped.flip();
      System.out.println("server unwrapped buffer position is " + unwrapped.position()
          + " and limit is " + unwrapped.limit());
    }
    ByteBufferInputStream bbis = new ByteBufferInputStream(unwrapped);
    DataInputStream dis = new DataInputStream(bbis);
    String welcome = dis.readUTF();
    if (unwrapped.position() >= unwrapped.limit()) {
      unwrapped.position(0).limit(unwrapped.capacity());
    }
    assertThat(welcome).isEqualTo("Hello world");
    System.out.println("server read Hello World message from client");
  }


  @Test(expected = SocketTimeoutException.class)
  public void handshakeCanTimeoutOnServer() throws Throwable {
    this.serverSocket = this.socketCreator.forCluster().createServerSocket(0, 0, this.localHost);
    this.serverThread = startServer(this.serverSocket, 1000);

    int serverPort = this.serverSocket.getLocalPort();
    Socket socket = new Socket();
    socket.connect(new InetSocketAddress(localHost, serverPort));
    await().untilAsserted(() -> assertFalse(serverThread.isAlive()));
    assertNotNull(serverException);
    if (serverException instanceof SSLException
        && serverException.getCause() instanceof SocketTimeoutException) {
      throw serverException.getCause();
    }
    throw serverException;
  }

  @Test(expected = SocketTimeoutException.class)
  public void handshakeWithPeerCanTimeout() throws Throwable {
    ServerSocketChannel serverChannel = ServerSocketChannel.open();
    serverSocket = serverChannel.socket();

    InetSocketAddress addr = new InetSocketAddress(localHost, 0);
    serverSocket.bind(addr, 10);
    int serverPort = this.serverSocket.getLocalPort();

    this.serverThread = startServerNIO(this.serverSocket, 1000);

    Socket socket = new Socket();
    await().atMost(5, TimeUnit.MINUTES).until(() -> {
      try {
        socket.connect(new InetSocketAddress(localHost, serverPort));
      } catch (ConnectException e) {
        return false;
      } catch (SocketException e) {
        return true; // server socket was closed
      }
      return true;
    });
    await().untilAsserted(() -> assertFalse(serverThread.isAlive()));
    assertNotNull(serverException);
    throw serverException;
  }

  @Test
  public void configureClientSSLSocketCanTimeOut() throws Exception {
    final Semaphore serverCoordination = new Semaphore(0);

    // configure a non-SSL server socket. We will connect
    // a client SSL socket to it and demonstrate that the
    // handshake times out
    final ServerSocket serverSocket = new ServerSocket();
    serverSocket.bind(new InetSocketAddress(LocalHostUtil.getLocalHost(), 0));
    Thread serverThread = new Thread() {
      @Override
      public void run() {
        serverCoordination.release();
        try (Socket clientSocket = serverSocket.accept()) {
          System.out.println("server thread accepted a connection");
          serverCoordination.acquire();
        } catch (Exception e) {
          System.err.println("accept failed");
          e.printStackTrace();
        }
        try {
          serverSocket.close();
        } catch (IOException e) {
          // ignored
        }
        System.out.println("server thread is exiting");
      }
    };
    serverThread.setName("SocketCreatorJUnitTest serverSocket thread");
    serverThread.setDaemon(true);
    serverThread.start();

    serverCoordination.acquire();

    SocketCreator socketCreator =
        SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER);

    int serverSocketPort = serverSocket.getLocalPort();
    try {
      await("connect to server socket").until(() -> {
        try {
          Socket clientSocket = socketCreator.forClient().connect(
              new HostAndPort(LocalHostUtil.getLocalHost().getHostAddress(), serverSocketPort),
              500);
          clientSocket.close();
          System.err.println(
              "client successfully connected to server but should not have been able to do so");
          return false;
        } catch (SSLException | SocketTimeoutException e) {
          IOException ioException = e;
          // we need to verify that this timed out in the handshake
          // code
          if (e instanceof SSLException && e.getCause() instanceof SocketTimeoutException) {
            ioException = (SocketTimeoutException) ioException.getCause();
          }
          System.out.println("client connect attempt timed out - checking stack trace");
          StackTraceElement[] trace = ioException.getStackTrace();
          for (StackTraceElement element : trace) {
            if (element.getMethodName().equals("configureClientSSLSocket")) {
              System.out.println("client connect attempt timed out in the appropriate method");
              return true;
            }
          }
          // it wasn't in the configuration method so we need to try again
        } catch (IOException e) {
          // server socket may not be in accept() yet, causing a connection-refused
          // exception
        }
        return false;
      });
    } finally {
      serverCoordination.release();
    }
  }

  private File findTestKeystore() throws IOException {
    return copyKeystoreResourceToFile("/ssl/trusted.keystore");
  }

  public File copyKeystoreResourceToFile(final String name) throws IOException {
    URL resource = getClass().getResource(name);
    assertThat(resource).isNotNull();

    File file = this.temporaryFolder.newFile(name.replaceFirst(".*/", ""));
    FileUtils.copyURLToFile(resource, file);
    return file;
  }

  private Thread startServer(final ServerSocket serverSocket, int timeoutMillis) throws Exception {
    Thread serverThread = new Thread(new MyThreadGroup(this.testName.getMethodName()), () -> {
      try {
        Socket socket = serverSocket.accept();
        SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER).handshakeIfSocketIsSSL(socket,
            timeoutMillis);
        assertEquals(0, socket.getSoTimeout());
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        messageFromClient.set((String) ois.readObject());
      } catch (Throwable throwable) {
        serverException = throwable;
      }
    }, this.testName.getMethodName() + "-server");

    serverThread.start();
    return serverThread;
  }

  private class MyThreadGroup extends ThreadGroup {

    public MyThreadGroup(final String name) {
      super(name);
    }

    @Override
    public void uncaughtException(final Thread thread, final Throwable throwable) {
      errorCollector.addError(throwable);
    }
  }

}
