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

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.CLUSTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Integration tests for SocketCreatorFactory with SSL.
 * <p>
 * <p>
 * Renamed from {@code JSSESocketJUnitTest}.
 *
 * @see ClientSocketFactoryIntegrationTest
 */
@Category({MembershipTest.class})
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
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();


  private Throwable serverException;

  @Before
  public void setUp() throws Exception {
    File keystore = findTestKeystore();
    System.setProperty("javax.net.ssl.trustStore", keystore.getCanonicalPath());
    System.setProperty("javax.net.ssl.trustStorePassword", "password");
    System.setProperty("javax.net.ssl.keyStore", keystore.getCanonicalPath());
    System.setProperty("javax.net.ssl.keyStorePassword", "password");

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(CLUSTER_SSL_ENABLED, "true");
    properties.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(CLUSTER_SSL_CIPHERS, "any");
    properties.setProperty(CLUSTER_SSL_PROTOCOLS, "TLSv1.2");

    this.distributionConfig = new DistributionConfigImpl(properties);

    SocketCreatorFactory.setDistributionConfig(this.distributionConfig);
    this.socketCreator = SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER);

    this.localHost = InetAddress.getLocalHost();
  }

  @After
  public void tearDown() throws Exception {
    if (this.clientSocket != null) {
      this.clientSocket.close();
    }
    if (this.serverSocket != null) {
      this.serverSocket.close();
    }
    if (this.serverThread != null && this.serverThread.isAlive()) {
      this.serverThread.interrupt();
    }
    SocketCreatorFactory.close();
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
    this.serverSocket = this.socketCreator.createServerSocket(0, 0, this.localHost);
    this.serverThread = startServer(this.serverSocket, 15000);

    int serverPort = this.serverSocket.getLocalPort();
    this.clientSocket = this.socketCreator.connectForServer(this.localHost, serverPort);

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

  @Test(expected = SocketTimeoutException.class)
  public void handshakeCanTimeoutOnServer() throws Throwable {
    this.serverSocket = this.socketCreator.createServerSocket(0, 0, this.localHost);
    this.serverThread = startServer(this.serverSocket, 1000);

    int serverPort = this.serverSocket.getLocalPort();
    Socket socket = new Socket();
    socket.connect(new InetSocketAddress(localHost, serverPort));
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
    serverSocket.bind(new InetSocketAddress(SocketCreator.getLocalHost(), 0));
    Thread serverThread = new Thread() {
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
          Socket clientSocket = socketCreator.connectForClient(
              SocketCreator.getLocalHost().getHostAddress(), serverSocketPort, 500);
          clientSocket.close();
          System.err.println(
              "client successfully connected to server but should not have been able to do so");
          return false;
        } catch (SocketTimeoutException e) {
          // we need to verify that this timed out in the handshake
          // code
          System.out.println("client connect attempt timed out - checking stack trace");
          StackTraceElement[] trace = e.getStackTrace();
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
      long startTime = System.currentTimeMillis();
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
