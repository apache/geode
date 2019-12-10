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
import static org.apache.geode.distributed.ConfigurationProperties.SSL_SERVER_NAME_EXTENSION;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.CLUSTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLSocket;

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

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class SSLSocketParameterExtensionIntegrationTest {

  private static final String MESSAGE =
      SSLSocketParameterExtensionIntegrationTest.class.getName() + " Message";

  private AtomicReference<String> messageFromClient = new AtomicReference<>();

  private DistributionConfig distributionConfig;
  private SocketCreator socketCreator;
  private InetAddress localHost;
  private Thread serverThread;
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private InternalCache cache;

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
    IgnoredException.addIgnoredException("javax.net.ssl.SSLException: Read timed out");

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
    properties.setProperty(SSL_SERVER_NAME_EXTENSION, "11");

    this.distributionConfig = new DistributionConfigImpl(properties);

    SocketCreatorFactory.setDistributionConfig(this.distributionConfig);
    this.socketCreator = SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER);

    this.localHost = InetAddress.getLocalHost();

    cache = (InternalCache) new CacheFactory(properties).create();
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
    cache.close();
  }

  @Test
  public void securedSocketCheckExtensions() throws Exception {
    this.serverSocket = this.socketCreator.createServerSocket(0, 0, this.localHost);
    this.serverThread = startServer(this.serverSocket, 15000);

    int serverPort = this.serverSocket.getLocalPort();
    this.clientSocket = this.socketCreator.connectForServer(this.localHost, serverPort);

    SSLSocket sslSocket = (SSLSocket) this.clientSocket;

    List<SNIServerName> serverNames = new ArrayList<>(1);
    SNIHostName serverName = new SNIHostName("11");
    serverNames.add(serverName);

    assertEquals(sslSocket.getSSLParameters().getServerNames(), serverNames);

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
