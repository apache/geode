/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.net;

import static com.jayway.awaitility.Awaitility.*;
import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.*;
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.geode.internal.FileUtil;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for SocketCreatorFactory with SSL.
 * <p>
 * <p>Renamed from {@code JSSESocketJUnitTest}.
 *
 * @see ClientSocketFactoryIntegrationTest
 */
@Category(IntegrationTest.class)
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
  public void socketCreatorShouldUseSsl() throws Exception {
    assertThat(this.socketCreator.useSSL()).isTrue();
  }

  @Test
  public void securedSocketTransmissionShouldWork() throws Exception {
    this.serverSocket = this.socketCreator.createServerSocket(0, 0, this.localHost);
    this.serverThread = startServer(this.serverSocket);

    int serverPort = this.serverSocket.getLocalPort();
    this.clientSocket = this.socketCreator.connectForServer(this.localHost, serverPort);

    // transmit expected string from Client to Server
    ObjectOutputStream output = new ObjectOutputStream(this.clientSocket.getOutputStream());
    output.writeObject(MESSAGE);
    output.flush();

    // this is the real assertion of this test
    await().atMost(1, TimeUnit.MINUTES).until(() -> assertThat(this.messageFromClient.get()).isEqualTo(MESSAGE));
  }

  private File findTestKeystore() throws IOException {
    return copyKeystoreResourceToFile("/ssl/trusted.keystore");
  }

  public File copyKeystoreResourceToFile(final String name) throws IOException {
    URL resource = getClass().getResource(name);
    assertThat(resource).isNotNull();

    File file = this.temporaryFolder.newFile(name.replaceFirst(".*/", ""));
    FileUtil.copy(resource, file);
    return file;
  }

  private Thread startServer(final ServerSocket serverSocket) throws Exception {
    Thread serverThread = new Thread(new MyThreadGroup(this.testName.getMethodName()), () -> {
      try {
        Socket socket = serverSocket.accept();
        SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER).configureServerSSLSocket(socket);
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        messageFromClient.set((String) ois.readObject());
      } catch (IOException | ClassNotFoundException e) {
        throw new Error(e);
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
