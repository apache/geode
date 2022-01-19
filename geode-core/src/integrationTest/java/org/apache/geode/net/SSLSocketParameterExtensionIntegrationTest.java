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
package org.apache.geode.net;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PARAMETER_EXTENSION;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.CLUSTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
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
import javax.net.ssl.SSLParameters;
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
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class SSLSocketParameterExtensionIntegrationTest {

  private static final String MESSAGE =
      SSLSocketParameterExtensionIntegrationTest.class.getName() + " Message";

  private final AtomicReference<String> messageFromClient = new AtomicReference<>();

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
    properties.setProperty(SSL_PARAMETER_EXTENSION, MySSLParameterExtension.class.getName());
    properties.setProperty(DISTRIBUTED_SYSTEM_ID, "11");

    cache = (InternalCache) new CacheFactory(properties).create();

    distributionConfig = new DistributionConfigImpl(properties);

    SocketCreatorFactory.setDistributionConfig(distributionConfig);
    socketCreator = SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER);

    localHost = InetAddress.getLocalHost();
  }

  @After
  public void tearDown() throws Exception {
    if (clientSocket != null) {
      clientSocket.close();
    }
    if (serverSocket != null) {
      serverSocket.close();
    }
    if (serverThread != null && serverThread.isAlive()) {
      serverThread.interrupt();
    }
    SocketCreatorFactory.close();
    cache.close();
  }

  @Test
  public void securedSocketCheckExtensions() throws Exception {
    serverSocket = socketCreator.forCluster().createServerSocket(0, 0, localHost);
    serverThread = startServer(serverSocket, 15000);

    int serverPort = serverSocket.getLocalPort();
    clientSocket = socketCreator.forCluster()
        .connect(new HostAndPort(localHost.getHostAddress(), serverPort));

    SSLSocket sslSocket = (SSLSocket) clientSocket;

    List<SNIServerName> serverNames = new ArrayList<>(1);
    SNIHostName serverName = new SNIHostName("11");
    serverNames.add(serverName);

    assertThat(sslSocket.getSSLParameters().getServerNames()).isEqualTo(serverNames);

    // transmit expected string from Client to Server
    ObjectOutputStream output = new ObjectOutputStream(clientSocket.getOutputStream());
    output.writeObject(MESSAGE);
    output.flush();

    // this is the real assertion of this test
    await().until(() -> {
      return !serverThread.isAlive();
    });
    assertNull(serverException);
    assertThat(messageFromClient.get()).isEqualTo(MESSAGE);
  }

  private File findTestKeystore() throws IOException {
    return copyKeystoreResourceToFile("/ssl/trusted.keystore");
  }

  public File copyKeystoreResourceToFile(final String name) throws IOException {
    URL resource = getClass().getResource(name);
    assertThat(resource).isNotNull();

    File file = temporaryFolder.newFile(name.replaceFirst(".*/", ""));
    FileUtils.copyURLToFile(resource, file);
    return file;
  }

  private Thread startServer(final ServerSocket serverSocket, int timeoutMillis) throws Exception {
    Thread serverThread = new Thread(new MyThreadGroup(testName.getMethodName()), () -> {
      try {
        Socket socket = serverSocket.accept();
        SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER).forCluster()
            .handshakeIfSocketIsSSL(socket,
                timeoutMillis);
        assertThat(socket.getSoTimeout()).isEqualTo(0);

        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        messageFromClient.set((String) ois.readObject());
      } catch (Throwable throwable) {
        serverException = throwable;
      }
    }, testName.getMethodName() + "-server");

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

  public static class MySSLParameterExtension implements SSLParameterExtension {
    SSLParameterExtensionContext context;

    @Override
    public void init(final SSLParameterExtensionContext seedContext) {
      context = seedContext;
    }

    @Override
    public SSLParameters modifySSLClientSocketParameters(SSLParameters parameters) {
      List<SNIServerName> serverNames = new ArrayList<>(1);
      SNIHostName serverName =
          new SNIHostName(String.valueOf(context.getDistributedSystemId()));
      serverNames.add(serverName);
      parameters.setServerNames(serverNames);
      return parameters;
    }

    @Override
    public SSLParameters modifySSLServerSocketParameters(SSLParameters parameters) {
      List<SNIServerName> serverNames = new ArrayList<>(1);
      SNIHostName serverName = new SNIHostName("server");
      serverNames.add(serverName);
      parameters.setServerNames(serverNames);
      return parameters;
    }

  }
}
