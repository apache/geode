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

import static junit.framework.TestCase.fail;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.CLUSTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.cert.CertificateException;
import java.util.Arrays;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Integration tests for SSL handshake specifically focusing on hostname validation.
 */

@Category({MembershipTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class SSLSocketHostNameVerificationIntegrationTest {

  @Parameter
  public boolean addCertificateSAN;

  @Parameter(1)
  public boolean doEndPointIdentification;

  @Parameters(name = "hasSAN={0} and doEndPointIdentification={1}")
  public static Iterable<Boolean[]> addCertificateOrNot() {
    return Arrays.asList(
        new Boolean[] {Boolean.TRUE, Boolean.TRUE},
        new Boolean[] {Boolean.TRUE, Boolean.FALSE},
        new Boolean[] {Boolean.FALSE, Boolean.TRUE},
        new Boolean[] {Boolean.FALSE, Boolean.FALSE});
  }

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
  public TestName testName = new TestName();

  private Throwable serverException;

  @Before
  public void setUp() throws Exception {
    IgnoredException.addIgnoredException("javax.net.ssl.SSLException: Read timed out");

    this.localHost = InetAddress.getLoopbackAddress();

    CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    CertStores certStores = CertStores.locatorStore();
    certStores.trust("ca", ca);

    CertificateBuilder certBuilder = new CertificateBuilder()
        .commonName("iAmTheServer")
        .issuedBy(ca);

    if (addCertificateSAN) {
      certBuilder.sanDnsName(this.localHost.getHostName());
    }

    CertificateMaterial locatorCert = certBuilder.generate();
    certStores.withCertificate("locator", locatorCert);

    this.distributionConfig =
        new DistributionConfigImpl(
            certStores.propertiesWith("cluster", false, doEndPointIdentification));

    SocketCreatorFactory.setDistributionConfig(this.distributionConfig);
    this.socketCreator = SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER);
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
  public void nioHandshakeValidatesHostName() throws Exception {
    ServerSocketChannel serverChannel = ServerSocketChannel.open();
    this.serverSocket = serverChannel.socket();

    InetSocketAddress addr = new InetSocketAddress(localHost, 0);
    serverSocket.bind(addr, 10);
    int serverPort = this.serverSocket.getLocalPort();

    this.serverThread = startServerNIO(serverSocket, 15000);

    await().until(() -> serverThread.isAlive());

    SocketChannel clientChannel = SocketChannel.open();
    await().until(
        () -> clientChannel.connect(new InetSocketAddress(localHost, serverPort)));

    this.clientSocket = clientChannel.socket();

    SSLEngine sslEngine =
        this.socketCreator.createSSLEngine(this.localHost.getHostName(), 1234);

    try {
      this.socketCreator.handshakeSSLSocketChannel(clientSocket.getChannel(),
          sslEngine, 0, true,
          ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize()),
          new BufferPool(mock(DMStats.class)));

      if (doEndPointIdentification && !addCertificateSAN) {
        fail("Failed to validate hostname in certificate SAN");
      } else {
        assertNull(serverException);
      }
    } catch (SSLHandshakeException sslException) {
      if (doEndPointIdentification && !addCertificateSAN) {
        assertThat(sslException).hasRootCauseInstanceOf(CertificateException.class)
            .hasStackTraceContaining("No name matching " + this.localHost.getHostName() + " found");
      } else {
        assertThat(sslException).doesNotHaveSameClassAs(new CertificateException(
            "No name matching " + this.localHost.getHostName() + " found"));
        throw sslException;
      }
    }
  }

  private Thread startServerNIO(final ServerSocket serverSocket, int timeoutMillis) {
    Thread serverThread = new Thread(new MyThreadGroup(this.testName.getMethodName()), () -> {
      NioSslEngine engine = null;
      Socket socket = null;
      try {
        socket = serverSocket.accept();
        SocketCreator sc = SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER);
        final SSLEngine sslEngine = sc.createSSLEngine(this.localHost.getHostName(), 1234);
        engine =
            sc.handshakeSSLSocketChannel(socket.getChannel(),
                sslEngine,
                timeoutMillis,
                false,
                ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize()),
                new BufferPool(mock(DMStats.class)));
      } catch (Throwable throwable) {
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
