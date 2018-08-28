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
package org.apache.geode.distributed.internal.tcpserver;

import static org.apache.geode.security.SecurableCommunicationChannels.LOCATOR;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.util.Properties;

import javax.net.ssl.SSLHandshakeException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.TestSSLUtils.CertificateBuilder;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class TCPClientSSLIntegrationTest {

  private InetAddress localhost;
  private int port;
  private FakeTcpServer server;
  private TcpClient client;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setup() {
    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
  }

  private void startServerAndClient(CertificateBuilder serverCertificate,
      CertificateBuilder clientCertificate)
      throws GeneralSecurityException, IOException {

    CertStores serverStore = CertStores.locatorStore();
    serverStore.withCertificate(serverCertificate);

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate(clientCertificate);

    Properties serverProperties = serverStore
        .trustSelf()
        .trust(clientStore.alias(), clientStore.certificate())
        .propertiesWith(LOCATOR, true, true);

    Properties clientProperties = clientStore
        .trust(serverStore.alias(), serverStore.certificate())
        .propertiesWith(LOCATOR, true, true);

    startTcpServer(serverProperties);

    client = new TcpClient(clientProperties);
  }

  @After
  public void teardown() {
    SocketCreatorFactory.close();
  }

  private void startTcpServer(Properties sslProperties) throws IOException {
    localhost = InetAddress.getLocalHost();
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    server = new FakeTcpServer(port, localhost, sslProperties, null,
        Mockito.mock(TcpHandler.class), Mockito.mock(PoolStatHelper.class),
        Thread.currentThread().getThreadGroup(), "server thread");
    server.start();
  }

  @Test
  public void clientConnectsIfServerCertificateHasHostname() throws Exception {
    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("tcp-server")
        .sanDnsName(InetAddress.getLocalHost().getHostName());

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("tcp-client");

    startServerAndClient(serverCertificate, clientCertificate);
  }

  @Test
  public void clientFailsToConnectIfServerCertificateNoHostname() throws Exception {
    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("tcp-server");

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("tcp-client");

    startServerAndClient(serverCertificate, clientCertificate);

    assertThatExceptionOfType(LocatorCancelException.class)
        .isThrownBy(() -> client.requestToServer(localhost, port, Boolean.valueOf(false), 5 * 1000))
        .withCause(
            new SSLHandshakeException("java.security.cert.CertificateException: No name matching "
                + localhost.getHostName() + " found"));
  }

  @Test
  public void clientFailsToConnectIfServerCertificateWrongHostname() throws Exception {
    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("tcp-server")
        .sanDnsName("example.com");

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("tcp-client");

    startServerAndClient(serverCertificate, clientCertificate);

    assertThatExceptionOfType(LocatorCancelException.class)
        .isThrownBy(() -> client.requestToServer(localhost, port, Boolean.valueOf(false), 5 * 1000))
        .withCause(new SSLHandshakeException(
            "java.security.cert.CertificateException: No subject alternative DNS name matching "
                + localhost.getHostName() + " found."));
  }

  private class FakeTcpServer extends TcpServer {
    private DistributionConfig distributionConfig;

    public FakeTcpServer(int port, InetAddress bind_address, Properties sslConfig,
        DistributionConfigImpl cfg, TcpHandler handler, PoolStatHelper poolHelper,
        ThreadGroup threadGroup, String threadName) {
      super(port, bind_address, sslConfig, cfg, handler, poolHelper, threadGroup, threadName, null,
          null);
      if (cfg == null) {
        cfg = new DistributionConfigImpl(sslConfig);
      }
      this.distributionConfig = cfg;
    }

    @Override
    protected SocketCreator getSocketCreator() {
      if (this.socketCreator == null) {
        SSLConfigurationFactory.setDistributionConfig(distributionConfig);
        SSLConfig sslConfig =
            SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.LOCATOR);
        this.socketCreator = new SocketCreator(sslConfig);
      }
      return socketCreator;
    }
  }
}
