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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLHandshakeException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class TCPClientSSLIntegrationTest {

  private InetAddress localhost;
  private int port;
  private TcpServer server;
  private TcpClient client;
  private CertificateMaterial ca;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setup() {
    SocketCreatorFactory.close(); // de-initialize fully
    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));

    ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();
  }

  @After
  public void after() {
    SocketCreatorFactory.close();
  }

  private void startServerAndClient(CertificateMaterial serverCertificate,
      CertificateMaterial clientCertificate, boolean enableHostNameValidation)
      throws GeneralSecurityException, IOException {

    CertStores serverStore = CertStores.locatorStore();
    serverStore.withCertificate("server", serverCertificate);
    serverStore.trust("ca", ca);

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate("client", clientCertificate);
    clientStore.trust("ca", ca);

    Properties serverProperties = serverStore
        .propertiesWith(LOCATOR, true, enableHostNameValidation);

    Properties clientProperties = clientStore
        .propertiesWith(LOCATOR, true, enableHostNameValidation);

    startTcpServer(serverProperties);

    client = new TcpClient(new SocketCreator(
        SSLConfigurationFactory.getSSLConfigForComponent(clientProperties,
            SecurableCommunicationChannel.LOCATOR)),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
  }

  private void startTcpServer(Properties sslProperties) throws IOException {
    localhost = InetAddress.getLocalHost();
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    TcpHandler tcpHandler = Mockito.mock(TcpHandler.class);
    when(tcpHandler.processRequest(any())).thenReturn("Running!");

    server = new TcpServer(
        port,
        localhost,
        tcpHandler,
        "server thread",
        (socket, input, firstByte) -> false,
        DistributionStats::getStatTime,
        Executors::newCachedThreadPool,
        new SocketCreator(
            SSLConfigurationFactory.getSSLConfigForComponent(
                new DistributionConfigImpl(sslProperties),
                SecurableCommunicationChannel.LOCATOR)),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        "not-a-system-property",
        "not-a-system-property");

    server.start();
  }

  @Test
  public void clientConnectsIfServerCertificateHasHostname() throws Exception {
    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("tcp-server")
        .issuedBy(ca)
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("tcp-client")
        .issuedBy(ca)
        .generate();

    startServerAndClient(serverCertificate, clientCertificate, true);
    String response =
        (String) client.requestToServer(new HostAndPort(localhost.getHostName(), port),
            Boolean.valueOf(false), 5 * 1000);
    assertThat(response).isEqualTo("Running!");
  }

  @Test
  public void clientChooseToDisableHasHostnameValidation() throws Exception {
    // no host name in server cert
    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("tcp-server")
        .issuedBy(ca)
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("tcp-client")
        .issuedBy(ca)
        .generate();

    startServerAndClient(serverCertificate, clientCertificate, false);
    String response =
        (String) client.requestToServer(new HostAndPort(localhost.getHostName(), port),
            Boolean.valueOf(false), 5 * 1000);
    assertThat(response).isEqualTo("Running!");
  }

  @Test
  public void clientFailsToConnectIfServerCertificateNoHostname() throws Exception {
    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("tcp-server")
        .issuedBy(ca)
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("tcp-client")
        .issuedBy(ca)
        .generate();

    startServerAndClient(serverCertificate, clientCertificate, true);

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> client.requestToServer(new HostAndPort(localhost.getHostName(), port),
            Boolean.valueOf(false), 5 * 1000))
        .withCauseInstanceOf(SSLHandshakeException.class)
        .withStackTraceContaining("No name matching " + localhost.getHostName() + " found");
  }

  @Test
  public void clientFailsToConnectIfServerCertificateWrongHostname() throws Exception {
    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("tcp-server")
        .issuedBy(ca)
        .sanDnsName("example.com")
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("tcp-client")
        .issuedBy(ca)
        .generate();

    startServerAndClient(serverCertificate, clientCertificate, true);

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> client.requestToServer(new HostAndPort(localhost.getHostName(), port),
            Boolean.valueOf(false), 5 * 1000))
        .withCauseInstanceOf(SSLHandshakeException.class)
        .withStackTraceContaining("No subject alternative DNS name matching "
            + localhost.getHostName() + " found.");
  }

}
