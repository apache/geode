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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.net.SocketCreatorFailHandshake;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({MembershipTest.class})
public class TCPServerSSLJUnitTest {

  Logger logger = LogService.getLogger();

  private InetAddress localhost;
  private int port;
  private TcpServer server;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  private final String expectedSocketTimeout = "2000";
  private ArrayList<Integer> recordedSocketTimeouts = new ArrayList<>();
  private SocketCreatorFailHandshake socketCreator;

  @Before
  public void setup() throws IOException {

    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));

    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.READ_TIMEOUT",
        expectedSocketTimeout);

    localhost = InetAddress.getLocalHost();

    socketCreator = new SocketCreatorFailHandshake(
        SSLConfigurationFactory.getSSLConfigForComponent(
            new DistributionConfigImpl(getSSLConfigurationProperties()),
            SecurableCommunicationChannel.LOCATOR),
        recordedSocketTimeouts);

    server = new TcpServer(
        0,
        localhost,
        Mockito.mock(TcpHandler.class),
        "server thread",
        (socket, input, firstByte) -> false, DistributionStats::getStatTime,
        Executors::newCachedThreadPool,
        socketCreator,
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.READ_TIMEOUT",
        GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.BACKLOG");

    server.start();
    port = server.getPort();
  }

  @After
  public void teardown() throws ConnectException, InterruptedException {

    /*
     * Initially, the DummySocketCreator was set to fail the TLS handshake. In order to shut
     * the TcpServer down, we have to communicate with it so the handshake has to succeed.
     */
    socketCreator.setFailTLSHandshake(false);

    getTcpClient().stop(new HostAndPort(localhost.getHostAddress(), port));

    server.join(60 * 1000);

    SocketCreatorFactory.close();
  }

  @Test
  public void testSSLSocketTimeOut() throws IOException, ClassNotFoundException {

    try {

      getTcpClient().requestToServer(new HostAndPort(localhost.getHostAddress(), port),
          Boolean.valueOf(false), 5 * 1000);
      throw new AssertionError("expected to get an exception but didn't");

    } catch (final IllegalStateException | IOException t) {
      // ok!
    }

    assertThat(recordedSocketTimeouts).hasSizeGreaterThan(0);

    for (final Integer socketTimeOut : recordedSocketTimeouts) {
      assertThat(Integer.parseInt(expectedSocketTimeout)).isEqualTo(socketTimeOut.intValue());
    }
  }

  private Properties getSSLConfigurationProperties() {
    Properties sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED_COMPONENTS,
        SecurableCommunicationChannel.LOCATOR.getConstant());
    sslProperties.setProperty(SSL_KEYSTORE,
        createTempFileFromResource(getClass(), "/org/apache/geode/internal/net/multiKey.jks")
            .getAbsolutePath());
    sslProperties.setProperty(SSL_TRUSTSTORE,
        createTempFileFromResource(getClass(),
            "/org/apache/geode/internal/net/multiKeyTrust.jks").getAbsolutePath());
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    sslProperties.setProperty(SSL_CIPHERS, "any");
    sslProperties.setProperty(SSL_PROTOCOLS, "any");
    return sslProperties;
  }

  private TcpClient getTcpClient() {
    return new TcpClient(SocketCreatorFactory
        .getSocketCreatorForComponent(
            new DistributionConfigImpl(getSSLConfigurationProperties()),
            SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
  }

}
