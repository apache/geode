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
import static org.apache.geode.distributed.internal.membership.adapter.SocketCreatorAdapter.asTcpSocketCreator;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.RestartableTcpHandler;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.tier.sockets.TcpServerFactory;
import org.apache.geode.internal.net.DummySocketCreator;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class TCPServerSSLJUnitTest {

  private InetAddress localhost;
  private int port;
  private DummyTcpServer server;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  private final String expectedSocketTimeout = "2000";

  @Before
  public void setup() {
    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
  }

  @After
  public void teardown() {
    SocketCreatorFactory.close();
  }

  private void startTimeDelayedTcpServer(Properties sslProperties,
      final ArrayList<Integer> recordedSocketTimeouts) throws IOException {
    localhost = InetAddress.getLocalHost();
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    server = new DummyTcpServer(port, localhost, sslProperties, null,
        Mockito.mock(RestartableTcpHandler.class), Mockito.mock(PoolStatHelper.class),
        "server thread", recordedSocketTimeouts);
    server.start();
  }

  @Test
  public void testSSLSocketTimeOut() throws IOException {

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "TcpServer.READ_TIMEOUT",
        expectedSocketTimeout);
    Properties sslProperties = getSSLConfigurationProperties();
    final ArrayList<Integer> recordedSocketTimeouts = new ArrayList<>();

    try {
      startTimeDelayedTcpServer(sslProperties, recordedSocketTimeouts);
      createTcpClientConnection(sslProperties);
    } catch (IllegalStateException e) {
      // connection will fail; Expected to have the exception thrown
    }

    Assert.assertEquals(1, recordedSocketTimeouts.size());
    for (Integer socketTimeOut : recordedSocketTimeouts) {
      Assert.assertEquals(Integer.parseInt(expectedSocketTimeout), socketTimeOut.intValue());
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

  private void createTcpClientConnection(final Properties clientProperties) {
    try {
      new TcpClient(
          asTcpSocketCreator(
              SocketCreatorFactory
                  .getSocketCreatorForComponent(
                      new DistributionConfigImpl(clientProperties),
                      SecurableCommunicationChannel.LOCATOR)))
                          .requestToServer(localhost, port, Boolean.valueOf(false), 5 * 1000);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  private static class DummyTcpServer extends TcpServer {
    DummyTcpServer(int port, InetAddress bind_address, Properties sslConfig,
        DistributionConfigImpl cfg, RestartableTcpHandler handler, PoolStatHelper poolHelper,
        String threadName, final List<Integer> recordedSocketTimeouts) {
      super(port, bind_address, sslConfig, cfg, handler, threadName,
          (socket, input, firstByte) -> false, DistributionStats::getStatTime,
          TcpServerFactory.createExecutorServiceSupplier(poolHelper),
          getSocketCreator(getDistributionConfig(sslConfig, cfg), recordedSocketTimeouts));
    }

    private static DistributionConfigImpl getDistributionConfig(
        final Properties sslConfig,
        final DistributionConfigImpl distributionConfig) {
      return distributionConfig == null ? new DistributionConfigImpl(sslConfig)
          : distributionConfig;
    }

    private static TcpSocketCreator getSocketCreator(
        final DistributionConfig distributionConfig,
        final List<Integer> recordedSocketsTimeouts) {
      final SSLConfig sslConfig =
          SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
              SecurableCommunicationChannel.LOCATOR);
      return asTcpSocketCreator(new DummySocketCreator(sslConfig, recordedSocketsTimeouts));
    }

  }
}
