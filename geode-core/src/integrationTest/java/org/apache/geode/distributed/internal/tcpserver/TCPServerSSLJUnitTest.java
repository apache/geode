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
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.DummySocketCreator;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.test.TestUtil;

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

  private void startTimeDelayedTcpServer(Properties sslProperties) throws IOException {
    localhost = InetAddress.getLocalHost();
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    server = new DummyTcpServer(port, localhost, sslProperties, null,
        Mockito.mock(TcpHandler.class), Mockito.mock(PoolStatHelper.class),
        "server thread");
    server.start();
  }

  @Test
  public void testSSLSocketTimeOut() throws IOException {
    try {

      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "TcpServer.READ_TIMEOUT",
          expectedSocketTimeout);
      Properties sslProperties = getSSLConfigurationProperties();
      startTimeDelayedTcpServer(sslProperties);

      createTcpClientConnection();

    } catch (LocatorCancelException e) {
      // we catching the LocatorCancelException. Expected to have the exception thrown
    }

    List<Integer> recordedSocketsForSocketCreator = server.getRecordedSocketTimeouts();

    Assert.assertEquals(1, recordedSocketsForSocketCreator.size());
    for (Integer socketTimeOut : recordedSocketsForSocketCreator) {
      Assert.assertEquals(Integer.parseInt(expectedSocketTimeout), socketTimeOut.intValue());
    }
  }

  private Properties getSSLConfigurationProperties() {
    Properties sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED_COMPONENTS,
        SecurableCommunicationChannel.LOCATOR.getConstant());
    sslProperties.setProperty(SSL_KEYSTORE,
        TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks"));
    sslProperties.setProperty(SSL_TRUSTSTORE,
        TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks"));
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    sslProperties.setProperty(SSL_CIPHERS, "any");
    sslProperties.setProperty(SSL_PROTOCOLS, "any");
    return sslProperties;
  }

  private void createTcpClientConnection() {
    try {
      new TcpClient().requestToServer(localhost, port, Boolean.valueOf(false), 5 * 1000);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  private class DummyTcpServer extends TcpServer {
    private DistributionConfig distributionConfig;
    private List<Integer> recordedSocketsTimeouts = new ArrayList<>();

    public DummyTcpServer(int port, InetAddress bind_address, Properties sslConfig,
        DistributionConfigImpl cfg, TcpHandler handler, PoolStatHelper poolHelper,
        String threadName) {
      super(port, bind_address, sslConfig, cfg, handler, poolHelper, threadName, null, null);
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
        this.socketCreator = new DummySocketCreator(sslConfig, recordedSocketsTimeouts);
      }
      return socketCreator;
    }

    public List<Integer> getRecordedSocketTimeouts() {
      return recordedSocketsTimeouts;
    }
  }
}
