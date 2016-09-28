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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.distributed.internal.DistributionConfig.*;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.*;
import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ClientSocketFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for ClientSocketFactory.
 *
 * <p>Extracted from {@code JSSESocketJUnitTest}.
 */
@Category(IntegrationTest.class)
public class ClientSocketFactoryIntegrationTest {

  private static final String EXCEPTION_MESSAGE = "TSocketFactory createSocket threw an IOException";

  private static volatile boolean invokedCreateSocket;

  private Socket socket;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty(GEMFIRE_PREFIX + "clientSocketFactory", TSocketFactory.class.getName());

    Properties properties = new Properties();
    properties.setProperty(CLUSTER_SSL_ENABLED, "false");
    DistributionConfig distributionConfig = new DistributionConfigImpl(properties);

    SocketCreatorFactory.setDistributionConfig(distributionConfig);
  }

  @After
  public void tearDown() throws Exception {
    if (this.socket != null) {
      this.socket.close();
    }

    System.clearProperty(GEMFIRE_PREFIX + "clientSocketFactory");
    SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER).initializeClientSocketFactory();

    invokedCreateSocket = false;
  }

  @Test
  public void testClientSocketFactory() throws Exception {
    assertThatThrownBy(() -> this.socket = SocketCreatorFactory.getSocketCreatorForComponent(CLUSTER).connectForClient("localhost", 12345, 0))
      .isExactlyInstanceOf(IOException.class)
      .hasMessage(EXCEPTION_MESSAGE);

    assertThat(invokedCreateSocket).isTrue();
  }

  private static class TSocketFactory implements ClientSocketFactory {

    public TSocketFactory() {
    }

    public Socket createSocket(final InetAddress address, final int port) throws IOException {
      invokedCreateSocket = true;
      throw new IOException(EXCEPTION_MESSAGE);
    }
  }
}
