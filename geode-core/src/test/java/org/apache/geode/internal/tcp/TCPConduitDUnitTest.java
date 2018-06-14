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
package org.apache.geode.internal.tcp;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.util.test.TestUtil;


@Category(DistributedTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class TCPConduitDUnitTest extends DistributedTestCase {
  public static final String TEST_REGION = "testRegion";
  private final Properties properties;

  @Parameterized.Parameters
  public static Collection<Properties> data() {
    Properties nonSSL = new Properties();
    nonSSL.setProperty(ConfigurationProperties.SOCKET_LEASE_TIME, "1000");
    nonSSL.setProperty(ConfigurationProperties.DISABLE_AUTO_RECONNECT, "true");

    Properties SSL = new Properties();
    SSL.putAll(nonSSL);

    final String keystorePath = TestUtil.getResourcePath(TCPConduitDUnitTest.class,
        "/org/apache/geode/cache/client/internal/default.keystore");

    SSL.setProperty(ConfigurationProperties.SSL_ENABLED_COMPONENTS, "cluster");
    SSL.setProperty(ConfigurationProperties.SSL_KEYSTORE, keystorePath);
    SSL.setProperty(ConfigurationProperties.SSL_KEYSTORE_PASSWORD, "password");
    SSL.setProperty(ConfigurationProperties.SSL_TRUSTSTORE, keystorePath);
    SSL.setProperty(ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD, "password");

    return Arrays.asList(nonSSL, SSL);
  }

  public TCPConduitDUnitTest(Properties properties) {
    this.properties = properties;
  }

  /**
   * This was a bug where SSL connections were being configured with a timeout that was non-zero,
   * but didn't handle socket timeouts. This resulted in a permanently hung P2P socket.
   *
   * We set the lease time to 1000, which in that case would cause the socket to break in 1 second.
   */
  @Test
  public void basicAcceptConnection() throws Exception {
    final VM vm1 = VM.getVM(1);
    final VM vm2 = VM.getVM(2);
    final VM vm3 = VM.getVM(3);

    disconnectAllFromDS();

    int port = startLocator();
    properties.put(ConfigurationProperties.LOCATORS, "localhost[" + port + "]");

    vm1.invoke(() -> startServer(properties));
    vm2.invoke(() -> startServer(properties));
    vm3.invoke(() -> startServer(properties));

    Thread.sleep(5000);

    try {
      Awaitility.await("for message to be sent").atMost(10, TimeUnit.SECONDS).until(() -> {
        final SerialAckedMessage serialAckedMessage = new SerialAckedMessage();
        serialAckedMessage.send(system.getAllOtherMembers(), false);
        return true;
      });
    } finally {
      // DUnit won't clean up properly if the sockets are hung; we have to crash the systems.
      IgnoredException.addIgnoredException("ForcedDisconnectException|loss of quorum");
      for (VM vm : Arrays.asList(vm1, vm2, vm3)) {
        vm.invoke("crash system in case it's hung", () -> {
          if (system != null && system.isConnected()) {
            DistributedTestUtils.crashDistributedSystem(system);
          }
        });
      }
      DistributedTestUtils.crashDistributedSystem(system);
    }
  }

  private int startLocator() throws Exception {
    Locator locator = Locator.startLocatorAndDS(0, new File(""), properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();
    return locator.getPort();
  }

  private void startServer(Properties properties) throws IOException {
    system = (InternalDistributedSystem) DistributedSystem.connect(properties);
  }
}
