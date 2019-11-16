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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.internal.membership.adapter.SocketCreatorAdapter.asTcpSocketCreator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.version.VersionManager;

/**
 * This tests the rolling upgrade for locators with different GOSSIPVERSION.
 */
@Category({MembershipTest.class})
public class TcpServerBackwardCompatDUnitTest extends JUnit4DistributedTestCase {

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    Invoke.invokeInEveryVM("Set TcpServer.isTesting true", () -> {
      TcpServer.isTesting = true;
    });
  }

  @Override
  public final void preTearDown() throws Exception {
    Invoke.invokeInEveryVM("Set TcpServer.isTesting true", () -> {
      TcpServer.isTesting = false;
    });
  }

  /**
   * This test starts two locators with current GOSSIPVERSION and then shuts down one of them and
   * restart it with new GOSSIPVERSION and verifies that it has recovered the system View. Then we
   * upgrade next locator.
   */
  @Test
  public void testGossipVersionBackwardCompatibility() {

    final VM locator0 = VM.getVM(0);
    final VM locator1 = VM.getVM(1);
    final VM locatorRestart0 = VM.getVM(2);
    final VM member = VM.getVM(3);

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    // Create properties for locator0
    final int port0 = ports[0];
    final File logFile0 = null;

    // Create properties for locator1
    final int port1 = ports[1];
    final File logFile1 = null;

    final String locators =
        VM.getHostName() + "[" + port0 + "]," + VM.getHostName() + "[" + port1 + "]";

    final Properties props = new Properties();
    props.setProperty(LOCATORS, locators);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    locator0.invoke("Starting first locator on port " + port0, () -> {
      try {
        TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION - 100,
            VersionManager.getInstance().getCurrentVersionOrdinal());

        Locator.startLocatorAndDS(port0, logFile0, props);
      } catch (IOException e) {
        fail("Locator1 start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
      }
    });

    // Start a new member to add it to discovery set of locator0.
    member.invoke("Start a member", () -> {
      disconnectFromDS();
      TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION - 100,
          VersionManager.getInstance().getCurrentVersionOrdinal());
      InternalDistributedSystem.connect(props);
    });

    // Start locator1 with props.
    locator1.invoke("Starting second locator on port " + port1,
        () -> restartLocator(port1, logFile1, props));

    // Stop first locator currently running in locator0 VM.
    locator0.invoke("Stopping first locator", () -> {
      Locator.getLocator().stop();
      disconnectFromDS();
    });

    // Restart first locator in new VM.
    locatorRestart0.invoke(() -> restartLocator(port0, logFile0, props));
  }

  private void restartLocator(int port0, File logFile0, Properties props) {
    try {
      TcpServer.TESTVERSION -= 100;
      TcpServer.OLDTESTVERSION -= 100;
      TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION,
          VersionManager.getInstance().getCurrentVersionOrdinal());
      TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.OLDTESTVERSION,
          Version.GFE_57.ordinal());

      Locator.startLocatorAndDS(port0, logFile0, props);

      // Start a gossip client to connect to first locator "locator0".
      FindCoordinatorRequest req = new FindCoordinatorRequest(
          new InternalDistributedMember("localhost", 1234));
      FindCoordinatorResponse response;

      response = (FindCoordinatorResponse) new TcpClient(
          asTcpSocketCreator(
              SocketCreatorFactory
                  .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR)),
          InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
          InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer())
              .requestToServer(SocketCreator.getLocalHost(), port0, req, 5000);
      assertThat(response).isNotNull();

    } catch (IllegalStateException e) {
      fail("a Locator start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
    } catch (Exception e) {
      fail("b Locator start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
    }
  }
}
