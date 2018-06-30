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
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * This tests the rolling upgrade for locators with different GOSSIPVERSION.
 */
@Category({DistributedTest.class, MembershipTest.class})
public class TcpServerBackwardCompatDUnitTest extends JUnit4DistributedTestCase {

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    Invoke.invokeInEveryVM(new CacheSerializableRunnable("Set TcpServer.isTesting true") {

      @Override
      public void run2() throws CacheException {
        TcpServer.isTesting = true;
      }
    });
  }

  @Override
  public final void preTearDown() throws Exception {
    Invoke.invokeInEveryVM(new CacheSerializableRunnable("Set TcpServer.isTesting true") {

      @Override
      public void run2() throws CacheException {
        TcpServer.isTesting = false;
      }
    });
  }

  /**
   * This test starts two locators with current GOSSIPVERSION and then shuts down one of them and
   * restart it with new GOSSIPVERSION and verifies that it has recoverd the system View. Then we
   * upgrade next locator.
   */
  @Test
  public void testGossipVersionBackwardCompatibility() {
    Host host = Host.getHost(0);
    final VM locator0 = host.getVM(0);
    final VM locator1 = host.getVM(1);
    final VM locatorRestart0 = host.getVM(2);
    final VM member = host.getVM(3);

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    // Create properties for locator0
    final int port0 = ports[0];
    final File logFile0 = null;// new File("");

    // Create properties for locator1
    final int port1 = ports[1];
    final File logFile1 = null;// new File("");

    final String locators =
        host.getHostName() + "[" + port0 + "]," + host.getHostName() + "[" + port1 + "]";

    final Properties props = new Properties();
    props.setProperty(LOCATORS, locators);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    // props.setProperty(LOG_LEVEL, "finest");

    // Start locator0 with props.
    // props.setProperty(DistributionConfig.START_LOCATOR_NAME, host.getHostName() + "["+port0+"]");
    locator0.invoke(new CacheSerializableRunnable("Starting first locator on port " + port0) {

      @Override
      public void run2() throws CacheException {
        try {
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION - 100,
              Version.CURRENT_ORDINAL);

          Locator.startLocatorAndDS(port0, logFile0, props);
        } catch (IOException e) {
          org.apache.geode.test.dunit.Assert.fail(
              "Locator1 start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
        }
      }
    });

    // Start a new member to add it to discovery set of locator0.
    member.invoke(new CacheSerializableRunnable("Start a member") {

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION - 100,
            Version.CURRENT_ORDINAL);
        InternalDistributedSystem.connect(props);
      }
    });

    // Start locator1 with props.
    // props.setProperty(DistributionConfig.START_LOCATOR_NAME, host.getHostName() + "["+port1+"]");
    locator1.invoke(new CacheSerializableRunnable("Starting second locator on port " + port1) {

      @Override
      public void run2() throws CacheException {
        try {
          TcpServer.TESTVERSION -= 100;
          TcpServer.OLDTESTVERSION -= 100;
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION,
              Version.CURRENT_ORDINAL);
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.OLDTESTVERSION,
              Version.GFE_57.ordinal());
          // assertIndexDetailsEquals("Gossip Version and Test version are not same",
          // TcpServer.GOSSIPVERSION, TcpServer.TESTVERSION);
          // assertIndexDetailsEquals("Previous Gossip Version and Test version are not same",
          // TcpServer.OLDGOSSIPVERSION, TcpServer.OLDTESTVERSION);

          Locator.startLocatorAndDS(port1, logFile1, props);

          // Start a gossip client to connect to first locator "locator0".
          FindCoordinatorRequest req = new FindCoordinatorRequest(
              new InternalDistributedMember(SocketCreator.getLocalHost(), 1234));
          FindCoordinatorResponse response = null;

          response = (FindCoordinatorResponse) new TcpClient()
              .requestToServer(SocketCreator.getLocalHost(), port1, req, 5000);
          assertNotNull(response);

        } catch (Exception e) {
          org.apache.geode.test.dunit.Assert.fail(
              "Locator1 start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
        }
      }
    });

    // Stop first locator currently running in locator0 VM.
    locator0.invoke(new CacheSerializableRunnable("Stopping first locator") {

      @Override
      public void run2() throws CacheException {
        Locator.getLocator().stop();
        disconnectFromDS();
      }
    });

    // Restart first locator in new VM.
    // props.setProperty(DistributionConfig.START_LOCATOR_NAME, host.getHostName() + "["+port0+"]");
    locatorRestart0
        .invoke(new CacheSerializableRunnable("Restarting first locator on port " + port0) {

          @Override
          public void run2() throws CacheException {
            try {
              TcpServer.TESTVERSION -= 100;
              TcpServer.OLDTESTVERSION -= 100;
              TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION,
                  Version.CURRENT_ORDINAL);
              TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.OLDTESTVERSION,
                  Version.GFE_57.ordinal());
              // assertIndexDetailsEquals("Gossip Version and Test version are not same",
              // TcpServer.GOSSIPVERSION, TcpServer.TESTVERSION);
              // assertIndexDetailsEquals("Previous Gossip Version and Test version are not same",
              // TcpServer.OLDGOSSIPVERSION, TcpServer.OLDTESTVERSION);

              Locator.startLocatorAndDS(port0, logFile0, props);

              // Start a gossip client to connect to first locator "locator0".
              FindCoordinatorRequest req = new FindCoordinatorRequest(
                  new InternalDistributedMember(SocketCreator.getLocalHost(), 1234));
              FindCoordinatorResponse response = null;

              response = (FindCoordinatorResponse) new TcpClient()
                  .requestToServer(SocketCreator.getLocalHost(), port0, req, 5000);
              assertNotNull(response);

            } catch (Exception e) {
              org.apache.geode.test.dunit.Assert.fail(
                  "Locator0 start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
            }
          }
        });
  }
}
