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
package org.apache.geode.internal.cache.wan;

import static com.palantir.docker.compose.execution.DockerComposeExecArgument.arguments;
import static com.palantir.docker.compose.execution.DockerComposeExecOption.options;
import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;

import com.palantir.docker.compose.DockerComposeRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;


/**
 * These tests use two Geode sites:
 *
 * - One site (the remote one) consisting of a 2-server, 1-locator Geode cluster.
 * The servers host a partition region (region-wan) and have gateway senders to receive events
 * from the other site with the same value for hostname-for-senders and listening on the
 * same port (2324).
 * The servers and locator run each inside a Docker container and are not route-able
 * from the host (where this JUnit test is running).
 * Another Docker container is running the HAProxy image and it's set up as a TCP load balancer.
 * The other site connects to the locator and to the gateway receivers via the
 * TCP load balancer that forwards traffic directed to the 20334 port to the locator and
 * traffic directed to the 2324 port to the receivers in a round robin fashion.
 *
 * - Another site consisting of a 1-server, 1-locator Geode cluster.
 * The server hosts a partition region (region-wan) and has a parallel gateway receiver
 * to send events to the remote site.
 *
 * The aim of the tests is verify that when several gateway receivers in a remote site
 * share the same port and hostname-for-senders, the pings sent from the gateway senders
 * reach the right gateway receiver and not just any of the receivers. Failure to do this
 * may result in the closing of connections by a gateway receiver for not having
 * received the ping in time.
 */
@Category({WanTest.class})
public class SeveralGatewayReceiversWithSamePortAndHostnameForSenders extends WANTestBase {

  private static final URL DOCKER_COMPOSE_PATH =
      SeveralGatewayReceiversWithSamePortAndHostnameForSenders.class
          .getResource("docker-compose.yml");

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static NotOnWindowsDockerRule docker =
      new NotOnWindowsDockerRule(() -> DockerComposeRule.builder()
          .file(DOCKER_COMPOSE_PATH.getPath()).build());

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Start locator
    docker.get().exec(options("-T"), "locator",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter-locator.gfsh"));
    // Start server1
    docker.get().exec(options("-T"), "server1",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter-server1.gfsh"));
    // Start server2
    docker.get().exec(options("-T"), "server2",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter-server2.gfsh"));
    // Create partition region and gateway receiver
    docker.get().exec(options("-T"), "locator",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter-create.gfsh"));
  }

  public SeveralGatewayReceiversWithSamePortAndHostnameForSenders() {
    super();
  }

  @Test
  public void testPingsToReceiversWithSamePortAndHostnameForSendersReachTheRightReceivers() {
    String senderId = "ln";
    String regionName = "region-wan";
    final int remoteLocPort = 20334;

    int locPort = vm0.invoke(() -> WANTestBase.createLocatorWithDSIdAndRemoteLoc(1, remoteLocPort));

    createCacheInVMs(locPort, vm1);

    // Use must use more than one dispatcher thread. With just one dispatcher thread, only one
    // connection will be created by the sender towards one of the receivers and it will be
    // monitored by the ping thread.
    // With more than one thread, several connections will be opened and there should be one ping
    // thread per remote receiver.
    vm1.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers("ln", 2, true, 100, 5, false,
        false, null, true, 5, GatewaySender.DEFAULT_ORDER_POLICY));

    vm1.invoke(
        () -> WANTestBase.createPartitionedRegion(regionName, senderId, 0, 10, isOffHeap()));

    startSenderInVMs(senderId, vm1);

    int NUM_PUTS = 10;

    putKeyValues(NUM_PUTS, regionName, vm1);

    // Wait longer than the value set in the receivers for
    // maximum-time-between-pings (60000) to verify that connections are not closed
    // by the receivers because each has received the pings timely.
    int maxTimeBetweenPingsInReceiver = 65000;
    try {
      Thread.sleep(maxTimeBetweenPingsInReceiver);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    int senderPoolDisconnects =
        vm1.invoke(() -> WANTestBase.getGatewaySenderPoolDisconnects(senderId));
    assertEquals(0, senderPoolDisconnects);
  }

  protected void putKeyValues(int numPuts, String region, VM vm) {
    final HashMap<Integer, Integer> keyValues = new HashMap<>();
    for (int i = 0; i < numPuts; i++) {
      keyValues.put(i, i);
    }

    vm.invoke(() -> WANTestBase.putGivenKeyValue(region, keyValues));
  }
}
