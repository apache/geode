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

package org.apache.geode.redis;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__DIR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__J;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__PORT;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__DIR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__INITIAL_HEAP;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__J;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__LOCATORS;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__MAXHEAP;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__SERVER_PORT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_OOM_COMMAND_NOT_ALLOWED;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.JedisClusterCRC16;

import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class OutOfMemoryDUnitTest {

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final long KEY_TTL_SECONDS = 10;
  private static final int MAX_ITERATION_COUNT = 4000;
  private static final int LARGE_VALUE_SIZE = 1024 * 1024;
  private static final String LARGE_VALUE = StringUtils.leftPad("a", LARGE_VALUE_SIZE);
  private static final String KEY = "key";

  private static String locatorPort;
  private static JedisCluster jedis;
  private static String server1Tag;
  private static String server2Tag;
  private static int[] redisServerPorts;

  private final AtomicInteger numberOfKeys = new AtomicInteger(0);

  @BeforeClass
  public static void setUpClass() throws Exception {
    IgnoredException.addIgnoredException("Member: .*? above .*? critical threshold");
    IgnoredException.addIgnoredException("LowMemoryException");

    int[] locatorPorts = getRandomAvailableTCPPorts(3);

    locatorPort = String.valueOf(locatorPorts[0]);
    int httpPort = locatorPorts[1];
    int rmiPort = locatorPorts[2];

    CommandStringBuilder startLocatorCommand = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__DIR, temporaryFolder.newFolder().getAbsolutePath())
        .addOption(START_LOCATOR__PORT, locatorPort)
        .addOption(START_LOCATOR__J, "-Dgemfire.jmx-manager=true")
        .addOption(START_LOCATOR__J, "-Dgemfire.jmx-manager-start=true")
        .addOption(START_LOCATOR__J, "-Dgemfire.jmx-manager-http-port=" + httpPort)
        .addOption(START_LOCATOR__J, "-Dgemfire.jmx-manager-port=" + rmiPort);

    gfsh.executeAndAssertThat(startLocatorCommand.getCommandString()).statusIsSuccess();
    redisServerPorts = getRandomAvailableTCPPorts(2);

    startServer(redisServerPorts[0]);
    startServer(redisServerPorts[1]);

    List<ClusterNode> nodes;
    try (Jedis singleNodeJedis =
        new Jedis(BIND_ADDRESS, redisServerPorts[0], REDIS_CLIENT_TIMEOUT)) {
      nodes = ClusterNodes.parseClusterNodes(singleNodeJedis.clusterNodes()).getNodes();
    }

    server1Tag = getHashtagForServerWithRedisPort(nodes, redisServerPorts[0]);
    server2Tag = getHashtagForServerWithRedisPort(nodes, redisServerPorts[1]);

    jedis =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPorts[0]), REDIS_CLIENT_TIMEOUT);
  }

  private static void startServer(int redisPort) throws Exception {
    CommandStringBuilder startServerCommand = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__DIR, temporaryFolder.newFolder().getAbsolutePath())
        .addOption(START_SERVER__SERVER_PORT, "0")
        .addOption(START_SERVER__LOCATORS, "localhost[" + locatorPort + "]")
        .addOption(START_SERVER__J, "-Dgemfire.geode-for-redis-enabled=true")
        .addOption(START_SERVER__J, "-Dgemfire.geode-for-redis-bind-address=" + BIND_ADDRESS)
        .addOption(START_SERVER__J, "-Dgemfire.geode-for-redis-port=" + redisPort)
        .addOption(START_SERVER__INITIAL_HEAP, "125m")
        .addOption(START_SERVER__MAXHEAP, "125m")
        .addOption(START_SERVER__CRITICAL__HEAP__PERCENTAGE, "50")
        .addOption(START_SERVER__J, "-XX:CMSInitiatingOccupancyFraction=45");
    gfsh.executeAndAssertThat(startServerCommand.getCommandString()).statusIsSuccess();
  }

  private static String getHashtagForServerWithRedisPort(List<ClusterNode> nodes, int redisPort) {
    // Find the node with the port that we're interested in
    ClusterNode serverNode = nodes
        .stream()
        .filter(node -> node.port == redisPort)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "Could not find a server with the provided redis port"));

    // Find a key that maps to a slot on that node
    String key = "";
    for (int i = 0; i < 1000; ++i) {
      key = "tag" + i;
      int slot = JedisClusterCRC16.getSlot(key);
      if (serverNode.isSlotOnNode(slot)) {
        break;
      }
    }
    return "{" + key + "}";
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    jedis.close();
    gfsh.connectAndVerify(Integer.parseInt(locatorPort), GfshCommandRule.PortType.locator);
    gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
  }

  @After
  public void tearDown() throws Exception {
    removeAllKeysAndForceGC();
  }

  @Test
  public void shouldReturnOOMError_forWriteOperations_whenThresholdReached() {
    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await()
        .untilAsserted(() -> assertThatThrownBy(() -> jedis.set(server1Tag + "oneMoreKey", "value"))
            .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    memoryPressure.cancel(true);
  }

  @Test
  public void shouldReturnOOMError_forSubscribe_whenThresholdReached() {
    MockSubscriber mockSubscriber = new MockSubscriber();
    JedisCluster subJedis =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPorts[0]), REDIS_CLIENT_TIMEOUT);

    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await()
        .untilAsserted(() -> assertThatThrownBy(() -> subJedis.subscribe(mockSubscriber, "channel"))
            .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    subJedis.close();

    memoryPressure.cancel(true);
  }

  @Test
  public void shouldReturnOOMError_forPublish_whenThresholdReached() {
    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await().untilAsserted(() -> assertThatThrownBy(() -> jedis.publish("channel", "message"))
        .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    memoryPressure.cancel(true);
  }

  @Test
  public void shouldReturnOOMError_onOtherServer_forWriteOperations_whenThresholdReached() {
    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await()
        .untilAsserted(() -> assertThatThrownBy(() -> jedis.set(server2Tag + "oneMoreKey", "value"))
            .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    memoryPressure.cancel(true);
  }

  @Test
  public void shouldAllowDeleteOperations_afterThresholdReached() {
    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await().untilAsserted(
        () -> assertThatNoException().isThrownBy(() -> jedis.del(server1Tag + KEY + 1)));

    memoryPressure.cancel(true);
  }

  @Test
  public void shouldAllowExpiration_afterThresholdReached() {
    fillServer1Memory(jedis, true);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, true));

    await().untilAsserted(() -> assertThat(jedis.ttl(server1Tag + KEY + 1)).isEqualTo(-2));

    memoryPressure.cancel(true);
  }

  @Test
  public void shouldAllowWriteOperations_afterDroppingBelowCriticalThreshold() {
    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await()
        .untilAsserted(() -> assertThatThrownBy(() -> jedis.set(server1Tag + "oneMoreKey", "value"))
            .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    memoryPressure.cancel(true);

    await().untilAsserted(() -> assertThatNoException().isThrownBy(
        () -> {
          removeAllKeysAndForceGC();
          jedis.set(server1Tag + "newKey", LARGE_VALUE);
        }));
  }

  @Test
  public void shouldAllowWriteOperations_onOtherServer_afterDroppingBelowCriticalThreshold() {
    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await()
        .untilAsserted(() -> assertThatThrownBy(() -> jedis.set(server2Tag + "oneMoreKey", "value"))
            .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    memoryPressure.cancel(true);

    await().untilAsserted(() -> assertThatNoException().isThrownBy(
        () -> {
          removeAllKeysAndForceGC();
          jedis.set(server2Tag + "newKey", LARGE_VALUE);
        }));
  }

  @Test
  public void shouldAllowSubscribe_afterDroppingBelowCriticalThreshold() {
    MockSubscriber mockSubscriber = new MockSubscriber();
    JedisCluster subJedis =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPorts[0]), REDIS_CLIENT_TIMEOUT);

    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    String channel = "channel";
    await().untilAsserted(() -> assertThatThrownBy(
        () -> subJedis.subscribe(mockSubscriber, channel))
            .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    memoryPressure.cancel(true);

    await().untilAsserted(() -> {
      removeAllKeysAndForceGC();
      executor.submit(() -> subJedis.subscribe(mockSubscriber, channel));
      assertThat(mockSubscriber.getSubscribedChannels()).isOne();
    });

    mockSubscriber.unsubscribe();
    subJedis.close();
  }

  @Test
  public void shouldAllowPublish_afterDroppingBelowCriticalThreshold() {
    fillServer1Memory(jedis, false);
    Future<Void> memoryPressure =
        executor.submit(() -> maintainMemoryPressure(jedis, false));

    await().untilAsserted(() -> assertThatThrownBy(() -> jedis.publish("channel", "message"))
        .hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED));

    memoryPressure.cancel(true);

    await().untilAsserted(
        () -> assertThatNoException().isThrownBy(() -> {
          removeAllKeysAndForceGC();
          jedis.publish("channel", "message");
        }));
  }

  private void fillServer1Memory(JedisCluster jedis, boolean withExpiration) {
    assertThatThrownBy(() -> {
      // First force GC to reduce the chances of dropping back below critical accidentally
      gfsh.execute("gc");
      for (int count = 0; count < MAX_ITERATION_COUNT; ++count) {
        if (withExpiration) {
          jedis.setex(server1Tag + KEY + numberOfKeys.get(), KEY_TTL_SECONDS, LARGE_VALUE);
        } else {
          jedis.set(server1Tag + KEY + numberOfKeys.get(), LARGE_VALUE);
        }
        numberOfKeys.incrementAndGet();
      }
    }).hasMessage("OOM " + ERROR_OOM_COMMAND_NOT_ALLOWED);
  }

  private void maintainMemoryPressure(JedisCluster jedis, boolean withExpiration) {
    while (!Thread.interrupted()) {
      fillServer1Memory(jedis, withExpiration);
    }
  }

  void removeAllKeysAndForceGC() throws Exception {
    // Remove all the keys to allow memory to drop below critical
    Set<String> keys = jedis.keys(server1Tag + "*");
    keys.addAll(jedis.keys(server2Tag + "*"));
    for (String key : keys) {
      jedis.del(key);
    }

    // Force GC
    gfsh.execute("gc");
  }
}
