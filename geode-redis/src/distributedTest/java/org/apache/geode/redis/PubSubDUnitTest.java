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
 *
 */

package org.apache.geode.redis;

import static java.lang.String.valueOf;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class PubSubDUnitTest {

  public static final String CHANNEL_NAME = "salutations";

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(5);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static int[] ports;

  static final String LOCAL_HOST = "127.0.0.1";
  static Jedis subscriber1;
  static Jedis subscriber2;
  static Jedis publisher1;
  static Jedis publisher2;

  static Properties locatorProperties;
  static Properties serverProperties1;
  static Properties serverProperties2;
  static Properties serverProperties3;
  static Properties serverProperties4;

  static MemberVM locator;
  static MemberVM server1;
  static MemberVM server2;
  static MemberVM server3;
  static MemberVM server4;

  @BeforeClass
  public static void beforeClass() throws Exception {
    ports = AvailablePortHelper.getRandomAvailableTCPPorts(4);

    locatorProperties = new Properties();
    serverProperties1 = new Properties();
    serverProperties2 = new Properties();
    serverProperties3 = new Properties();
    serverProperties4 = new Properties();

    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    serverProperties1.setProperty(REDIS_PORT, valueOf(ports[0]));
    serverProperties1.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    serverProperties2.setProperty(REDIS_PORT, valueOf(ports[1]));
    serverProperties2.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    serverProperties3.setProperty(REDIS_PORT, valueOf(ports[2]));
    serverProperties3.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    serverProperties4.setProperty(REDIS_PORT, valueOf(ports[3]));
    serverProperties4.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    locator = cluster.startLocatorVM(0, locatorProperties);
    server1 = cluster.startServerVM(1, serverProperties1, locator.getPort());
    server2 = cluster.startServerVM(2, serverProperties2, locator.getPort());
    server3 = cluster.startServerVM(3, serverProperties3, locator.getPort());
    server4 = cluster.startServerVM(4, serverProperties4, locator.getPort());
    serverProperties1.setProperty("log-level", "error");
    serverProperties2.setProperty("log-level", "error");
    serverProperties3.setProperty("log-level", "error");
    serverProperties4.setProperty("log-level", "error");

    subscriber1 = new Jedis(LOCAL_HOST, ports[0]);
    subscriber2 = new Jedis(LOCAL_HOST, ports[1]);
    publisher1 = new Jedis(LOCAL_HOST, ports[2]);
    publisher2 = new Jedis(LOCAL_HOST, ports[3]);

    gfsh.connectAndVerify(locator);
  }

  @Before
  public void testSetup() {
    subscriber1.flushAll();
    subscriber2.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    subscriber1.disconnect();
    subscriber2.disconnect();
    publisher1.disconnect();
    publisher2.disconnect();

    server1.stop();
    server2.stop();
    server3.stop();
    server4.stop();
  }

  @Test
  public void shouldContinueToFunction_whenOneSubscriberShutsDownGracefully_givenTwoSubscribersOnePublisher()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);

    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future = executor.submit(
        () -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future = executor.submit(
        () -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS))
        .as("channel subscription was not received")
        .isTrue();

    Long result = publisher1.publish(CHANNEL_NAME, "hello");
    assertThat(result).isEqualTo(2);

    server1.stop();
    Long resultFromSecondMessage = publisher1.publish(CHANNEL_NAME, "hello again");
    assertThat(resultFromSecondMessage).isEqualTo(1);

    mockSubscriber2.unsubscribe(CHANNEL_NAME);
    GeodeAwaitility.await().untilAsserted(subscriber2Future::get);

    restartServerVM1();
    reconnectSubscriber1();
  }

  @Test
  public void shouldContinueToFunction_whenOneSubscriberShutsDownAbruptly_givenTwoSubscribersOnePublisher()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);

    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future = executor.submit(
        () -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future = executor.submit(
        () -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS))
        .as("channel subscription was not received")
        .isTrue();

    Long result = publisher1.publish(CHANNEL_NAME, "hello");
    assertThat(result).isEqualTo(2);

    cluster.crashVM(1);
    publisher1.publish(CHANNEL_NAME, "hello again");

    mockSubscriber2.unsubscribe(CHANNEL_NAME);

    GeodeAwaitility.await().untilAsserted(subscriber2Future::get);

    restartServerVM1();
    reconnectSubscriber1();
  }

  private void restartServerVM1() {
    cluster.startServerVM(1, serverProperties1, locator.getPort());
    await()
        .untilAsserted(() -> gfsh.executeAndAssertThat("list members")
            .statusIsSuccess()
            .hasTableSection()
            .hasColumn("Name")
            .containsOnly("locator-0", "server-1", "server-2", "server-3", "server-4"));
  }

  private void reconnectSubscriber1() {
    subscriber1 = new Jedis(LOCAL_HOST, ports[0]);
  }

  @Test
  public void shouldContinueToFunction_whenOneSubscriberShutsDownGracefully_givenTwoSubscribersTwoPublishers()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);

    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future = executor.submit(
        () -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future = executor.submit(
        () -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS))
        .as("channel subscription was not received")
        .isTrue();

    Long resultPublisher1 = publisher1.publish(CHANNEL_NAME, "hello");
    Long resultPublisher2 = publisher2.publish(CHANNEL_NAME, "hello");
    assertThat(resultPublisher1).isEqualTo(2);
    assertThat(resultPublisher2).isEqualTo(2);

    server1.stop();

    publisher1.publish(CHANNEL_NAME, "hello again");
    publisher2.publish(CHANNEL_NAME, "hello again");

    mockSubscriber2.unsubscribe(CHANNEL_NAME);

    GeodeAwaitility.await().untilAsserted(subscriber2Future::get);

    restartServerVM1();
    reconnectSubscriber1();
  }

  @Test
  public void testSubscribePublishUsingDifferentServers() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future = executor.submit(
        () -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future = executor.submit(
        () -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS))
        .as("channel subscription was not received")
        .isTrue();

    Long result = publisher1.publish(CHANNEL_NAME, "hello");
    assertThat(result).isEqualTo(2);

    mockSubscriber1.unsubscribe(CHANNEL_NAME);
    mockSubscriber2.unsubscribe(CHANNEL_NAME);

    GeodeAwaitility.await().untilAsserted(subscriber1Future::get);
    GeodeAwaitility.await().untilAsserted(subscriber2Future::get);
  }

  @Test
  public void testConcurrentPubSub() throws Exception {
    int CLIENT_COUNT = 10;
    int ITERATIONS = 1000;

    CountDownLatch latch = new CountDownLatch(2);
    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future = executor.submit(
        () -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future = executor.submit(
        () -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS))
        .as("channel subscription was not received")
        .isTrue();

    List<Future<Void>> futures = new LinkedList<>();
    for (int i = 0; i < CLIENT_COUNT; i++) {
      Jedis publisher = new Jedis("localhost", ports[i % 2]);

      Callable<Void> callable = () -> {
        for (int j = 0; j < ITERATIONS; j++) {
          publisher.publish(CHANNEL_NAME, "hello");
        }
        return null;
      };

      futures.add(executor.submit(callable));
    }

    for (Future<Void> future : futures) {
      GeodeAwaitility.await().untilAsserted(future::get);
    }

    mockSubscriber1.unsubscribe(CHANNEL_NAME);
    mockSubscriber2.unsubscribe(CHANNEL_NAME);

    GeodeAwaitility.await().untilAsserted(subscriber1Future::get);
    GeodeAwaitility.await().untilAsserted(subscriber2Future::get);

    assertThat(mockSubscriber1.getReceivedMessages().size()).isEqualTo(CLIENT_COUNT * ITERATIONS);
    assertThat(mockSubscriber2.getReceivedMessages().size()).isEqualTo(CLIENT_COUNT * ITERATIONS);
  }

  @Test
  public void testPubSubWithMoreSubscribersThanNettyWorkerThreads() throws Exception {
    int CLIENT_COUNT = 1000;
    String CHANNEL_NAME = "best_channel_ever";

    List<Jedis> clients = new ArrayList<>();

    // Build up an initial set of subscribers
    for (int i = 0; i < CLIENT_COUNT; i++) {
      Jedis client = new Jedis("localhost", ports[0]);
      clients.add(client);

      CountDownLatch latch = new CountDownLatch(1);
      MockSubscriber mockSubscriber = new MockSubscriber(latch);
      executor.submit(() -> client.subscribe(mockSubscriber, CHANNEL_NAME));
      latch.await();

      clients.add(client);
    }

    Jedis publishingClient = new Jedis("localhost", ports[0]);
    long result = 0;

    for (int i = 0; i < 10; i++) {
      result += publishingClient.publish(CHANNEL_NAME, "this is amazing");
    }

    assertThat(result).isEqualTo(CLIENT_COUNT * 10);

    clients.forEach(Jedis::close);
  }

  @Test
  public void testPubSubWithManyClientsDisconnecting() throws Exception {
    int CLIENT_COUNT = 10;
    int ITERATIONS = 400;

    Random random = new Random();
    List<Jedis> clients = new ArrayList<>();

    // Build up an initial set of subscribers
    for (int i = 0; i < CLIENT_COUNT; i++) {
      Jedis client = new Jedis("localhost", ports[0]);
      clients.add(client);

      CountDownLatch latch = new CountDownLatch(1);
      MockSubscriber mockSubscriber = new MockSubscriber(latch);
      executor.submit(() -> client.subscribe(mockSubscriber, CHANNEL_NAME));
      latch.await();
    }

    // Start actively publishing in the background
    Jedis publishingClient = new Jedis("localhost", ports[0]);
    Callable<Void> callable = () -> {
      for (int j = 0; j < ITERATIONS; j++) {
        publishingClient.publish(CHANNEL_NAME, "hello - " + j);
      }
      return null;
    };

    Future<Void> future = executor.submit(callable);

    // Abnormally close and recreate new subscribers without unsubscribing
    for (int i = 0; i < ITERATIONS; i++) {
      int candy = random.nextInt(CLIENT_COUNT);
      clients.get(candy).close();

      Jedis client = new Jedis("localhost", ports[0]);
      CountDownLatch latch = new CountDownLatch(1);
      MockSubscriber mockSubscriber = new MockSubscriber(latch);
      executor.submit(() -> client.subscribe(mockSubscriber, CHANNEL_NAME));
      latch.await();

      clients.set(candy, client);
    }

    GeodeAwaitility.await().untilAsserted(() -> future.get());

    clients.forEach(Jedis::close);
  }
}
