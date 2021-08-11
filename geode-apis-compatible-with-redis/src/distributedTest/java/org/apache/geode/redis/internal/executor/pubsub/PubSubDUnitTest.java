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

package org.apache.geode.redis.internal.executor.pubsub;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


public class PubSubDUnitTest {

  public static final String CHANNEL_NAME = "salutations";
  public static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(6);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String LOCAL_HOST = "127.0.0.1";
  private static Jedis subscriber1;
  private static Jedis subscriber2;
  private static Jedis publisher1;
  private static Jedis publisher2;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static MemberVM server4;
  private static MemberVM server5;

  private static int redisServerPort1;
  private static int redisServerPort2;
  private static int redisServerPort3;
  private static int redisServerPort4;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0);

    int locatorPort = locator.getPort();
    SerializableFunction<ServerStarterRule> operator = x -> x
        .withSystemProperty("io.netty.eventLoopThreads", "10")
        .withConnectionToLocator(locatorPort);

    server1 = cluster.startRedisVM(1, operator);
    server2 = cluster.startRedisVM(2, operator);
    server3 = cluster.startRedisVM(3, operator);
    server4 = cluster.startRedisVM(4, operator);
    server5 = cluster.startServerVM(5, locator.getPort());

    for (VM v : Host.getHost(0).getAllVMs()) {
      v.invoke(() -> {
        Logger logger = LogService.getLogger("org.apache.geode.redis");
        Configurator.setAllLevels(logger.getName(), Level.getLevel("DEBUG"));
        FastLogger.setDelegating(true);
      });
    }

    redisServerPort1 = cluster.getRedisPort(1);
    redisServerPort2 = cluster.getRedisPort(2);
    redisServerPort3 = cluster.getRedisPort(3);
    redisServerPort4 = cluster.getRedisPort(4);

    subscriber1 = new Jedis(LOCAL_HOST, redisServerPort1, 120000);
    subscriber2 = new Jedis(LOCAL_HOST, redisServerPort2, 120000);
    publisher1 = new Jedis(LOCAL_HOST, redisServerPort3, 120000);
    publisher2 = new Jedis(LOCAL_HOST, redisServerPort4, 120000);

    gfsh.connectAndVerify(locator);
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
    server5.stop();
  }

  @Test
  public void shouldNotHang_givenPublishingAndSubscribingSimultaneously() {
    ArrayList<Thread> threads = new ArrayList<>();
    AtomicLong publishCount = new AtomicLong();
    Random random = new Random();
    int SUBSCRIBER_COUNT = 5;
    int CHANNEL_COUNT = 200;

    for (int i = 0; i < CHANNEL_COUNT; i++) {
      String channelName = "theBestChannel" + i;
      Thread thread = new LoggingThread(channelName, () -> {
        ArrayList<MockSubscriber> mockSubscribers = new ArrayList<>();
        ArrayList<Jedis> clients = new ArrayList<>();
        ArrayList<Future<Void>> subscribeFutures = new ArrayList<>();

        for (int j = 0; j < SUBSCRIBER_COUNT; j++) {
          MockSubscriber mockSubscriber = new MockSubscriber();
          mockSubscribers.add(mockSubscriber);
          Jedis client = getConnection(random);
          clients.add(client);

          Future<Void> f = executor.submit(() -> {
            client.subscribe(mockSubscriber, channelName);
          });
          subscribeFutures.add(f);
        }

        mockSubscribers.forEach(x -> x.awaitSubscribe(channelName));

        Jedis localPublisher = getConnection(random);
        mockSubscribers.forEach(x -> x.prepareMessagesReceivedLatch(1));
        localPublisher.publish(channelName, "hi");
        mockSubscribers.forEach(MockSubscriber::awaitMessagesReceived);
        localPublisher.close();

        mockSubscribers.forEach(s -> {
          publishCount.addAndGet(s.getReceivedMessages().size());
          s.unsubscribe(channelName);
          s.awaitUnsubscribe(channelName);
        });

        subscribeFutures.forEach(x -> {
          try {
            x.get(JEDIS_TIMEOUT, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
        clients.forEach(Jedis::close);
      });

      threads.add(thread);
      thread.start();
    }

    threads.forEach(thread -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    assertThat(publishCount.get()).isEqualTo(CHANNEL_COUNT * SUBSCRIBER_COUNT);
  }

  @Test
  public void shouldContinueToFunction_whenOneServerShutsDownGracefully_givenTwoSubscribersOnePublisher()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);

    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    executor.submit(() -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future =
        executor.submit(() -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS)).as("channel subscription was not received")
        .isTrue();

    publisher1.publish(CHANNEL_NAME, "hello");

    server1.stop();
    mockSubscriber2.prepareMessagesReceivedLatch(1);
    publisher1.publish(CHANNEL_NAME, "hello again");
    mockSubscriber2.awaitMessagesReceived();

    mockSubscriber2.unsubscribe(CHANNEL_NAME);
    GeodeAwaitility.await().untilAsserted(subscriber2Future::get);

    restartServerVM1();
    reconnectSubscriber1();
  }

  @Test
  public void shouldContinueToFunction_whenOneServerShutsDownAbruptly_givenTwoSubscribersOnePublisher()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);

    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future =
        executor.submit(() -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future =
        executor.submit(() -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS))
        .as("channel subscription was not received").isTrue();

    publisher1.publish(CHANNEL_NAME, "hello");

    cluster.crashVM(2);

    boolean published = false;
    do {
      try {
        publisher1.publish(CHANNEL_NAME, "hello again");
        published = true;
      } catch (JedisConnectionException ex) {
        if (ex.getMessage().contains("Unexpected end of stream.")) {
          publisher1 = new Jedis(LOCAL_HOST, redisServerPort3, 120000);
          // fall through and retry
        } else {
          throw ex;
        }
      }
    } while (!published);

    mockSubscriber1.unsubscribe(CHANNEL_NAME);

    GeodeAwaitility.await().untilAsserted(subscriber1Future::get);
    try {
      subscriber2Future.get();
    } catch (ExecutionException e) {
      // exception expected since we killed server 2
    }

    restartServerVM2();
    reconnectSubscriber2();
  }

  @Test
  public void shouldContinueToFunction_whenOneServerShutsDownGracefully_givenTwoSubscribersTwoPublishers()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);

    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future =
        executor.submit(() -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    executor.submit(() -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS)).as("channel subscription was not received")
        .isTrue();

    publisher1.publish(CHANNEL_NAME, "hello");
    publisher2.publish(CHANNEL_NAME, "hello");

    server2.stop();

    mockSubscriber1.prepareMessagesReceivedLatch(1);
    publisher1.publish(CHANNEL_NAME, "hello again");
    publisher2.publish(CHANNEL_NAME, "hello again");

    mockSubscriber1.awaitMessagesReceived();

    mockSubscriber1.unsubscribe(CHANNEL_NAME);

    GeodeAwaitility.await().untilAsserted(subscriber1Future::get);

    restartServerVM2();
    reconnectSubscriber2();
  }

  @Test
  public void testSubscribePublishUsingDifferentServers() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Future<Void> subscriber1Future =
        executor.submit(() -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future =
        executor.submit(() -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS)).as("channel subscription was not received")
        .isTrue();

    mockSubscriber1.prepareMessagesReceivedLatch(1);
    mockSubscriber2.prepareMessagesReceivedLatch(1);
    publisher1.publish(CHANNEL_NAME, "hello");

    mockSubscriber1.awaitMessagesReceived();
    mockSubscriber2.awaitMessagesReceived();

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

    Future<Void> subscriber1Future =
        executor.submit(() -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME));
    Future<Void> subscriber2Future =
        executor.submit(() -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME));

    assertThat(latch.await(30, TimeUnit.SECONDS)).as("channel subscription was not received")
        .isTrue();

    List<Future<Void>> futures = new LinkedList<>();
    int[] ports = new int[] {redisServerPort1, redisServerPort2};
    for (int i = 0; i < CLIENT_COUNT; i++) {
      Jedis publisher = new Jedis("localhost", ports[i % 2]);

      int localI = i;
      Callable<Void> callable = () -> {
        for (int j = 0; j < ITERATIONS; j++) {
          publisher.publish(CHANNEL_NAME, String.format("hello-%d-%d", localI, j));
        }
        publisher.close();
        return null;
      };

      futures.add(executor.submit(callable));
    }

    for (Future<Void> future : futures) {
      GeodeAwaitility.await().untilAsserted(future::get);
    }

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber1.getReceivedMessages().size())
            .isEqualTo(CLIENT_COUNT * ITERATIONS));
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber2.getReceivedMessages().size())
            .isEqualTo(CLIENT_COUNT * ITERATIONS));

    mockSubscriber1.unsubscribe(CHANNEL_NAME);
    mockSubscriber2.unsubscribe(CHANNEL_NAME);

    subscriber1Future.get();
    subscriber2Future.get();
  }

  @Test
  public void testPubSubWithMoreSubscribersThanNettyWorkerThreads() throws Exception {
    int CLIENT_COUNT = 100;
    String CHANNEL_NAME = "best_channel_ever";

    List<Jedis> clients = new ArrayList<>();
    List<MockSubscriber> subscribers = new ArrayList<>();

    // Build up an initial set of subscribers
    for (int i = 0; i < CLIENT_COUNT; i++) {
      Jedis client = new Jedis("localhost", redisServerPort1);
      clients.add(client);

      CountDownLatch latch = new CountDownLatch(1);
      MockSubscriber mockSubscriber = new MockSubscriber(latch);
      executor.submit(() -> client.subscribe(mockSubscriber, CHANNEL_NAME));
      latch.await();

      subscribers.add(mockSubscriber);
      clients.add(client);
    }

    Jedis publishingClient = new Jedis("localhost", redisServerPort1);
    long result = 0;

    for (int i = 0; i < 10; i++) {
      result += publishingClient.publish(CHANNEL_NAME, "this is amazing");
    }

    assertThat(result).isEqualTo(CLIENT_COUNT * 10);

    subscribers.forEach(x -> {
      x.unsubscribe();
      x.awaitUnsubscribe(CHANNEL_NAME);
    });
    clients.forEach(Jedis::close);
  }

  @Test
  public void testPubSubWithManyClientsDisconnecting() throws Exception {
    int CLIENT_COUNT = 1;
    int ITERATIONS = 1000;
    String LOCAL_CHANNEL_NAME = "disconnecting_channel";

    Random random = new Random();
    List<Jedis> clients = new ArrayList<>();

    // Build up an initial set of subscribers
    for (int i = 0; i < CLIENT_COUNT; i++) {
      Jedis client = new Jedis("localhost", redisServerPort1);
      clients.add(client);

      CountDownLatch latch = new CountDownLatch(1);
      MockSubscriber mockSubscriber = new MockSubscriber(latch);
      executor.submit(() -> client.subscribe(mockSubscriber, LOCAL_CHANNEL_NAME));
      latch.await();
    }

    // Start actively publishing in the background
    Jedis publishingClient = new Jedis("localhost", redisServerPort1, 60_000);
    Callable<Void> callable = () -> {
      for (int j = 0; j < ITERATIONS; j++) {
        publishingClient.publish(LOCAL_CHANNEL_NAME, "hello - " + j);
      }
      return null;
    };

    Future<Void> future = executor.submit(callable);

    // Abnormally close and recreate new subscribers without unsubscribing
    for (int i = 0; i < ITERATIONS; i++) {
      int candy = random.nextInt(CLIENT_COUNT);
      clients.get(candy).close();

      Jedis client = new Jedis("localhost", redisServerPort1);
      CountDownLatch latch = new CountDownLatch(1);
      MockSubscriber mockSubscriber = new MockSubscriber(latch);
      executor.submit(() -> client.subscribe(mockSubscriber, LOCAL_CHANNEL_NAME));
      latch.await();

      clients.set(candy, client);
    }

    GeodeAwaitility.await().untilAsserted(() -> future.get());

    clients.forEach(Jedis::close);
  }

  private void restartServerVM1() {
    cluster.startRedisVM(1, locator.getPort());
    waitForRestart();
    redisServerPort1 = cluster.getRedisPort(1);
  }

  private void restartServerVM2() {
    cluster.startRedisVM(2, locator.getPort());
    waitForRestart();
    redisServerPort2 = cluster.getRedisPort(2);
  }

  private void waitForRestart() {
    await().untilAsserted(
        () -> gfsh.executeAndAssertThat("list members").statusIsSuccess().hasTableSection()
            .hasColumn("Name")
            .containsOnly("locator-0", "server-1", "server-2", "server-3", "server-4",
                "server-5"));
  }

  private void reconnectSubscriber1() {
    subscriber1 = new Jedis(LOCAL_HOST, redisServerPort1);
  }

  private void reconnectSubscriber2() {
    subscriber2 = new Jedis(LOCAL_HOST, redisServerPort2);
  }

  private Jedis getConnection(Random random) {
    Jedis client = null;

    for (int i = 0; i < 10; i++) {
      int randPort = random.nextInt(4) + 1;
      try {
        client = new Jedis("localhost", cluster.getRedisPort(randPort), JEDIS_TIMEOUT);
        client.ping();
        return client;
      } catch (Exception e) {
        try {
          if (client != null) {
            client.close();
          }
        } catch (Exception exception) {
        }
      }
    }
    throw new RuntimeException("Tried 10 times, but could not get a good connection.");
  }
}
