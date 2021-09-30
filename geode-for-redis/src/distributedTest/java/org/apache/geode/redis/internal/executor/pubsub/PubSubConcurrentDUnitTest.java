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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


public class PubSubConcurrentDUnitTest {

  public static final String CHANNEL_NAME = "salutations";
  public static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  public static final int PUBLISHER_COUNT = 5;
  public static final int PUBLISH_ITERATIONS = 10000;
  public static final int SUBSCRIBER_COUNT = 5;
  public static final int EXPECTED_MESSAGES = PUBLISHER_COUNT * PUBLISH_ITERATIONS;


  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(4);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  public static ExecutorService subscribeExecutor;

  private static final List<Jedis> subscribers = new ArrayList<>(SUBSCRIBER_COUNT);
  private static final List<LatencySubscriber> mockSubscribers = new ArrayList<>(SUBSCRIBER_COUNT);
  private static final List<Future<Void>> subscriberFutures = new ArrayList<>(SUBSCRIBER_COUNT);

  private static final String LOCAL_HOST = "127.0.0.1";

  private static MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;
  private VM subscriberVM;

  private int redisServerPort1;
  private int redisServerPort2;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0);
  }

  @Before
  public void before() throws Exception {
    int locatorPort = locator.getPort();
    SerializableFunction<ServerStarterRule> operator = x -> x
        .withSystemProperty("io.netty.eventLoopThreads", "10")
        .withConnectionToLocator(locatorPort);

    server1 = cluster.startRedisVM(1, operator);
    server2 = cluster.startRedisVM(2, operator);
    subscriberVM = cluster.getVM(3);

    for (VM v : Host.getHost(0).getAllVMs()) {
      v.invoke(() -> {
        Logger logger = LogService.getLogger("org.apache.geode.redis");
        Configurator.setAllLevels(logger.getName(), Level.getLevel("INFO"));
        FastLogger.setDelegating(true);
      });
    }

    redisServerPort1 = cluster.getRedisPort(1);
    redisServerPort2 = cluster.getRedisPort(2);

    gfsh.connectAndVerify(locator);
  }

  @After
  public void tearDown() {
    server1.stop();
    server2.stop();
    subscriberVM.invoke(() -> {
      subscribers.clear();
      mockSubscribers.clear();
      subscriberFutures.clear();
      subscribeExecutor.shutdownNow();
    });
  }

  @Test
  public void testConcurrentPubSubWithChannels() {
    concurrentPubSub(false);
  }

  @Test
  public void testConcurrentPubSubWithPatterns() {
    concurrentPubSub(true);
  }

  private void concurrentPubSub(boolean usePattern) {
    int[] ports = new int[] {redisServerPort1, redisServerPort2};

    subscriberVM.invoke(new CreateSubscribers(ports, usePattern));

    List<Future<Void>> futures = new LinkedList<>();
    for (int i = 0; i < PUBLISHER_COUNT; i++) {
      Jedis publisher = new Jedis(LOCAL_HOST, ports[i % ports.length]);
      int localI = i;
      Callable<Void> callable = () -> {
        long start = System.nanoTime();
        for (int j = 0; j < PUBLISH_ITERATIONS; j++) {
          publisher.publish(CHANNEL_NAME,
              String.format("hello-%d-%d-%d", localI, j, System.nanoTime()));
        }
        long end = System.nanoTime();
        System.out.println("Did " + PUBLISH_ITERATIONS + " publishes in "
            + TimeUnit.SECONDS.convert(end - start, TimeUnit.NANOSECONDS) + " seconds to "
            + SUBSCRIBER_COUNT + " subscribers. Nanos/op=" + (end - start) / PUBLISH_ITERATIONS);
        publisher.close();
        return null;
      };

      futures.add(executor.submit(callable));
    }

    for (Future<Void> future : futures) {
      GeodeAwaitility.await().untilAsserted(future::get);
    }

    subscriberVM.invoke(new WaitForSubscribers(usePattern));
  }

  private static class LatencySubscriber extends JedisPubSub {
    private long totalLatency = 0;
    private int messageCount = 0;
    private final CountDownLatch subscriptionLatch;
    // key is the publisherId and value is the messageId
    private final Map<Integer, Integer> receivedMessages = new HashMap<>();

    public LatencySubscriber(CountDownLatch subscriptionLatch) {
      this.subscriptionLatch = subscriptionLatch;
    }

    @Override
    public void onMessage(String channel, String message) {
      long end = System.nanoTime();
      long start = Long.parseLong(message.substring(message.lastIndexOf('-') + 1));
      totalLatency += end - start;
      messageCount++;
      // if (messageCount % 1000 == 0) {
      // System.out.println(getLatencyReport());
      // }
      int publisherIdStart = message.indexOf('-') + 1;
      int publisherIdEnd = message.indexOf('-', publisherIdStart);
      int publisherId = Integer.parseInt(message.substring(publisherIdStart, publisherIdEnd));
      int messageIdStart = publisherIdEnd + 1;
      int messageIdEnd = message.indexOf('-', messageIdStart);
      int messageId = Integer.parseInt(message.substring(messageIdStart, messageIdEnd));
      if (receivedMessages.containsKey(publisherId)) {
        assertThat(messageId).isEqualTo(receivedMessages.get(publisherId) + 1);
      } else {
        assertThat(messageId).isZero();
      }
      receivedMessages.put(publisherId, messageId);
    }

    public void onPMessage(String pattern, String channel, String message) {
      onMessage(channel, message);
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
      subscriptionLatch.countDown();
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
      subscriptionLatch.countDown();
    }

    public int getMessageCount() {
      return messageCount;
    }

    public String getLatencyReport() {
      return "Received " + messageCount + " messages in " + totalLatency + " nanos. nanos/op="
          + (totalLatency / messageCount);
    }
  }

  private static class CreateSubscribers implements SerializableRunnableIF {
    private final int[] ports;
    private final boolean usePattern;

    public CreateSubscribers(int[] ports, boolean usePattern) {
      this.ports = ports;
      this.usePattern = usePattern;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() throws Exception {
      subscribeExecutor = LoggingExecutors.newCachedThreadPool("subscribers", true);
      CountDownLatch latch = new CountDownLatch(SUBSCRIBER_COUNT);
      for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
        Jedis subscriber = new Jedis(LOCAL_HOST, ports[i % ports.length], 120000);
        subscribers.add(subscriber);
        LatencySubscriber mockSubscriber = new LatencySubscriber(latch);
        mockSubscribers.add(mockSubscriber);
        if (usePattern) {
          subscriberFutures.add((Future<Void>) subscribeExecutor
              .submit(() -> subscriber.psubscribe(mockSubscriber, "*")));
        } else {
          subscriberFutures.add((Future<Void>) subscribeExecutor
              .submit(() -> subscriber.subscribe(mockSubscriber, CHANNEL_NAME)));
        }
      }

      assertThat(latch.await(30, TimeUnit.SECONDS)).as("channel subscription was not received")
          .isTrue();
    }
  }
  private static class WaitForSubscribers implements SerializableRunnableIF {
    private final boolean usePattern;

    public WaitForSubscribers(boolean usePattern) {
      this.usePattern = usePattern;
    }

    @Override
    public void run() throws Exception {
      for (LatencySubscriber mockSubscriber : mockSubscribers) {
        GeodeAwaitility.await()
            .untilAsserted(() -> assertThat(mockSubscriber.getMessageCount())
                .isEqualTo(EXPECTED_MESSAGES));
      }

      for (LatencySubscriber mockSubscriber : mockSubscribers) {
        System.out.println(mockSubscriber.getLatencyReport());
        if (usePattern) {
          mockSubscriber.punsubscribe("*");
        } else {
          mockSubscriber.unsubscribe(CHANNEL_NAME);
        }
      }

      for (Future<Void> subscriberFuture : subscriberFutures) {
        subscriberFuture.get();
      }
      for (Jedis subscriber : subscribers) {
        subscriber.disconnect();
      }
    }
  }
}
