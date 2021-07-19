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

package org.apache.geode.redis.internal.executor.pubsub;

import static org.apache.geode.redis.internal.executor.pubsub.AbstractSubscriptionsIntegrationTest.REDIS_CLIENT_TIMEOUT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.StreamConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.mocks.MockBinarySubscriber;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractPubSubIntegrationTest implements RedisIntegrationTest {
  private Jedis publisher;
  private Jedis subscriber;

  public static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Before
  public void setUp() {
    subscriber = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    publisher = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    subscriber.close();
    publisher.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void punsubscribe_whenNonexistent() {
    assertThat((List<Object>) subscriber.sendCommand(Protocol.Command.PUNSUBSCRIBE, "Nonexistent"))
        .containsExactly("punsubscribe".getBytes(), "Nonexistent".getBytes(),
            0L);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void unsubscribe_whenNoSubscriptionsExist_shouldNotHang() {
    assertThat((List<Object>) subscriber.sendCommand(Protocol.Command.UNSUBSCRIBE))
        .containsExactly("unsubscribe".getBytes(), null, 0L);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void punsubscribe_whenNoSubscriptionsExist_shouldNotHang() {
    assertThat((List<Object>) subscriber.sendCommand(Protocol.Command.PUNSUBSCRIBE))
        .containsExactly("punsubscribe".getBytes(), null, 0L);
  }

  @Test
  public void testOneSubscriberOneChannel() {
    List<String> expectedMessages = Arrays.asList("hello");

    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(1);

    mockSubscriber.awaitMessageReceived(1L);
    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages));
  }

  @Test
  public void punsubscribe_givenSubscribe_doesNotReduceSubscriptions() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    try {
      waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

      mockSubscriber.punsubscribe("salutations");
      waitFor(() -> mockSubscriber.punsubscribeInfos.size() == 1);

      assertThat(mockSubscriber.punsubscribeInfos.get(0).channel).isEqualTo("salutations");
      assertThat(mockSubscriber.punsubscribeInfos.get(0).count).isEqualTo(1);
      assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(1);
    } finally {
      // now cleanup the actual subscription
      mockSubscriber.unsubscribe("salutations");
      waitFor(() -> !subscriberThread.isAlive());
    }
  }

  @Test
  public void unsubscribe_givenPsubscribe_doesNotReduceSubscriptions() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.psubscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    try {
      waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

      mockSubscriber.unsubscribe("salutations");
      waitFor(() -> mockSubscriber.unsubscribeInfos.size() == 1);

      assertThat(mockSubscriber.unsubscribeInfos.get(0).channel).isEqualTo("salutations");
      assertThat(mockSubscriber.unsubscribeInfos.get(0).count).isEqualTo(1);
      assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(1);
    } finally {
      // now cleanup the actual subscription
      mockSubscriber.punsubscribe("salutations");
      waitFor(() -> !subscriberThread.isAlive());
    }
  }

  @Test
  public void unsubscribe_onNonExistentSubscription_doesNotReduceSubscriptions() {
    MockSubscriber mockSubscriber = new MockSubscriber();
    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    try {
      waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
      mockSubscriber.unsubscribe("NonExistent");
      waitFor(() -> mockSubscriber.unsubscribeInfos.size() == 1);

      assertThat(mockSubscriber.unsubscribeInfos.get(0).channel).isEqualTo("NonExistent");
      assertThat(mockSubscriber.unsubscribeInfos.get(0).count).isEqualTo(1);
      assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(1);
    } finally {
      // now cleanup the actual subscription
      mockSubscriber.unsubscribe("salutations");
      waitFor(() -> !subscriberThread.isAlive());
    }
  }

  @Test
  public void unsubscribe_whenGivenAnEmptyString() {
    MockSubscriber mockSubscriber = new MockSubscriber();
    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "salutations");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    try {
      waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
      mockSubscriber.unsubscribe("");
      waitFor(() -> mockSubscriber.unsubscribeInfos.size() == 1);

      assertThat(mockSubscriber.unsubscribeInfos.get(0).channel).isEqualTo("");
      assertThat(mockSubscriber.unsubscribeInfos.get(0).count).isEqualTo(1);
      assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(1);
    } finally {
      // now cleanup the actual subscription
      mockSubscriber.unsubscribe();
      waitFor(() -> !subscriberThread.isAlive());
    }
  }

  @Test
  public void unsubscribeWithEmptyChannel_doesNotUnsubscribeExistingChannels() {
    MockSubscriber mockSubscriber = new MockSubscriber();
    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "salutations");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    try {
      waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
      mockSubscriber.unsubscribe("");
      waitFor(() -> mockSubscriber.unsubscribeInfos.size() == 1);

      Long result = publisher.publish("salutations", "heyho");
      waitFor(() -> mockSubscriber.getReceivedMessages().size() == 1);

      assertThat(result).isEqualTo(1);
      assertThat(mockSubscriber.getReceivedMessages().get(0)).isEqualTo("heyho");
    } finally {
      // now cleanup the actual subscription
      mockSubscriber.unsubscribe();
      waitFor(() -> !subscriberThread.isAlive());
    }
  }

  @Test
  public void canSubscribeToAnEmptyString() {
    MockSubscriber mockSubscriber = new MockSubscriber();
    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    Long result = publisher.publish("", "blank");
    assertThat(result).isEqualTo(1);

    mockSubscriber.awaitMessageReceived(1L);
    mockSubscriber.unsubscribe("");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).containsExactly("blank");
  }

  @Test
  public void punsubscribe_onNonExistentSubscription_doesNotReduceSubscriptions() {
    MockSubscriber mockSubscriber = new MockSubscriber();
    Runnable runnable = () -> {
      subscriber.psubscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    try {
      waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
      mockSubscriber.punsubscribe("NonExistent");
      waitFor(() -> mockSubscriber.punsubscribeInfos.size() == 1);

      assertThat(mockSubscriber.punsubscribeInfos.get(0).channel).isEqualTo("NonExistent");
      assertThat(mockSubscriber.punsubscribeInfos.get(0).count).isEqualTo(1);
      assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(1);
    } finally {
      // now cleanup the actual subscription
      mockSubscriber.punsubscribe("salutations");
      waitFor(() -> !subscriberThread.isAlive());
    }
  }

  @Test
  public void testPublishBinaryData() {
    byte[] expectedMessage = new byte[256];
    for (int i = 0; i < 256; i++) {
      expectedMessage[i] = (byte) i;
    }

    MockBinarySubscriber mockSubscriber = new MockBinarySubscriber();

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations".getBytes());
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    Long result = publisher.publish("salutations".getBytes(), expectedMessage);
    assertThat(result).isEqualTo(1);

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedMessages()).isNotEmpty());
    assertThat(mockSubscriber.getReceivedMessages().get(0)).isEqualTo(expectedMessage);

    mockSubscriber.unsubscribe("salutations".getBytes());
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());
  }

  @Test
  public void testSubscribeAndPublishUsingBinaryData() {
    byte[] binaryBlob = new byte[256];
    for (int i = 0; i < 256; i++) {
      binaryBlob[i] = (byte) i;
    }

    MockBinarySubscriber mockSubscriber = new MockBinarySubscriber();

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, binaryBlob);
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    Long result = publisher.publish(binaryBlob, binaryBlob);
    assertThat(result).isEqualTo(1);

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(mockSubscriber.getReceivedMessages()).isNotEmpty());
    assertThat(mockSubscriber.getReceivedMessages().get(0)).isEqualTo(binaryBlob);

    mockSubscriber.unsubscribe(binaryBlob);
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

  }

  @Test
  public void testOneSubscriberSubscribingToTwoChannels() {
    List<String> expectedMessages = Arrays.asList("hello", "howdy");
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "salutations", "yuletide");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(1);

    result = publisher.publish("yuletide", "howdy");
    assertThat(result).isEqualTo(1);
    mockSubscriber.awaitMessageReceived(2L);
    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    assertThat(mockSubscriber.unsubscribeInfos).hasSize(1);
    assertThat(mockSubscriber.unsubscribeInfos.get(0).channel).isEqualTo("salutations");
    assertThat(mockSubscriber.unsubscribeInfos.get(0).count).isEqualTo(1);
    mockSubscriber.unsubscribeInfos.clear();
    mockSubscriber.unsubscribe("yuletide");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    assertThat(mockSubscriber.unsubscribeInfos).hasSize(1);
    assertThat(mockSubscriber.unsubscribeInfos.get(0).channel).isEqualTo("yuletide");
    assertThat(mockSubscriber.unsubscribeInfos.get(0).count).isEqualTo(0);
    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages);
  }

  @Test
  public void testSubscribingAndUnsubscribingFromMultipleChannels() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "salutations", "yuletide");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    mockSubscriber.unsubscribe("yuletide", "salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    List<String> unsubscribedChannels = mockSubscriber.unsubscribeInfos.stream()
        .map(x -> x.channel)
        .collect(Collectors.toList());
    assertThat(unsubscribedChannels).containsExactlyInAnyOrder("salutations", "yuletide");

    List<Integer> channelCounts = mockSubscriber.unsubscribeInfos.stream()
        .map(x -> x.count)
        .collect(Collectors.toList());
    assertThat(channelCounts).containsExactlyInAnyOrder(1, 0);

  }

  @Test
  public void testPsubscribingAndPunsubscribingFromMultipleChannels() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> subscriber.psubscribe(mockSubscriber, "sal*", "yul*");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    mockSubscriber.punsubscribe("yul*", "sal*");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());
    assertThat(mockSubscriber.punsubscribeInfos).containsExactly(
        new MockSubscriber.UnsubscribeInfo("yul*", 1),
        new MockSubscriber.UnsubscribeInfo("sal*", 0));
  }

  @Test
  public void testPunsubscribingImplicitlyFromAllChannels() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> subscriber.psubscribe(mockSubscriber, "sal*", "yul*");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    mockSubscriber.punsubscribe();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());
    assertThat(mockSubscriber.punsubscribeInfos).containsExactly(
        new MockSubscriber.UnsubscribeInfo("sal*", 1),
        new MockSubscriber.UnsubscribeInfo("yul*", 0));
  }

  @Test
  public void ensureOrderingOfPublishedMessages() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);

    Future<Void> future1 =
        executor.submit(() -> runSubscribeAndPublish(1, 3000, running));

    running.set(false);
    future1.get();
  }

  private void runSubscribeAndPublish(int index, int minimumIterations, AtomicBoolean running)
      throws Exception {

    int iterationCount = 0;
    Jedis publisher = getConnection();

    while (iterationCount < minimumIterations || running.get()) {
      List<String> result = subscribeAndPublish(index, iterationCount, publisher);

      assertThat(result)
          .as("Failed at iteration " + iterationCount)
          .containsExactly("message", "pmessage");

      iterationCount++;
    }

    publisher.close();
  }

  private List<String> subscribeAndPublish(int index, int iteration, Jedis localPublisher)
      throws Exception {
    String channel = index + ".foo.bar";
    String pChannel = index + ".foo.*";

    MockSubscriber mockSubscriber = new MockSubscriber();

    try (Jedis localSubscriber = getConnection()) {
      Future<Void> future =
          executor.submit(() -> localSubscriber.subscribe(mockSubscriber, channel));

      mockSubscriber.awaitSubscribe(channel);

      mockSubscriber.psubscribe(pChannel);
      mockSubscriber.awaitPSubscribe(pChannel);

      localPublisher.publish(channel, "hello-" + index + "-" + iteration);
      mockSubscriber.awaitPMessageReceived(1L);
      mockSubscriber.awaitMessageReceived(1L);

      mockSubscriber.unsubscribe(channel);
      mockSubscriber.awaitUnsubscribe(channel);
      mockSubscriber.punsubscribe(pChannel);
      mockSubscriber.awaitPunsubscribe(pChannel);

      future.get();
    }

    return mockSubscriber.getReceivedEvents();
  }

  @Test
  public void testTwoSubscribersOneChannel() {
    Jedis subscriber2 = new Jedis("localhost", getPort(), JEDIS_TIMEOUT);
    MockSubscriber mockSubscriber1 = new MockSubscriber();
    MockSubscriber mockSubscriber2 = new MockSubscriber();

    Runnable runnable1 = () -> subscriber.subscribe(mockSubscriber1, "salutations");
    Runnable runnable2 = () -> subscriber2.subscribe(mockSubscriber2, "salutations");

    Thread subscriber1Thread = new Thread(runnable1);
    subscriber1Thread.start();
    Thread subscriber2Thread = new Thread(runnable2);
    subscriber2Thread.start();

    waitFor(() -> mockSubscriber1.getSubscribedChannels() == 1);
    waitFor(() -> mockSubscriber2.getSubscribedChannels() == 1);

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(2);
    mockSubscriber1.awaitMessageReceived(1L);
    mockSubscriber1.unsubscribe("salutations");
    waitFor(() -> mockSubscriber1.getSubscribedChannels() == 0);
    waitFor(() -> !subscriber1Thread.isAlive());
    assertThat(mockSubscriber1.unsubscribeInfos).hasSize(1);
    assertThat(mockSubscriber1.unsubscribeInfos.get(0).channel).isEqualTo("salutations");
    assertThat(mockSubscriber1.unsubscribeInfos.get(0).count).isEqualTo(0);

    result = publisher.publish("salutations", "goodbye");
    assertThat(result).isEqualTo(1);
    mockSubscriber2.awaitMessageReceived(1L);
    mockSubscriber2.unsubscribe("salutations");
    waitFor(() -> mockSubscriber2.getSubscribedChannels() == 0);
    waitFor(() -> !subscriber2Thread.isAlive());
    assertThat(mockSubscriber2.unsubscribeInfos).hasSize(1);
    assertThat(mockSubscriber2.unsubscribeInfos.get(0).channel).isEqualTo("salutations");
    assertThat(mockSubscriber2.unsubscribeInfos.get(0).count).isEqualTo(0);

    assertThat(mockSubscriber1.getReceivedMessages()).isEqualTo(Collections.singletonList("hello"));
    assertThat(mockSubscriber2.getReceivedMessages()).isEqualTo(Arrays.asList("hello", "goodbye"));

    subscriber2.close();
  }

  @Test
  public void testPublishToNonexistentChannel() {
    Long result = publisher.publish("thisChannelDoesn'tExist", "hello");
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testOneSubscriberOneChannelTwoTimes() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable1 = () -> subscriber.subscribe(mockSubscriber, "salutations", "salutations");

    Thread subscriberThread = new Thread(runnable1);
    subscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(1);

    mockSubscriber.awaitMessageReceived(1L);
    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);

    waitFor(() -> !subscriberThread.isAlive());
    assertThat(mockSubscriber.unsubscribeInfos)
        .containsExactly(new MockSubscriber.UnsubscribeInfo("salutations", 0));
    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(Collections.singletonList("hello"));
  }

  @Test
  public void testDeadSubscriber() {
    Jedis deadSubscriber = new Jedis("localhost", getPort(), JEDIS_TIMEOUT);

    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      deadSubscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    deadSubscriber.close();

    waitFor(() -> !deadSubscriber.isConnected());
    Long result = publisher.publish("salutations", "hello");

    assertThat(result).isEqualTo(0);
    assertThat(mockSubscriber.getReceivedMessages()).isEmpty();
  }

  @Test
  public void testPatternSubscribe() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.psubscribe(mockSubscriber, "sal*s");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    String message = "hello-" + System.currentTimeMillis();

    Long result = publisher.publish("salutations", message);
    assertThat(result).isEqualTo(1);

    assertThat(mockSubscriber.getReceivedMessages()).isEmpty();
    GeodeAwaitility.await().until(() -> !mockSubscriber.getReceivedPMessages().isEmpty());
    assertThat(mockSubscriber.getReceivedPMessages()).containsExactly(message);

    mockSubscriber.punsubscribe("sal*s");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());
    assertThat(mockSubscriber.punsubscribeInfos)
        .containsExactly(new MockSubscriber.UnsubscribeInfo("sal*s", 0));
  }

  @Test
  public void testSubscribeToSamePattern() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.psubscribe(mockSubscriber, "sal*s");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    mockSubscriber.psubscribe("sal*s");
    GeodeAwaitility.await()
        .during(5, TimeUnit.SECONDS)
        .until(() -> mockSubscriber.getSubscribedChannels() == 1);

    String message = "hello-" + System.currentTimeMillis();

    Long result = publisher.publish("salutations", message);
    assertThat(result).isEqualTo(1);

    assertThat(mockSubscriber.getReceivedMessages()).isEmpty();
    GeodeAwaitility.await().until(() -> !mockSubscriber.getReceivedPMessages().isEmpty());
    assertThat(mockSubscriber.getReceivedPMessages()).containsExactly(message);

    mockSubscriber.punsubscribe("sal*s");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());
    assertThat(mockSubscriber.punsubscribeInfos)
        .containsExactly(new MockSubscriber.UnsubscribeInfo("sal*s", 0));
  }

  @Test
  public void testPatternAndRegularSubscribe() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    mockSubscriber.psubscribe("sal*s");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(2);

    mockSubscriber.awaitMessageReceived(1L);
    mockSubscriber.punsubscribe("sal*s");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    assertThat(mockSubscriber.punsubscribeInfos).hasSize(1);
    assertThat(mockSubscriber.punsubscribeInfos.get(0).channel).isEqualTo("sal*s");
    assertThat(mockSubscriber.punsubscribeInfos.get(0).count).isEqualTo(1);

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);

    waitFor(() -> !subscriberThread.isAlive());
    assertThat(mockSubscriber.unsubscribeInfos).hasSize(1);
    assertThat(mockSubscriber.unsubscribeInfos.get(0).channel).isEqualTo("salutations");
    assertThat(mockSubscriber.unsubscribeInfos.get(0).count).isEqualTo(0);

    assertThat(mockSubscriber.getReceivedMessages()).containsExactly("hello");
    assertThat(mockSubscriber.getReceivedPMessages()).containsExactly("hello");
  }

  @Test
  public void testPatternWithoutAGlob() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    mockSubscriber.psubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(2);

    mockSubscriber.awaitMessageReceived(1L);
    mockSubscriber.punsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);

    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).containsExactly("hello");
    assertThat(mockSubscriber.getReceivedPMessages()).containsExactly("hello");
  }

  @Test
  public void concurrentSubscribers_andPublishers_doesNotHang()
      throws InterruptedException, ExecutionException {
    AtomicBoolean running = new AtomicBoolean(true);

    Future<Integer> makeSubscribersFuture1 =
        executor.submit(() -> makeSubscribers(10000, running));
    Future<Integer> makeSubscribersFuture2 =
        executor.submit(() -> makeSubscribers(10000, running));

    Future<Integer> publish1 = executor.submit(() -> doPublishing(1, 10000, running));
    Future<Integer> publish2 = executor.submit(() -> doPublishing(2, 10000, running));
    Future<Integer> publish3 = executor.submit(() -> doPublishing(3, 10000, running));
    Future<Integer> publish4 = executor.submit(() -> doPublishing(4, 10000, running));
    Future<Integer> publish5 = executor.submit(() -> doPublishing(5, 10000, running));

    running.set(false);

    assertThat(makeSubscribersFuture1.get()).isGreaterThanOrEqualTo(10);
    assertThat(makeSubscribersFuture2.get()).isGreaterThanOrEqualTo(10);

    assertThat(publish1.get()).isGreaterThan(0);
    assertThat(publish2.get()).isGreaterThan(0);
    assertThat(publish3.get()).isGreaterThan(0);
    assertThat(publish4.get()).isGreaterThan(0);
    assertThat(publish5.get()).isGreaterThan(0);
  }

  private Jedis getConnection() {
    Exception lastException = null;

    for (int i = 0; i < 20; i++) {
      Jedis client = null;
      try {
        client = new Jedis("localhost", getPort(), JEDIS_TIMEOUT);
        client.ping();
        return client;
      } catch (Exception e) {
        lastException = e;
        if (client != null) {
          try {
            client.close();
          } catch (Exception ignore) {
          }
        }
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    runit("docker", "ps", "-a");

    throw new RuntimeException("Tried 10 times, but could not get a good connection.",
        lastException);
  }

  // TODO: This exists for debugging some flakiness in this test. If this is fully resolved in the
  // future, please delete this and the associated dependency in build.gradle
  private void runit(String command, String... args) {
    StreamConsumer consumer = stream -> {
      InputStreamReader inputStreamReader = new InputStreamReader(stream);
      BufferedReader bufReader = new BufferedReader(inputStreamReader);
      String line;
      while ((line = bufReader.readLine()) != null) {
        System.err.println("::::: " + line);
      }
    };

    new ProcBuilder(command)
        .withArgs(args)
        .withOutputConsumer(consumer)
        .withTimeoutMillis(60000)
        .run();
  }

  int doPublishing(int index, int minimumIterations, AtomicBoolean running) {
    int iterationCount = 0;
    int publishedMessages = 0;
    Jedis client = getConnection();
    try {
      while (iterationCount < minimumIterations || running.get()) {
        publishedMessages += client.publish("my-channel", "boo-" + index + "-" + iterationCount);
        iterationCount++;
      }
    } finally {
      client.close();
    }

    return publishedMessages;
  }

  int makeSubscribers(int minimumIterations, AtomicBoolean running)
      throws InterruptedException, ExecutionException {
    ExecutorService executor = Executors.newFixedThreadPool(100);
    Queue<Future<Void>> workQ = new ConcurrentLinkedQueue<>();

    Future<Integer> consumer = executor.submit(() -> {
      int subscribersProcessed = 0;
      while (subscribersProcessed < minimumIterations || running.get()) {
        if (workQ.isEmpty()) {
          Thread.yield();
          continue;
        }
        Future<Void> f = workQ.poll();
        try {
          f.get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        subscribersProcessed++;
      }
      return subscribersProcessed;
    });

    int iterationCount = 0;
    String channel = "my-channel";
    while (iterationCount < minimumIterations || running.get()) {
      Future<Void> f = executor.submit(() -> {
        Jedis client = getConnection();
        ExecutorService secondaryExecutor = Executors.newSingleThreadExecutor();
        MockSubscriber mockSubscriber = new MockSubscriber();
        AtomicReference<Thread> innerThread = new AtomicReference<>();
        Future<Void> inner = secondaryExecutor.submit(() -> {
          innerThread.set(Thread.currentThread());
          client.subscribe(mockSubscriber, channel);
          return null;
        });
        mockSubscriber.awaitSubscribe(channel);
        if (inner.isDone()) {
          throw new RuntimeException("inner completed before unsubscribe");
        }

        mockSubscriber.unsubscribe(channel);

        try {
          inner.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
          LogService.getLogger().debug("=> {} {}", innerThread.get(), innerThread.get().getState());
          for (StackTraceElement st : innerThread.get().getStackTrace()) {
            LogService.getLogger().debug("-> {}", st);
          }
          throw new RuntimeException("inner.get() errored after unsubscribe: " + e.getMessage());
        }

        mockSubscriber.awaitUnsubscribe(channel);
        client.close();
        secondaryExecutor.shutdownNow();

        return null;
      });
      workQ.add(f);
      iterationCount++;
    }

    int result = consumer.get();
    executor.shutdownNow();

    return result;
  }

  private void waitFor(Callable<Boolean> booleanCallable) {
    GeodeAwaitility.await()
        .ignoreExceptions() // ignoring socket closed exceptions
        .until(booleanCallable);
  }
}
