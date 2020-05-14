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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.mocks.MockBinarySubscriber;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.RedisTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category({RedisTest.class})
public class PubSubIntegrationTest {
  static Jedis publisher;
  static Jedis subscriber;
  static final int REDIS_CLIENT_TIMEOUT = 100000;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void setUp() {
    subscriber = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
    publisher = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void tearDown() {
    subscriber.close();
    publisher.close();
  }

  public int getPort() {
    return server.getPort();
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

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages);
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

    mockSubscriber.unsubscribe("salutations".getBytes());
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages().get(0)).isEqualTo(expectedMessage);
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
    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    mockSubscriber.unsubscribe("yuletide");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages);
  }

  @Test
  public void testTwoSubscribersOneChannel() {
    Jedis subscriber2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
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
    mockSubscriber1.unsubscribe("salutations");
    waitFor(() -> mockSubscriber1.getSubscribedChannels() == 0);
    waitFor(() -> !subscriber1Thread.isAlive());

    result = publisher.publish("salutations", "goodbye");
    assertThat(result).isEqualTo(1);
    mockSubscriber2.unsubscribe("salutations");
    waitFor(() -> mockSubscriber2.getSubscribedChannels() == 0);
    waitFor(() -> !subscriber2Thread.isAlive());

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

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);

    waitFor(() -> !subscriberThread.isAlive());
    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(Collections.singletonList("hello"));
  }

  @Test
  public void testDeadSubscriber() {
    Jedis deadSubscriber = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);

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

    mockSubscriber.punsubscribe("sal*s");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);

    waitFor(() -> !subscriberThread.isAlive());

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

    mockSubscriber.punsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);

    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).containsExactly("hello");
    assertThat(mockSubscriber.getReceivedPMessages()).containsExactly("hello");
  }

  private void waitFor(Callable<Boolean> booleanCallable) {
    GeodeAwaitility.await()
        .ignoreExceptions() // ignoring socket closed exceptions
        .until(booleanCallable);
  }
}
