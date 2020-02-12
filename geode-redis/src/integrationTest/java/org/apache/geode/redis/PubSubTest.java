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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class PubSubTest {
  private static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Random rand;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() {
    rand = new Random();
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }

  @Test
  public void testOneSubscriberOneChannel() throws InterruptedException {
    Jedis subscriber = new Jedis("localhost", port);
    Jedis publisher = new Jedis("localhost", port);
    List<String> expectedMessages = Arrays.asList("hello");

    CountDownLatch latch = new CountDownLatch(1);
    MockSubscriber mockSubscriber = new MockSubscriber(latch);

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    assertThat(latch.await(2, TimeUnit.SECONDS))
        .as("channel subscriptions were not received")
        .isTrue();

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(1);

    mockSubscriber.unsubscribe("salutations");

    subscriberThread.join(2000);

    assertThat(subscriberThread.isAlive())
        .as("subscriber thread should not be alive")
        .isFalse();

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages);
  }

  @Test
  public void testOneSubscriberSubscribingToTwoChannels() throws Exception {
    Jedis subscriber = new Jedis("localhost", port);
    Jedis publisher = new Jedis("localhost", port);
    List<String> expectedMessages = Arrays.asList("hello", "howdy");
    CountDownLatch latch = new CountDownLatch(2);
    MockSubscriber mockSubscriber = new MockSubscriber(latch);

    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "salutations", "yuletide");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    assertThat(latch.await(2, TimeUnit.SECONDS))
        .as("channel subscriptions were not received")
        .isTrue();

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(1);

    result = publisher.publish("yuletide", "howdy");
    assertThat(result).isEqualTo(1);
    mockSubscriber.unsubscribe("salutations");
    mockSubscriber.unsubscribe("yuletide");

    subscriberThread.join(2000);

    assertThat(subscriberThread.isAlive())
        .as("subscriber thread should not be alive")
        .isFalse();

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages);
  }

  @Test
  public void testTwoSubscribersOneChannel() throws Exception {
    Jedis subscriber1 = new Jedis("localhost", port);
    Jedis subscriber2 = new Jedis("localhost", port);
    Jedis publisher = new Jedis("localhost", port);
    CountDownLatch latch = new CountDownLatch(2);
    MockSubscriber mockSubscriber1 = new MockSubscriber(latch);
    MockSubscriber mockSubscriber2 = new MockSubscriber(latch);

    Runnable runnable1 = () -> subscriber1.subscribe(mockSubscriber1, "salutations");

    Runnable runnable2 = () -> subscriber2.subscribe(mockSubscriber2, "salutations");

    Thread subscriber1Thread = new Thread(runnable1);
    subscriber1Thread.start();
    Thread subscriber2Thread = new Thread(runnable2);
    subscriber2Thread.start();

    assertThat(latch.await(2, TimeUnit.SECONDS))
        .as("channel subscriptions were not received")
        .isTrue();

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(2);
    mockSubscriber1.unsubscribe("salutations");

    subscriber1Thread.join(2000);

    assertThat(subscriber1Thread.isAlive())
        .as("subscriber1 thread should not be alive")
        .isFalse();

    result = publisher.publish("salutations", "goodbye");
    assertThat(result).isEqualTo(1);
    mockSubscriber2.unsubscribe("salutations");

    subscriber2Thread.join(2000);
    assertThat(subscriber2Thread.isAlive())
        .as("subscriber2 thread should not be alive")
        .isFalse();

    assertThat(mockSubscriber1.getReceivedMessages()).isEqualTo(Collections.singletonList("hello"));
    assertThat(mockSubscriber2.getReceivedMessages()).isEqualTo(Arrays.asList("hello", "goodbye"));
  }

  @Test
  public void testPublishToNonexistentChannel() throws Exception {
    Jedis publisher = new Jedis("localhost", port);
    Long result = publisher.publish("thisChannelDoesn'tExist", "hello");
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testOneSubscriberOneChannelTwoTimes() throws Exception {
    Jedis subscriber = new Jedis("localhost", port);
    Jedis publisher = new Jedis("localhost", port);
    CountDownLatch latch = new CountDownLatch(2);
    MockSubscriber mockSubscriber = new MockSubscriber(latch);

    Runnable runnable1 = () -> subscriber.subscribe(mockSubscriber, "salutations", "salutations");

    Thread subscriberThread = new Thread(runnable1);
    subscriberThread.start();

    assertThat(latch.await(2, TimeUnit.SECONDS))
        .as("channel subscriptions were not received")
        .isTrue();
    assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(1);

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(1);

    mockSubscriber.unsubscribe("salutations");

    subscriberThread.join(2000);

    assertThat(subscriberThread.isAlive())
        .as("subscriber1 thread should not be alive")
        .isFalse();

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(Collections.singletonList("hello"));
  }

  @Test
  public void testDeadSubscriber() throws InterruptedException {
    Jedis subscriber = new Jedis("localhost", port);
    Jedis publisher = new Jedis("localhost", port);

    CountDownLatch latch = new CountDownLatch(1);
    MockSubscriber mockSubscriber = new MockSubscriber(latch);

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();

    assertThat(latch.await(2, TimeUnit.SECONDS))
        .as("channel subscriptions were not received")
        .isTrue();

    subscriber.close();
    subscriberThread.join(2000);

    assertThat(subscriberThread.isAlive())
        .as("subscriber thread should not be alive")
        .isFalse();

    Long result = publisher.publish("salutations", "hello");
    assertThat(result).isEqualTo(0);

    assertThat(mockSubscriber.getReceivedMessages()).isEmpty();
  }
}
