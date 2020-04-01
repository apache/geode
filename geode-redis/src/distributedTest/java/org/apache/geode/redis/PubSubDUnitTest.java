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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class PubSubDUnitTest {

  public static final String CHANNEL_NAME = "salutations";

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static int[] ports;

  private static MemberVM server1, server2;

  @BeforeClass
  public static void beforeClass() {
    ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    Properties redisProps = new Properties();
    redisProps.put(ConfigurationProperties.REDIS_BIND_ADDRESS, "localhost");

    MemberVM locator = cluster.startLocatorVM(0);

    redisProps.put(ConfigurationProperties.REDIS_PORT, Integer.toString(ports[0]));
    server1 = cluster.startServerVM(1, redisProps, locator.getPort());
    redisProps.put(ConfigurationProperties.REDIS_PORT, Integer.toString(ports[1]));
    server2 = cluster.startServerVM(2, redisProps, locator.getPort());
  }

  @Test
  public void testSubscribePublishUsingDifferentServers() throws Exception {
    Jedis subscriber1 = new Jedis("localhost", ports[0]);
    Jedis subscriber2 = new Jedis("localhost", ports[1]);
    Jedis publisher = new Jedis("localhost", ports[1]);

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

    Long result = publisher.publish(CHANNEL_NAME, "hello");
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

    Jedis subscriber1 = new Jedis("localhost", ports[0]);
    Jedis subscriber2 = new Jedis("localhost", ports[1]);

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
      Jedis client = new Jedis("localhost", ports[i % 2]);

      Callable<Void> callable = () -> {
        for (int j = 0; j < ITERATIONS; j++) {
          client.publish(CHANNEL_NAME, "hello");
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

}
