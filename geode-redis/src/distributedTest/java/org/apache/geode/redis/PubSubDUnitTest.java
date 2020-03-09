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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PubSubDUnitTest {

  public static final String CHANNEL_NAME = "salutations";
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static int[] ports;

  private static MemberVM server1, server2;

  @BeforeClass
  public static void beforeClass() {
    ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    Properties redisProps = new Properties();
    redisProps.put("redis-bind-address", "localhost");

    MemberVM locator = cluster.startLocatorVM(0);

    redisProps.put("redis-port", Integer.toString(ports[0]));
    server1 = cluster.startServerVM(1, redisProps, locator.getPort());
    redisProps.put("redis-port", Integer.toString(ports[1]));
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

    Runnable runnable1 = () -> subscriber1.subscribe(mockSubscriber1, CHANNEL_NAME);
    Thread subscriberThread1 = new Thread(runnable1);
    subscriberThread1.start();

    Runnable runnable2 = () -> subscriber2.subscribe(mockSubscriber2, CHANNEL_NAME);
    Thread subscriberThread2 = new Thread(runnable2);
    subscriberThread2.start();

    assertThat(latch.await(30, TimeUnit.SECONDS))
        .as("channel subscription was not received")
        .isTrue();

    Long result = publisher.publish(CHANNEL_NAME, "hello");
    assertThat(result).isEqualTo(2);

    mockSubscriber1.unsubscribe(CHANNEL_NAME);
    mockSubscriber2.unsubscribe(CHANNEL_NAME);

    GeodeAwaitility.await().untilAsserted(subscriberThread1::join);
    GeodeAwaitility.await().untilAsserted(subscriberThread2::join);
  }

}
