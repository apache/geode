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

package org.apache.geode.redis.internal.executor.publishAndSubscribe;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractSubscriptionsIntegrationTest implements RedisIntegrationTest {

  public static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private Jedis client;

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Before
  public void setUp() {
    client = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    client.close();
  }

  @Test
  public void pingWhileSubscribed() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    executor.submit(() -> client.subscribe(mockSubscriber, "same"));
    mockSubscriber.awaitSubscribe("same");
    mockSubscriber.ping();
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedPings().size()).isEqualTo(1));
    assertThat(mockSubscriber.getReceivedPings().get(0)).isEqualTo("");
    mockSubscriber.unsubscribe();
  }

  @Test
  public void pingWithArgumentWhileSubscribed() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    executor.submit(() -> client.subscribe(mockSubscriber, "same"));
    mockSubscriber.awaitSubscribe("same");
    mockSubscriber.ping("potato");
    // JedisPubSub PING with message is not currently possible, will submit a PR
    // (https://github.com/xetorthio/jedis/issues/2049)
    // until then, we have to call this second ping to flush the client
    mockSubscriber.ping();
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedPings().size()).isEqualTo(2));
    assertThat(mockSubscriber.getReceivedPings().get(0)).isEqualTo("potato");
    mockSubscriber.unsubscribe();
  }

  @Test
  public void unallowedCommandsWhileSubscribed() {
    client.sendCommand(Protocol.Command.SUBSCRIBE, "hello");

    assertThatThrownBy(() -> client.set("not", "supported")).hasMessageContaining(
        "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
    client.sendCommand(Protocol.Command.UNSUBSCRIBE);
  }

  @Test
  public void multiSubscribe() {
    MockSubscriber mockSubscriber = new MockSubscriber();

    executor.submit(() -> client.subscribe(mockSubscriber, "same"));
    mockSubscriber.awaitSubscribe("same");
    mockSubscriber.psubscribe("sam*");
    mockSubscriber.awaitPSubscribe("sam*");

    Jedis publisher = new Jedis("localhost", getPort());
    long publishCount = publisher.publish("same", "message");

    assertThat(publishCount).isEqualTo(2L);
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedMessages()).hasSize(1));
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedPMessages()).hasSize(1));
    assertThat(mockSubscriber.getReceivedEvents()).containsExactly("message", "pmessage");
    mockSubscriber.unsubscribe();
  }

  @Test
  public void unsubscribingImplicitlyFromAllChannels_doesNotUnsubscribeFromPatterns() {
    Jedis publisher = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    MockSubscriber mockSubscriber = new MockSubscriber();

    executor.submit(() -> client.subscribe(mockSubscriber, "salutations", "yuletide"));
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(2));
    mockSubscriber.psubscribe("p*", "g*");
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(4));

    mockSubscriber.unsubscribe();
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(2));

    List<String> unsubscribedChannels = mockSubscriber.unsubscribeInfos.stream()
        .map(x -> x.channel)
        .collect(Collectors.toList());
    assertThat(unsubscribedChannels).containsExactlyInAnyOrder("salutations", "yuletide");
    List<Integer> channelCounts = mockSubscriber.unsubscribeInfos.stream()
        .map(x -> x.count)
        .collect(Collectors.toList());
    assertThat(channelCounts).containsExactlyInAnyOrder(3, 2);

    Long result = publisher.publish("salutations", "greetings");
    assertThat(result).isEqualTo(0);
    result = publisher.publish("potato", "potato");
    assertThat(result).isEqualTo(1);
    result = publisher.publish("gnochi", "fresh");
    assertThat(result).isEqualTo(1);

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedPMessages()).hasSize(2));
    assertThat(mockSubscriber.getReceivedPMessages()).containsExactlyInAnyOrder("potato", "fresh");
    assertThat(mockSubscriber.getReceivedMessages()).isEmpty();

    mockSubscriber.punsubscribe();
    publisher.close();
  }


  @Test
  public void punsubscribingImplicitlyFromAllPatterns_doesNotUnsubscribeFromChannels() {
    Jedis publisher = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    MockSubscriber mockSubscriber = new MockSubscriber();

    executor.submit(() -> client.psubscribe(mockSubscriber, "p*", "g*"));
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(2));
    mockSubscriber.subscribe("salutations", "yuletide");
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(4));

    mockSubscriber.punsubscribe();
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getSubscribedChannels()).isEqualTo(2));

    List<String> punsubscribedChannels = mockSubscriber.punsubscribeInfos.stream()
        .map(x -> x.channel)
        .collect(Collectors.toList());
    assertThat(punsubscribedChannels).containsExactlyInAnyOrder("p*", "g*");

    List<Integer> channelCounts = mockSubscriber.punsubscribeInfos.stream()
        .map(x -> x.count)
        .collect(Collectors.toList());
    assertThat(channelCounts).containsExactlyInAnyOrder(3, 2);

    Long result = publisher.publish("potato", "potato");
    assertThat(result).isEqualTo(0);
    result = publisher.publish("salutations", "greetings");
    assertThat(result).isEqualTo(1);
    result = publisher.publish("yuletide", "tidings");
    assertThat(result).isEqualTo(1);

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedMessages()).hasSize(2));
    assertThat(mockSubscriber.getReceivedMessages()).containsExactlyInAnyOrder("greetings",
        "tidings");
    assertThat(mockSubscriber.getReceivedPMessages()).isEmpty();

    mockSubscriber.unsubscribe();
    publisher.close();
  }

}
