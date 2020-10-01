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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractSubscriptionsIntegrationTest implements RedisPortSupplier {

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Test
  public void pingWhileSubscribed() {
    Jedis client = new Jedis("localhost", getPort());
    MockSubscriber mockSubscriber = new MockSubscriber();

    executor.submit(() -> client.subscribe(mockSubscriber, "same"));
    mockSubscriber.awaitSubscribe("same");
    mockSubscriber.ping();
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mockSubscriber.getReceivedPings().size()).isEqualTo(1));
    assertThat(mockSubscriber.getReceivedPings().get(0)).isEqualTo("");
    mockSubscriber.unsubscribe();
    client.close();
  }

  @Test
  public void pingWithArgumentWhileSubscribed() {
    Jedis client = new Jedis("localhost", getPort());
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
    client.close();
  }

  @Test
  public void multiSubscribe() {
    Jedis client = new Jedis("localhost", getPort());
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
    client.close();
  }

  @Test
  public void unallowedCommandsWhileSubscribed() {
    Jedis client = new Jedis("localhost", getPort());

    client.sendCommand(Protocol.Command.SUBSCRIBE, "hello");

    assertThatThrownBy(() -> client.set("not", "supported")).hasMessageContaining(
        "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
    client.sendCommand(Protocol.Command.UNSUBSCRIBE);
    client.close();
  }
}
