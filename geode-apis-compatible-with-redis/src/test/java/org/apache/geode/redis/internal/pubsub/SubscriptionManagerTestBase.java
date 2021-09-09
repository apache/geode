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
package org.apache.geode.redis.internal.pubsub;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public abstract class SubscriptionManagerTestBase {

  @Test
  public void defaultManagerIsEmpty() {
    assertThat(createManager().getSubscriptionCount()).isZero();
  }

  @Test
  public void defaultManager_getIds_returnsEmptyList() {
    assertThat(createManager().getIds()).isEmpty();
  }

  @Test
  public void defaultManager_getIdsWithPattern_returnsEmptyList() {
    assertThat(createManager().getIds(stringToBytes("*"))).isEmpty();
  }

  @Test
  public void defaultManager_countWithChannel_isZero() {
    assertThat(createManager().getSubscriptionCount(stringToBytes("channel"))).isZero();
  }

  @Test
  public void defaultManager_removeWithClient_doesNothing() {
    AbstractSubscriptionManager<?> manager = createManager();
    manager.remove(createClient());
    assertThat(manager.getSubscriptionCount()).isZero();
  }

  @Test
  public void defaultManager_removeWithClientAndChannel_doesNothing() {
    byte[] channel = stringToBytes("channel");
    AbstractSubscriptionManager<?> manager = createManager();
    manager.remove(channel, createClient());
    assertThat(manager.getSubscriptionCount()).isZero();
  }

  @Test
  public void defaultManager_foreach_doesNothing() {
    byte[] channel = stringToBytes("channel");
    AtomicInteger count = new AtomicInteger();
    AbstractSubscriptionManager<?> manager = createManager();
    manager.foreachSubscription(channel, (name, client, sub) -> count.getAndIncrement());
    assertThat(count.get()).isZero();
  }

  @Test
  public void managerWithMultipleSubsAndClients_foreachDoesExpectedIterations() {
    byte[] channel1 = stringToBytes("channel1");
    byte[] channel2 = stringToBytes("channel2");
    byte[] channel3 = stringToBytes("channel3");
    AtomicInteger count = new AtomicInteger();
    Client client1 = createClient();
    Client client2 = createClient();
    Client client3 = createClient();
    AbstractSubscriptionManager<?> manager = createManager();
    manager.add(channel1, client1);
    manager.add(channel1, client2);
    manager.add(channel1, client3);
    manager.add(channel2, client2);
    manager.add(channel3, client3);
    manager.foreachSubscription(channel1, (name, client, sub) -> count.getAndIncrement());
    assertThat(count.get()).isEqualTo(3);
  }

  @Test
  public void managerWithOneSub_hasCorrectCounts() {
    AbstractSubscriptionManager<?> manager = createManager(1);
    assertThat(manager.getSubscriptionCount()).isOne();
    assertThat(manager.getSubscriptionCount(stringToBytes("channel1"))).isOne();
  }

  @Test
  public void managerWithOneSub_hasCorrectIds() {
    AbstractSubscriptionManager<?> manager = createManager(1);
    assertThat(manager.getIds()).containsExactlyInAnyOrder(stringToBytes("channel1"));
    assertThat(manager.getIds(stringToBytes("*")))
        .containsExactlyInAnyOrder(stringToBytes("channel1"));
  }

  @Test
  public void managerWithMultipleSubs_hasCorrectIds() {
    AbstractSubscriptionManager<?> manager = createManager(2);
    assertThat(manager.getIds()).containsExactlyInAnyOrder(stringToBytes("channel1"),
        stringToBytes("channel2"));
    assertThat(manager.getIds(stringToBytes("*")))
        .containsExactlyInAnyOrder(stringToBytes("channel1"), stringToBytes("channel2"));
  }

  @Test
  public void managerWithSubs_isEmpty_afterClientRemove() {
    Client client = createClient();
    AbstractSubscriptionManager<?> manager = createManager(3, client);

    manager.remove(client);

    assertThat(manager.getSubscriptionCount()).isZero();
  }

  @Test
  public void managerWithOneSub_isEmpty_afterRemove() {
    Client client = createClient();
    AbstractSubscriptionManager<?> manager = createManager(1, client);
    byte[] channel = stringToBytes("channel1");

    manager.remove(channel, client);

    assertThat(manager.getSubscriptionCount()).isZero();
  }

  @Test
  public void addingDuplicateDoesNothing() {
    Client client = createClient();
    AbstractSubscriptionManager<?> manager = createManager(1, client);
    byte[] channel = stringToBytes("channel1");

    Object result = manager.add(channel, client);

    assertThat(manager.getSubscriptionCount()).isOne();
    assertThat(result).isNull();
  }

  @Test
  public void addingTwoSubscriptionsWithDifferentClients() {
    Client client1 = createClient();
    Client client2 = createClient();
    AbstractSubscriptionManager<?> manager = createManager();
    byte[] channel = stringToBytes("channel");

    Object result1 = manager.add(channel, client1);
    Object result2 = manager.add(channel, client2);

    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
    assertThat(result1).isNotNull();
    assertThat(result2).isNotNull();
    assertThat(manager.getSubscriptionCount(channel)).isEqualTo(2);
    assertThat(manager.getIds()).containsExactlyInAnyOrder(channel);
    assertThat(manager.getIds(stringToBytes("ch*"))).containsExactlyInAnyOrder(channel);
  }

  protected Client createClient() {
    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    return new Client(channel, mock(PubSub.class));
  }

  protected AbstractSubscriptionManager<?> createManager(int subCount, Client client) {
    AbstractSubscriptionManager<?> manager = createManager();
    for (int i = 1; i <= subCount; i++) {
      byte[] channel = stringToBytes("channel" + i);
      manager.add(channel, client);
    }
    return manager;
  }

  protected AbstractSubscriptionManager<?> createManager(int subCount) {
    return createManager(subCount, createClient());
  }

  protected abstract AbstractSubscriptionManager<?> createManager();
}
