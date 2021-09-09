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


public class ChannelSubscriptionManagerTest {

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
    manager.foreachSubscription(channel, sub -> {
      count.getAndIncrement();
    });
    assertThat(count.get()).isZero();
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
  public void managerWithSubs_isEmpty_afterClientRemove() {
    Client client = createClient();
    AbstractSubscriptionManager<?> manager = createManager(3, client);

    manager.remove(client);

    assertThat(manager.getSubscriptionCount()).isZero();
  }

  private Client createClient() {
    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    return new Client(channel, mock(PubSub.class));
  }

  private AbstractSubscriptionManager<?> createManager() {
    return new ChannelSubscriptionManager();
  }

  private AbstractSubscriptionManager<?> createManager(int subCount) {
    return createManager(subCount, createClient());
  }

  private AbstractSubscriptionManager<?> createManager(int subCount, Client client) {
    ChannelSubscriptionManager manager = new ChannelSubscriptionManager();
    for (int i = 1; i <= subCount; i++) {
      byte[] channel = stringToBytes("channel" + i);
      manager.add(channel, client);
    }
    return manager;
  }


}
