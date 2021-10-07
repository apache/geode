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

package org.apache.geode.redis.internal.pubsub;


import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.netty.Client;

public class SubscriptionsIntegrationTest {

  private static final int ITERATIONS = 1000;

  private final Subscriptions subscriptions = new Subscriptions();

  private final AtomicInteger channelCount = new AtomicInteger();

  private byte[] createChannel() {
    int myChannelCount = channelCount.incrementAndGet();
    return stringToBytes("channel-" + myChannelCount);
  }

  private Client createClient() {
    Client client = mock(Client.class);
    when(client.addChannelSubscription(any())).thenReturn(true);
    return client;
  }

  @Test
  public void add_doesNotThrowException_whenListIsConcurrentlyModified() {
    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.addChannel(createChannel(), createClient()),
        i -> subscriptions.addChannel(createChannel(), createClient()))
            .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS * 2);
  }

  @Test
  public void getChannelSubscriptionCount_doesNotThrowException_whenListIsConcurrentlyModified() {
    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.addChannel(createChannel(), createClient()),
        i -> subscriptions
            .getChannelSubscriptionCount(stringToBytes("channel-" + channelCount.get())))
                .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void findChannelNames_doesNotThrowException_whenListIsConcurrentlyModified() {
    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.addChannel(createChannel(), createClient()),
        i -> subscriptions.findChannelNames())
            .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void findChannelNames_withPattern_doesNotThrowException_whenListIsConcurrentlyModified() {
    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.addChannel(createChannel(), createClient()),
        i -> subscriptions.findChannelNames(stringToBytes("channel-*")))
            .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void removeByClient_doesNotThrowException_whenListIsConcurrentlyModified() {
    final Subscriptions subscriptions = new Subscriptions();

    List<Client> clients = new LinkedList<>();
    for (int i = 0; i < ITERATIONS; i++) {
      Channel channel = mock(Channel.class);
      when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
      Client client = new Client(channel, mock(PubSub.class));
      clients.add(client);
      subscriptions.addChannel(stringToBytes("channel"), client);
    }

    new ConcurrentLoopingThreads(1,
        i -> clients.forEach(subscriptions::remove),
        i -> subscriptions.getChannelSubscriptionCount(stringToBytes("channel")))
            .run();

    assertThat(subscriptions.size()).isEqualTo(0);
  }

  @Test
  public void unsubscribeByChannelAndClient_doesNotThrowException_whenListIsConcurrentlyModified() {
    List<Client> clients = new LinkedList<>();
    for (int i = 0; i < ITERATIONS; i++) {
      Channel channel = mock(Channel.class);
      when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
      Client client = new Client(channel, mock(PubSub.class));
      clients.add(client);
      subscriptions.addChannel(stringToBytes("channel"), client);
    }

    new ConcurrentLoopingThreads(1,
        i -> clients
            .forEach(c -> subscriptions.unsubscribe(singletonList(stringToBytes("channel")), c)),
        i -> clients
            .forEach(c -> subscriptions.unsubscribe(singletonList(stringToBytes("channel")), c)))
                .run();

    assertThat(subscriptions.size()).isEqualTo(0);
  }
}
