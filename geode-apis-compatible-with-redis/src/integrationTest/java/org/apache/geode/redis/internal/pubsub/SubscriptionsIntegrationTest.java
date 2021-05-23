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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.mocks.DummySubscription;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class SubscriptionsIntegrationTest {

  private static final int ITERATIONS = 1000;

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Test
  public void add_doesNotThrowException_whenListIsConcurrentlyModified() {
    final Subscriptions subscriptions = new Subscriptions();

    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.add(new DummySubscription()),
        i -> subscriptions.add(new DummySubscription()))
            .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS * 2);
  }

  @Test
  public void exists_doesNotThrowException_whenListIsConcurrentlyModified() {
    final Subscriptions subscriptions = new Subscriptions();

    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.add(new DummySubscription()),
        i -> subscriptions.exists("channel", mock(Client.class)))
            .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void findSubscriptionsByClient_doesNotThrowException_whenListIsConcurrentlyModified() {
    final Subscriptions subscriptions = new Subscriptions();

    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.add(new DummySubscription()),
        i -> subscriptions.findSubscriptions(mock(Client.class)))
            .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void findSubscriptionsByChannel_doesNotThrowException_whenListIsConcurrentlyModified() {
    final Subscriptions subscriptions = new Subscriptions();

    new ConcurrentLoopingThreads(ITERATIONS,
        i -> subscriptions.add(new DummySubscription()),
        i -> subscriptions.findSubscriptions("channel".getBytes()))
            .run();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void removeByClient_doesNotThrowException_whenListIsConcurrentlyModified() {
    final Subscriptions subscriptions = new Subscriptions();

    List<Client> clients = new LinkedList<>();
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    for (int i = 0; i < ITERATIONS; i++) {
      Channel channel = mock(Channel.class);
      when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
      Client client = new Client(channel);
      clients.add(client);
      subscriptions
          .add(new ChannelSubscription(client, "channel".getBytes(), context, subscriptions));
    }

    new ConcurrentLoopingThreads(1,
        i -> clients.forEach(subscriptions::remove),
        i -> clients.forEach(c -> subscriptions.exists("channel", c)))
            .run();

    assertThat(subscriptions.size()).isEqualTo(0);
  }

  @Test
  public void removeByChannelAndClient_doesNotThrowException_whenListIsConcurrentlyModified() {
    final Subscriptions subscriptions = new Subscriptions();

    List<Client> clients = new LinkedList<>();
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    for (int i = 0; i < ITERATIONS; i++) {
      Channel channel = mock(Channel.class);
      when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
      Client client = new Client(channel);

      clients.add(client);
      subscriptions
          .add(new ChannelSubscription(client, "channel".getBytes(), context, subscriptions));
    }

    new ConcurrentLoopingThreads(1,
        i -> clients.forEach(c -> subscriptions.remove("channel".getBytes(), c)),
        i -> clients.forEach(c -> subscriptions.remove("channel".getBytes(), c)))
            .run();

    assertThat(subscriptions.size()).isEqualTo(0);
  }
}
