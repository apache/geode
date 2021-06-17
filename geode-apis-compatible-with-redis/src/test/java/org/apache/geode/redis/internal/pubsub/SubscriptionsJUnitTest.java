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

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SubscriptionsJUnitTest {

  @Test
  public void correctlyIdentifiesChannelSubscriber() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    subscriptions
        .add(new ChannelSubscription(client, stringToBytes("subscriptions"), context,
            subscriptions));

    assertThat(subscriptions.exists(stringToBytes("subscriptions"), client)).isTrue();
    assertThat(subscriptions.exists(stringToBytes("unknown"), client)).isFalse();
  }

  @Test
  public void correctlyIdentifiesPatternSubscriber() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    GlobPattern pattern = new GlobPattern("sub*s");

    subscriptions.add(new PatternSubscription(client, pattern, context, subscriptions));

    assertThat(subscriptions.exists(pattern, client)).isTrue();
  }

  @Test
  public void doesNotMisidentifyChannelAsPattern() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    GlobPattern globPattern1 = new GlobPattern("sub*s");
    GlobPattern globPattern2 = new GlobPattern("subscriptions");

    subscriptions
        .add(new ChannelSubscription(client, stringToBytes("subscriptions"), context,
            subscriptions));

    assertThat(subscriptions.exists(globPattern1, client)).isFalse();
    assertThat(subscriptions.exists(globPattern2, client)).isFalse();
  }

  @Test
  public void doesNotMisidentifyWhenBothTypesArePresent() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    GlobPattern globby = new GlobPattern("sub*s");

    subscriptions
        .add(new ChannelSubscription(client, stringToBytes("subscriptions"), context,
            subscriptions));
    subscriptions.add(new PatternSubscription(client, globby, context, subscriptions));

    assertThat(subscriptions.exists(globby, client)).isTrue();
    assertThat(subscriptions.exists(stringToBytes("subscriptions"), client)).isTrue();
  }


  @Test
  public void verifyDifferentMockChannelsNotEqual() {
    Channel mockChannelOne = mock(Channel.class);
    Channel mockChannelTwo = mock(Channel.class);

    assertThat(mockChannelOne).isNotEqualTo(mockChannelTwo);
    assertThat(mockChannelOne.equals(mockChannelTwo)).isFalse();
  }

  @Test
  public void findSubscribers() {
    Subscriptions subscriptions = new Subscriptions();

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Channel mockChannelOne = mock(Channel.class);
    Channel mockChannelTwo = mock(Channel.class);
    when(mockChannelOne.closeFuture()).thenReturn(mock(ChannelFuture.class));
    when(mockChannelTwo.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client clientOne = new Client(mockChannelOne);
    Client clientTwo = new Client(mockChannelTwo);

    ChannelSubscription subscriptionOne =
        new ChannelSubscription(clientOne, stringToBytes("subscriptions"), context,
            subscriptions);
    ChannelSubscription subscriptionTwo =
        new ChannelSubscription(clientTwo, stringToBytes("monkeys"), context, subscriptions);

    subscriptions.add(subscriptionOne);
    subscriptions.add(subscriptionTwo);

    assertThat(subscriptions.findSubscriptions(clientOne)).containsExactly(subscriptionOne);
  }

  @Test
  public void removeByClient() {
    Subscriptions subscriptions = new Subscriptions();

    Channel mockChannelOne = mock(Channel.class);
    Channel mockChannelTwo = mock(Channel.class);

    when(mockChannelOne.closeFuture()).thenReturn(mock(ChannelFuture.class));
    when(mockChannelTwo.closeFuture()).thenReturn(mock(ChannelFuture.class));

    Client clientOne = new Client(mockChannelOne);
    Client clientTwo = new Client(mockChannelTwo);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    ChannelSubscription subscriptionOne =
        new ChannelSubscription(clientOne, stringToBytes("subscriptions"), context,
            subscriptions);
    ChannelSubscription subscriptionTwo =
        new ChannelSubscription(clientTwo, stringToBytes("monkeys"), context, subscriptions);

    subscriptions.add(subscriptionOne);
    subscriptions.add(subscriptionTwo);

    subscriptions.remove(clientOne);

    assertThat(subscriptions.findSubscriptions(clientOne)).isEmpty();
    assertThat(subscriptions.findSubscriptions(clientTwo)).containsExactly(subscriptionTwo);
  }

  @Test
  public void removeByClientAndPattern() {

    Subscriptions subscriptions = new Subscriptions();
    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    ChannelSubscription channelSubscriberOne =
        new ChannelSubscription(client, stringToBytes("subscriptions"), context,
            subscriptions);
    GlobPattern pattern = new GlobPattern("monkeys");
    PatternSubscription patternSubscriber = new PatternSubscription(client,
        pattern, context, subscriptions);
    ChannelSubscription channelSubscriberTwo =
        new ChannelSubscription(client, stringToBytes("monkeys"), context, subscriptions);

    subscriptions.add(channelSubscriberOne);
    subscriptions.add(patternSubscriber);
    subscriptions.add(channelSubscriberTwo);

    subscriptions.remove(pattern, client);

    assertThat(subscriptions
        .findSubscriptions(client))
            .containsExactlyInAnyOrder(
                channelSubscriberOne,
                channelSubscriberTwo);
  }

  @Test
  public void findChannelNames_shouldReturnAllChannelNames_whenCalledWithoutParameter() {
    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo =
        new ChannelSubscription(client, stringToBytes("foo"), context, subject);

    Subscription subscriptionBar =
        new ChannelSubscription(client, stringToBytes("bar"), context, subject);

    subject.add(subscriptionFoo);
    subject.add(subscriptionBar);

    List<byte[]> result = subject.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"),
        stringToBytes("bar"));
  }

  @Test
  public void findChannelNames_shouldReturnOnlyMatchingChannelNames_whenCalledWithPattern() {
    Subscriptions subject = new Subscriptions();
    byte[] pattern = stringToBytes("b*");

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    subject.add(new ChannelSubscription(client, stringToBytes("foo"), context, subject));
    subject.add(new ChannelSubscription(client, stringToBytes("bar"), context, subject));
    subject
        .add(new ChannelSubscription(client, stringToBytes("barbarella"), context, subject));

    List<byte[]> result = subject.findChannelNames(pattern);

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("bar"),
        stringToBytes("barbarella"));
  }

  @Test
  public void findChannelNames_shouldNotReturnPatternSubscriptions() {

    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo =
        new ChannelSubscription(client, stringToBytes("foo"), context, subject);

    Subscription patternSubscriptionBar =
        new PatternSubscription(client, new GlobPattern("bar"), context, subject);

    subject.add(subscriptionFoo);
    subject.add(patternSubscriptionBar);

    List<byte[]> result = subject.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }


  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithoutPattern() {

    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo1 =
        new ChannelSubscription(client, stringToBytes("foo"), context, subject);

    Subscription subscriptionFoo2 =
        new ChannelSubscription(client, stringToBytes("foo"), context, subject);

    subject.add(subscriptionFoo1);
    subject.add(subscriptionFoo2);

    List<byte[]> result = subject.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }

  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithPattern() {

    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo1 =
        new ChannelSubscription(client, stringToBytes("foo"), context, subject);

    Subscription subscriptionFoo2 =
        new ChannelSubscription(client, stringToBytes("foo"), context, subject);

    subject.add(subscriptionFoo1);
    subject.add(subscriptionFoo2);

    List<byte[]> result = subject.findChannelNames(stringToBytes("f*"));

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }
}
