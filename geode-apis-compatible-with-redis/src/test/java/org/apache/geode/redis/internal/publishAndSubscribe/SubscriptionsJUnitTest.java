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

package org.apache.geode.redis.internal.publishAndSubscribe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.Coder;
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
        .add(new ChannelSubscription(client, Coder.stringToBytes("subscriptions"), context,
            subscriptions));

    assertThat(subscriptions.exists(Coder.stringToBytes("subscriptions"), client)).isTrue();
    assertThat(subscriptions.exists(Coder.stringToBytes("unknown"), client)).isFalse();
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
        .add(new ChannelSubscription(client, Coder.stringToBytes("subscriptions"), context,
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
        .add(new ChannelSubscription(client, Coder.stringToBytes("subscriptions"), context,
            subscriptions));
    subscriptions.add(new PatternSubscription(client, globby, context, subscriptions));

    assertThat(subscriptions.exists(globby, client)).isTrue();
    assertThat(subscriptions.exists(Coder.stringToBytes("subscriptions"), client)).isTrue();
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
        new ChannelSubscription(clientOne, Coder.stringToBytes("subscriptions"), context,
            subscriptions);
    ChannelSubscription subscriptionTwo =
        new ChannelSubscription(clientTwo, Coder.stringToBytes("monkeys"), context, subscriptions);

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
        new ChannelSubscription(clientOne, Coder.stringToBytes("subscriptions"), context,
            subscriptions);
    ChannelSubscription subscriptionTwo =
        new ChannelSubscription(clientTwo, Coder.stringToBytes("monkeys"), context, subscriptions);

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
        new ChannelSubscription(client, Coder.stringToBytes("subscriptions"), context,
            subscriptions);
    GlobPattern pattern = new GlobPattern("monkeys");
    PatternSubscription patternSubscriber = new PatternSubscription(client,
        pattern, context, subscriptions);
    ChannelSubscription channelSubscriberTwo =
        new ChannelSubscription(client, Coder.stringToBytes("monkeys"), context, subscriptions);

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
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    Subscription subscriptionBar =
        new ChannelSubscription(client, Coder.stringToBytes("bar"), context, subject);

    subject.add(subscriptionFoo);
    subject.add(subscriptionBar);

    List<Object> result = subject.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(Coder.stringToBytes("foo"),
        Coder.stringToBytes("bar"));
  }

  @Test
  public void findChannelNames_shouldReturnOnlyMatchingChannelNames_whenCalledWithPattern() {
    Subscriptions subject = new Subscriptions();
    byte[] pattern = Coder.stringToBytes("b*");

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    subject.add(new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject));
    subject.add(new ChannelSubscription(client, Coder.stringToBytes("bar"), context, subject));
    subject
        .add(new ChannelSubscription(client, Coder.stringToBytes("barbarella"), context, subject));

    List<Object> result = subject.findChannelNames(pattern);

    assertThat(result).containsExactlyInAnyOrder(Coder.stringToBytes("bar"),
        Coder.stringToBytes("barbarella"));
  }

  @Test
  public void findChannelNames_shouldNotReturnPatternSubscriptions() {

    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo =
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    Subscription patternSubscriptionBar =
        new PatternSubscription(client, new GlobPattern("bar"), context, subject);

    subject.add(subscriptionFoo);
    subject.add(patternSubscriptionBar);

    List<Object> result = subject.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(Coder.stringToBytes("foo"));
  }


  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithoutPattern() {

    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo1 =
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    Subscription subscriptionFoo2 =
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    subject.add(subscriptionFoo1);
    subject.add(subscriptionFoo2);

    List<Object> result = subject.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(Coder.stringToBytes("foo"));
  }

  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithPattern() {

    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo1 =
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    Subscription subscriptionFoo2 =
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    subject.add(subscriptionFoo1);
    subject.add(subscriptionFoo2);

    List<Object> result = subject.findChannelNames(Coder.stringToBytes("f*"));

    assertThat(result).containsExactlyInAnyOrder(Coder.stringToBytes("foo"));
  }

  @Test
  public void findNumberOfSubscribersByChannel_shouldReturnListOfChannelsAndSubscriberCount_givenListOfActiveChannels() {
    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo1 =
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    Subscription subscriptionFoo2 =
        new ChannelSubscription(client, Coder.stringToBytes("foo"), context, subject);

    subject.add(subscriptionFoo1);
    subject.add(subscriptionFoo2);

    List<byte[]> channels = new ArrayList<>();
    channels.add(Coder.stringToBytes("foo"));

    List<Object> actual = subject.findNumberOfSubscribersForChannel(channels);

    assertThat(actual.get(0)).isEqualTo(Coder.stringToBytes("foo"));

    assertThat(actual.get(1)).isEqualTo(2L);
  }

  @Test
  public void findNumberOfSubscribersByChannel_shouldReturnChannelNameAndZero_givenInactiveActiveChannel() {
    Subscriptions subject = new Subscriptions();

    List<byte[]> channels = new ArrayList<>();
    channels.add(Coder.stringToBytes("bar"));

    ArrayList<Object> result =
        (ArrayList<Object>) subject.findNumberOfSubscribersForChannel(channels);

    assertThat(result.get(0)).isEqualTo(Coder.stringToBytes("bar"));
    assertThat(result.get(1)).isEqualTo(0L);
  }


  @Test
  public void findNumberOfSubscribersByChannel_shouldPatterAndZero_givenPatternSubscription() {
    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);

    Subscription subscriptionFoo1 =
        new PatternSubscription(client, new GlobPattern("f*"), context, subject);

    subject.add(subscriptionFoo1);

    List<byte[]> channels = new ArrayList<>();
    channels.add(Coder.stringToBytes("f*"));

    List<Object> actual = subject.findNumberOfSubscribersForChannel(channels);

    assertThat(actual.get(0)).isEqualTo(Coder.stringToBytes("f*"));
    assertThat(actual.get(1)).isEqualTo(0L);
  }
}
