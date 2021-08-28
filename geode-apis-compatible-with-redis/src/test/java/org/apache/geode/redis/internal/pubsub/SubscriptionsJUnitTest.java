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
    when(context.getClient()).thenReturn(client);

    subscriptions
        .add(new ChannelSubscription(stringToBytes("subscriptions"), context,
            subscriptions));

    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("unknown"))).isZero();
    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("subscriptions"))).isOne();
  }

  @Test
  public void correctlyIdentifiesPatternSubscriber() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    when(context.getClient()).thenReturn(client);

    byte[] pattern = stringToBytes("sub*s");

    subscriptions.add(new PatternSubscription(pattern, context, subscriptions));

    assertThat(subscriptions.getChannelSubscriptionCount()).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount()).isOne();
    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("subscriptions"))).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("subscriptions"))).isOne();
  }

  @Test
  public void doesNotMisidentifyChannelAsPattern() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    when(context.getClient()).thenReturn(client);

    subscriptions
        .add(new ChannelSubscription(stringToBytes("subscriptions"), context,
            subscriptions));

    assertThat(subscriptions.getChannelSubscriptionCount()).isOne();
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("subscriptions"))).isZero();
  }

  @Test
  public void doesNotMisidentifyWhenBothTypesArePresent() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    when(context.getClient()).thenReturn(client);

    byte[] globby = stringToBytes("sub*s");

    subscriptions
        .add(new ChannelSubscription(stringToBytes("subscriptions"), context,
            subscriptions));
    subscriptions.add(new PatternSubscription(globby, context, subscriptions));

    assertThat(subscriptions.size()).isEqualTo(2);
    assertThat(subscriptions.getPatternSubscriptionCount()).isOne();
    assertThat(subscriptions.getChannelSubscriptionCount()).isOne();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("sub1s"))).isOne();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("sub1sF"))).isZero();
    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"));
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

    ExecutionHandlerContext context1 = mock(ExecutionHandlerContext.class);
    ExecutionHandlerContext context2 = mock(ExecutionHandlerContext.class);
    Channel mockChannelOne = mock(Channel.class);
    Channel mockChannelTwo = mock(Channel.class);
    when(mockChannelOne.closeFuture()).thenReturn(mock(ChannelFuture.class));
    when(mockChannelTwo.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client clientOne = new Client(mockChannelOne);
    Client clientTwo = new Client(mockChannelTwo);
    when(context1.getClient()).thenReturn(clientOne);
    when(context2.getClient()).thenReturn(clientTwo);

    ChannelSubscription subscriptionOne =
        new ChannelSubscription(stringToBytes("subscriptions"), context1,
            subscriptions);
    ChannelSubscription subscriptionTwo =
        new ChannelSubscription(stringToBytes("monkeys"), context2, subscriptions);

    subscriptions.add(subscriptionOne);
    subscriptions.add(subscriptionTwo);


    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"),
            stringToBytes("monkeys"));
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

    ExecutionHandlerContext context1 = mock(ExecutionHandlerContext.class);
    ExecutionHandlerContext context2 = mock(ExecutionHandlerContext.class);
    when(context1.getClient()).thenReturn(clientOne);
    when(context2.getClient()).thenReturn(clientTwo);

    ChannelSubscription subscriptionOne =
        new ChannelSubscription(stringToBytes("subscriptions"), context1,
            subscriptions);
    ChannelSubscription subscriptionTwo =
        new ChannelSubscription(stringToBytes("monkeys"), context2, subscriptions);

    clientOne.addChannelSubscription(stringToBytes("subscriptions"));
    subscriptions.add(subscriptionOne);
    subscriptions.add(subscriptionTwo);

    subscriptions.remove(clientOne);

    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("monkeys"));
  }

  @Test
  public void removeByClientAndPattern() {

    Subscriptions subscriptions = new Subscriptions();
    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    when(context.getClient()).thenReturn(client);

    ChannelSubscription channelSubscriberOne =
        new ChannelSubscription(stringToBytes("subscriptions"), context,
            subscriptions);
    byte[] pattern = stringToBytes("monkeys");
    PatternSubscription patternSubscriber = new PatternSubscription(
        pattern, context, subscriptions);
    ChannelSubscription channelSubscriberTwo =
        new ChannelSubscription(stringToBytes("monkeys"), context, subscriptions);

    subscriptions.add(channelSubscriberOne);
    subscriptions.add(patternSubscriber);
    subscriptions.add(channelSubscriberTwo);

    subscriptions.punsubscribe(pattern, client);

    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"),
            stringToBytes("monkeys"));
  }

  @Test
  public void findChannelNames_shouldReturnAllChannelNames_whenCalledWithoutParameter() {
    Subscriptions subject = new Subscriptions();

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    when(context.getClient()).thenReturn(client);

    ChannelSubscription subscriptionFoo =
        new ChannelSubscription(stringToBytes("foo"), context, subject);

    ChannelSubscription subscriptionBar =
        new ChannelSubscription(stringToBytes("bar"), context, subject);

    subject.add(subscriptionFoo);
    subject.add(subscriptionBar);

    List<byte[]> result = subject.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"), stringToBytes("bar"));
  }

  @Test
  public void findChannelNames_shouldReturnOnlyMatchingChannelNames_whenCalledWithPattern() {
    Subscriptions subject = new Subscriptions();
    byte[] pattern = stringToBytes("b*");

    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    Client client = new Client(channel);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    when(context.getClient()).thenReturn(client);

    subject.add(new ChannelSubscription(stringToBytes("foo"), context, subject));
    subject.add(new ChannelSubscription(stringToBytes("bar"), context, subject));
    subject
        .add(new ChannelSubscription(stringToBytes("barbarella"), context, subject));

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
    when(context.getClient()).thenReturn(client);

    ChannelSubscription subscriptionFoo =
        new ChannelSubscription(stringToBytes("foo"), context, subject);

    PatternSubscription patternSubscriptionBar =
        new PatternSubscription(stringToBytes("bar"), context, subject);

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
    when(context.getClient()).thenReturn(client);

    ChannelSubscription subscriptionFoo1 =
        new ChannelSubscription(stringToBytes("foo"), context, subject);

    ChannelSubscription subscriptionFoo2 =
        new ChannelSubscription(stringToBytes("foo"), context, subject);

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
    when(context.getClient()).thenReturn(client);

    ChannelSubscription subscriptionFoo1 =
        new ChannelSubscription(stringToBytes("foo"), context, subject);

    ChannelSubscription subscriptionFoo2 =
        new ChannelSubscription(stringToBytes("foo"), context, subject);

    subject.add(subscriptionFoo1);
    subject.add(subscriptionFoo2);

    List<byte[]> result = subject.findChannelNames(stringToBytes("f*"));

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }
}
