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

package org.apache.geode.redis.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.netty.channel.Channel;
import org.junit.Test;

import org.apache.geode.redis.internal.org.apache.hadoop.fs.GlobPattern;

public class SubscriptionsJUnitTest {

  @Test
  public void correctlyIdentifiesChannelSubscriber() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Client client = new Client(channel);

    subscriptions.add(new ChannelSubscription(client, "subscriptions", context));

    assertThat(subscriptions.exists("subscriptions", client)).isTrue();
    assertThat(subscriptions.exists("unknown", client)).isFalse();
  }

  @Test
  public void correctlyIdentifiesPatternSubscriber() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Client client = new Client(channel);
    GlobPattern pattern = new GlobPattern("sub*s");

    subscriptions.add(new PatternSubscription(client, pattern, context));

    assertThat(subscriptions.exists(pattern, client)).isTrue();
  }

  @Test
  public void doesNotMisidentifyChannelAsPattern() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Client client = new Client(channel);
    GlobPattern globPattern1 = new GlobPattern("sub*s");
    GlobPattern globPattern2 = new GlobPattern("subscriptions");

    subscriptions.add(new ChannelSubscription(client, "subscriptions", context));

    assertThat(subscriptions.exists(globPattern1, client)).isFalse();
    assertThat(subscriptions.exists(globPattern2, client)).isFalse();
  }

  @Test
  public void doesNotMisidentifyWhenBothTypesArePresent() {
    Subscriptions subscriptions = new Subscriptions();

    Channel channel = mock(Channel.class);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Client client = new Client(channel);
    GlobPattern globby = new GlobPattern("sub*s");

    subscriptions.add(new ChannelSubscription(client, "subscriptions", context));
    subscriptions.add(new PatternSubscription(client, globby, context));

    assertThat(subscriptions.exists(globby, client)).isTrue();
    assertThat(subscriptions.exists("subscriptions", client)).isTrue();
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

    Channel mockChannelOne = mock(Channel.class);
    Channel mockChannelTwo = mock(Channel.class);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Client clientOne = new Client(mockChannelOne);
    Client clientTwo = new Client(mockChannelTwo);

    ChannelSubscription subscriptionOne =
        new ChannelSubscription(clientOne, "subscriptions", context);
    ChannelSubscription subscriptionTwo = new ChannelSubscription(clientTwo, "monkeys", context);

    subscriptions.add(subscriptionOne);
    subscriptions.add(subscriptionTwo);

    assertThat(subscriptions.findSubscriptions(clientOne)).containsExactly(subscriptionOne);
  }

  @Test
  public void removeByClient() {
    Subscriptions subscriptions = new Subscriptions();
    Channel mockChannelOne = mock(Channel.class);
    Channel mockChannelTwo = mock(Channel.class);
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Client clientOne = new Client(mockChannelOne);
    Client clientTwo = new Client(mockChannelTwo);

    ChannelSubscription subscriptionOne =
        new ChannelSubscription(clientOne, "subscriptions", context);
    ChannelSubscription subscriptionTwo = new ChannelSubscription(clientTwo, "monkeys", context);

    subscriptions.add(subscriptionOne);
    subscriptions.add(subscriptionTwo);

    subscriptions.remove(clientOne);

    assertThat(subscriptions.findSubscriptions(clientOne)).isEmpty();
    assertThat(subscriptions.findSubscriptions(clientTwo)).containsExactly(subscriptionTwo);
  }

  @Test
  public void removeByClientAndPattern() {

    Subscriptions subscriptions = new Subscriptions();
    Channel mockChannelOne = mock(Channel.class);

    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Client client = new Client(mockChannelOne);

    ChannelSubscription channelSubscriberOne =
        new ChannelSubscription(client, "subscriptions", context);
    GlobPattern pattern = new GlobPattern("monkeys");
    PatternSubscription patternSubscriber = new PatternSubscription(client,
        pattern, context);
    ChannelSubscription channelSubscriberTwo = new ChannelSubscription(client, "monkeys", context);

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
}
