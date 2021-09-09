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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPUNSUBSCRIBE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bUNSUBSCRIBE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public class SubscriptionsJUnitTest {

  private final Subscriptions subscriptions = new Subscriptions();

  @Test
  public void correctlyIdentifiesChannelSubscriber() {
    Client client = createClient();

    addChannelSubscription(client, "subscriptions");

    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("unknown"))).isZero();
    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("subscriptions"))).isOne();
  }

  @Test
  public void correctlyCountsSubscriptions() {
    Client client1 = createClient();
    Client client2 = createClient();
    Client client3 = createClient();

    addChannelSubscription(client1, "subscription1");
    addChannelSubscription(client3, "subscription1");
    addChannelSubscription(client2, "subscription2");
    addPatternSubscription(client1, "sub*");
    addPatternSubscription(client3, "sub*");
    addPatternSubscription(client2, "subscription?");

    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("subscription1"))).isEqualTo(5);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("subscription2"))).isEqualTo(4);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("subscription3"))).isEqualTo(3);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("sub1"))).isEqualTo(2);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("none"))).isEqualTo(0);
  }

  @Test
  public void foreachHitsEachSubscription() {
    Client client1 = createClient();
    Client client2 = createClient();
    Client client3 = createClient();

    addChannelSubscription(client1, "subscription1");
    addChannelSubscription(client3, "subscription1");
    addChannelSubscription(client2, "subscription2");
    addPatternSubscription(client1, "sub*");
    addPatternSubscription(client3, "sub*");
    addPatternSubscription(client2, "subscription?");

    List<String> hits = new ArrayList<>();
    subscriptions.forEachSubscription(stringToBytes("subscription1"),
        subscription -> hits.add(bytesToString(subscription.getSubscriptionName())));
    assertThat(hits).containsExactlyInAnyOrder("subscription1", "subscription1", "sub*", "sub*",
        "subscription?");
  }

  @Test
  public void subscribeReturnsExpectedResult() {
    Client client = createClient();
    final byte[] channel = stringToBytes("channel");

    SubscribeResult result = subscriptions.subscribe(channel, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(channel);
    assertThat(result.getSubscription()).isInstanceOf(ChannelSubscription.class);
    assertThat(result.getSubscription().getSubscriptionName()).isEqualTo(channel);
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getChannelSubscriptions()).containsExactlyInAnyOrder(channel);
  }

  @Test
  public void psubscribeReturnsExpectedResult() {
    Client client = createClient();
    final byte[] pattern = stringToBytes("pattern");

    SubscribeResult result = subscriptions.psubscribe(pattern, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(pattern);
    assertThat(result.getSubscription()).isInstanceOf(PatternSubscription.class);
    assertThat(result.getSubscription().getSubscriptionName()).isEqualTo(pattern);
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getPatternSubscriptions()).containsExactlyInAnyOrder(pattern);
  }

  @Test
  public void subscribeDoesNothingIfAlreadySubscribed() {
    Client client = createClient();
    final byte[] channel = stringToBytes("channel");

    subscriptions.subscribe(channel, client);
    SubscribeResult result = subscriptions.subscribe(channel, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(channel);
    assertThat(result.getSubscription()).isNull();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getChannelSubscriptions()).containsExactlyInAnyOrder(channel);
  }

  @Test
  public void psubscribeDoesNothingIfAlreadySubscribed() {
    Client client = createClient();
    final byte[] pattern = stringToBytes("pattern");

    subscriptions.psubscribe(pattern, client);
    SubscribeResult result = subscriptions.psubscribe(pattern, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(pattern);
    assertThat(result.getSubscription()).isNull();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getPatternSubscriptions()).containsExactlyInAnyOrder(pattern);
  }

  @Test
  public void psubscribeCleanUpAfterFailedSubscribe() {
    Client client = createClient();
    final byte[] pattern = stringToBytes("\\C");

    assertThatThrownBy(() -> subscriptions.psubscribe(pattern, client))
        .isInstanceOf(PatternSyntaxException.class);

    assertThat(client.getSubscriptionCount()).isZero();
  }

  @Test
  public void correctlyIdentifiesPatternSubscriber() {
    Client client = createClient();

    addPatternSubscription(client, "sub*s");

    assertThat(subscriptions.getChannelSubscriptionCount()).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount()).isOne();
    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("subscriptions"))).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("subscriptions"))).isOne();
  }

  @Test
  public void doesNotMisidentifyChannelAsPattern() {
    Client client = createClient();

    addChannelSubscription(client, "subscriptions");

    assertThat(subscriptions.getChannelSubscriptionCount()).isOne();
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("subscriptions"))).isZero();
  }

  @Test
  public void doesNotMisidentifyWhenBothTypesArePresent() {
    Client client = createClient();

    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "sub*s");

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
  public void findSubscribers() {
    Client client1 = createClient();
    Client client2 = createClient();

    addChannelSubscription(client1, "subscriptions");
    addChannelSubscription(client2, "monkeys");

    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"),
            stringToBytes("monkeys"));
  }

  @Test
  public void removeByClient() {
    Client clientOne = createClient();
    Client clientTwo = createClient();
    addChannelSubscription(clientOne, "subscriptions");
    addChannelSubscription(clientTwo, "monkeys");

    subscriptions.remove(clientOne);

    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("monkeys"));
  }

  @Test
  public void removeByClientAndPattern() {
    byte[] pattern = stringToBytes("monkeys");
    Client client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    Collection<Collection<?>> result = subscriptions.punsubscribe(singletonList(pattern), client);

    assertThat(result).containsExactly(asList(bPUNSUBSCRIBE, pattern, 2L));
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeOnePattern() {
    byte[] pattern = stringToBytes("monkeys");
    Client client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    Collection<Collection<?>> result = subscriptions.punsubscribe(singletonList(pattern), client);

    assertThat(result).containsExactly(asList(bPUNSUBSCRIBE, pattern, 2L));
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeTwoPatterns() {
    byte[] pattern1 = stringToBytes("monkeys");
    byte[] pattern2 = stringToBytes("subscriptions");
    Client client = createClient();
    addPatternSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    Collection<Collection<?>> result =
        subscriptions.punsubscribe(asList(pattern1, pattern2), client);

    assertThat(result).containsExactly(asList(bPUNSUBSCRIBE, pattern1, 2L),
        asList(bPUNSUBSCRIBE, pattern2, 1L));
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeAllPatterns() {
    byte[] pattern = stringToBytes("monkeys");
    Client client = createClient();
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    Collection<Collection<?>> result = subscriptions.punsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    Collection<Object> firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(bPUNSUBSCRIBE, pattern, 1L);
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeAllChannelsWhenNoSubscriptions() {
    Client client = createClient();

    Collection<Collection<?>> result = subscriptions.unsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    Collection<Object> firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(bUNSUBSCRIBE, null, 0L);
  }

  @Test
  public void unsubscribeAllPatternsWhenNoSubscriptions() {
    Client client = createClient();

    Collection<Collection<?>> result = subscriptions.punsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    Collection<Object> firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(bPUNSUBSCRIBE, null, 0L);
  }

  @Test
  public void unsubscribeOneChannel() {
    byte[] channel = stringToBytes("monkeys");
    Client client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    Collection<Collection<?>> result = subscriptions.unsubscribe(singletonList(channel), client);

    assertThat(result).containsExactly(asList(bUNSUBSCRIBE, channel, 2L));
    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"));
  }

  @Test
  public void unsubscribeTwoChannels() {
    byte[] channel1 = stringToBytes("monkeys");
    byte[] channel2 = stringToBytes("subscriptions");
    Client client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    Collection<Collection<?>> result =
        subscriptions.unsubscribe(asList(channel1, channel2), client);

    assertThat(result).containsExactly(asList(bUNSUBSCRIBE, channel1, 2L),
        asList(bUNSUBSCRIBE, channel2, 1L));
    assertThat(subscriptions.findChannelNames()).isEmpty();
  }

  @Test
  public void unsubscribeAllChannels() {
    byte[] channel = stringToBytes("monkeys");
    Client client = createClient();
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    Collection<Collection<?>> result = subscriptions.unsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    Collection<Object> firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(bUNSUBSCRIBE, channel, 1L);
    assertThat(subscriptions.findChannelNames()).isEmpty();
  }

  @Test
  public void findChannelNames_shouldReturnAllChannelNames_whenCalledWithoutParameter() {
    Client client = createClient();
    addChannelSubscription(client, "foo");
    addChannelSubscription(client, "bar");

    List<byte[]> result = subscriptions.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"), stringToBytes("bar"));
  }

  @Test
  public void findChannelNames_shouldReturnOnlyMatchingChannelNames_whenCalledWithPattern() {
    byte[] pattern = stringToBytes("b*");
    Client client = createClient();
    addChannelSubscription(client, "foo");
    addChannelSubscription(client, "bar");
    addChannelSubscription(client, "barbarella");

    List<byte[]> result = subscriptions.findChannelNames(pattern);

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("bar"),
        stringToBytes("barbarella"));
  }

  @Test
  public void findChannelNames_shouldNotReturnPatternSubscriptions() {
    Client client = createClient();
    addChannelSubscription(client, "foo");
    addPatternSubscription(client, "bar");

    List<byte[]> result = subscriptions.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }

  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithoutPattern() {
    Client client1 = createClient();
    Client client2 = createClient();
    addChannelSubscription(client1, "foo");
    addChannelSubscription(client2, "foo");

    List<byte[]> result = subscriptions.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }

  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithPattern() {
    Client client1 = createClient();
    Client client2 = createClient();
    addChannelSubscription(client1, "foo");
    addChannelSubscription(client2, "foo");

    List<byte[]> result = subscriptions.findChannelNames(stringToBytes("f*"));

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }


  private void addPatternSubscription(Client client, String pattern) {
    byte[] patternBytes = stringToBytes(pattern);
    subscriptions.addPattern(patternBytes, client);
  }

  private void addChannelSubscription(Client client, String channel) {
    byte[] channelBytes = stringToBytes(channel);
    subscriptions.addChannel(channelBytes, client);
  }

  private Channel createChannel() {
    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    return channel;
  }

  private Client createClient() {
    return new Client(createChannel(), mock(PubSub.class));
  }

}
